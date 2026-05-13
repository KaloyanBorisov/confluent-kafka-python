# Multi-Language Kafka Clients

## Can I combine different API language implementations on both sides of the broker?

**Yes, absolutely.** This is one of Kafka's core design principles — the broker is language-agnostic. It only speaks the **Kafka wire protocol** (TCP binary protocol), so any client that implements it can talk to any broker regardless of what other clients are connected.

## How it works

```
┌─────────────────────────────────────────────────────┐
│                   Kafka Broker                       │
│                                                      │
│   topic: orders                                      │
│   ┌──────────────────────────────────────────────┐  │
│   │ partition 0 │ partition 1 │ partition 2      │  │
│   └──────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
         ▲                          │
         │ produce                  │ consume
         │                          ▼
┌─────────────────┐      ┌─────────────────────┐
│  Python service │      │   Java service       │
│  (ML pipeline)  │      │   (Kafka Streams)    │
└─────────────────┘      └─────────────────────┘

         ▲                          │
         │ produce                  │ consume
         │                          ▼
┌─────────────────┐      ┌─────────────────────┐
│  Node.js API    │      │   Go microservice    │
│  (web backend)  │      │   (order processor)  │
└─────────────────┘      └─────────────────────┘
```

Any producer, any consumer — they all just read/write bytes from/to partitions.

---

## The One Requirement: Schema Agreement

The broker doesn't care about message format — it stores raw bytes. But producers and consumers must agree on how to interpret those bytes. Schema Registry solves this:

```
Python producer                        Java consumer
──────────────────                     ──────────────────
AvroSerializer                         KafkaAvroDeserializer
    │                                      │
    ▼                                      ▼
Schema Registry ◄─────── same schema ────► Schema Registry
    │                                      │
    ▼                                      ▼
[ schema ID ][ avro bytes ] ──────────► decoded object
```

As long as both sides use Schema Registry with the same subject/schema, the language doesn't matter. A Python producer writing Avro can be consumed by Java, Go, Node, or .NET without any changes.

---

## Real-World Mixed Patterns

| Producer | Consumer | Use case |
|---|---|---|
| Node.js (web API) | Java (Kafka Streams) | Web events → real-time aggregation |
| Python (ML model) | Go (microservice) | Predictions → downstream processing |
| Java (CDC connector) | Python (data pipeline) | Database changes → analytics |
| Go (IoT gateway) | Python + Java + Node | Fan-out to multiple services |
| Any language | Any language | Decoupled microservices |

---

## What You Need to Agree On

| Thing | Why |
|---|---|
| **Serialization format** | Avro / Protobuf / JSON — both sides must use the same |
| **Schema** | Register in Schema Registry, reference by subject name |
| **Topic name** | Must match on producer and consumer |
| **Key/Value structure** | Same schema for key and value on both sides |

Everything else — language, framework, library version, deployment platform — is completely independent on each side.

---

## Client Performance Comparison

```
Fastest
   │
   │  1. C / C++        librdkafka itself — the foundation everything else builds on
   │
   │  2. Java           kafka-clients (Apache) — JVM JIT gets very close to C
   │                    Confluent's Java client — same, plus enterprise features
   │
   │  3. Go             confluent-kafka-go — thin CGo wrapper over librdkafka
   │                    sarama — pure Go, slightly slower but no CGo overhead
   │
   │  4. .NET           Confluent.Kafka — librdkafka binding, very close to Go
   │
   │  5. Python         confluent-kafka-python — librdkafka binding
   │                    (GIL limits true parallelism, but I/O bound work is fine)
   │
   │  6. Node.js        @confluentinc/kafka-javascript — librdkafka binding
   │                    (event loop overhead on top of librdkafka)
   │
   │  7. Python         kafka-python — pure Python, no librdkafka
   │     Node.js        kafkajs — pure JS, no librdkafka
   │
Slowest
```

### Why Java Competes with C

The JVM JIT compiler optimizes hot paths at runtime — after warmup, Java throughput benchmarks within 5-10% of raw librdkafka. For long-running services this gap is negligible.

**Weakness:** startup time and memory footprint. Java needs 1-2s to warm up and uses significantly more RAM.

### Why Python/Node Are Not "Slow" for Kafka

The actual bottleneck in Kafka is **network I/O and broker throughput**, not the client language. A Python or Node client can saturate a Kafka broker just as well as a Java client because:

- Message serialization/deserialization is the hot path — Avro/Protobuf libraries are C extensions in Python too
- librdkafka handles batching, compression, and network in C regardless of which language calls it
- A single broker partition tops out around 100-200 MB/s regardless of client language

### Approximate Benchmarks (single producer)

| Client | Throughput | Latency (p99) |
|---|---|---|
| Java (Confluent) | ~1.5M msgs/sec | ~5ms |
| Go (confluent-kafka-go) | ~1.3M msgs/sec | ~6ms |
| .NET (Confluent.Kafka) | ~1.2M msgs/sec | ~7ms |
| Python (confluent-kafka) | ~1.0M msgs/sec | ~8ms |
| Node.js (kafka-javascript) | ~0.9M msgs/sec | ~10ms |
| Python (kafka-python) | ~200K msgs/sec | ~30ms |
| Node.js (kafkajs) | ~300K msgs/sec | ~20ms |

### Choosing a Client

Speed difference between the librdkafka-based clients (Java, Go, .NET, Python, Node) is marginal in production. The real choice driver is:

| Client | Best for |
|---|---|
| **Java** | Highest throughput, Kafka Streams, mature ecosystem |
| **Go** | Microservices, low memory, fast startup |
| **Python** | Data pipelines, ML, schema-heavy workflows |
| **Node.js** | Kafka as part of a JS/TS backend service |
| **Pure JS/Python** | Low-volume use cases or when native builds are not possible |

---

## TypeScript / JavaScript Client

Confluent's `@confluentinc/kafka-javascript` is the direct equivalent of this Python client — same underlying librdkafka C library, same Schema Registry integration, same Confluent Platform support.

### Two API styles

**Promisified (async/await) — equivalent to AIOProducer/AIOConsumer:**
```typescript
import { KafkaJS } from '@confluentinc/kafka-javascript';

const kafka = new KafkaJS.Kafka({ brokers: ['broker:9092'] });
const producer = kafka.producer();
await producer.connect();
await producer.send({ topic: 'test', messages: [{ value: 'hello' }] });
await producer.disconnect();
```

**Classic callback style — equivalent to synchronous Producer/Consumer:**
```typescript
import { Producer } from '@confluentinc/kafka-javascript';

const producer = new Producer({ 'bootstrap.servers': 'broker:9092' });
producer.produce('test', null, Buffer.from('hello'));
producer.flush(5000, () => producer.disconnect());
```

### Schema Registry in TypeScript

```typescript
import { SchemaRegistryClient, AvroSerializer } from '@confluentinc/kafka-javascript';

const sr = new SchemaRegistryClient({ baseURLs: ['http://localhost:8081'] });
const serializer = new AvroSerializer(sr, SerdeType.VALUE, { useSchemaId: 1 });

const encoded = await serializer.serialize(topic, { name: 'alice' });
await producer.send({ topic, messages: [{ value: encoded }] });
```

### JavaScript client options

| Library | Notes |
|---|---|
| `@confluentinc/kafka-javascript` | Built on librdkafka, full Schema Registry support, recommended for Confluent |
| `kafkajs` | Pure JS, popular, no librdkafka, no built-in Schema Registry |
| `node-rdkafka` | Older librdkafka binding, lower-level, less maintained |
