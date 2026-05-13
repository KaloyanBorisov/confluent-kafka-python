# AsyncIO Avro Producer Example

## The Core Idea

Produce one Kafka message where the value is **Avro-serialized** and the schema is **registered/fetched from Schema Registry** — all done asynchronously using `asyncio`.

Three components work together:

```
Schema Registry  ←→  AsyncAvroSerializer  →  AIOProducer  →  Kafka Broker
  (schema store)       (encode + frame)       (async send)
```

---

## The Three Main Components

### 1. Schema Registry Client

```python
sr_client = AsyncSchemaRegistryClient(sr_conf)
```

Schema Registry is a separate service that stores Avro/JSON/Protobuf schemas. Before a producer can serialize a message, it needs to register the schema and get back a **schema ID** (an integer). That ID gets embedded in every message so consumers know how to deserialize it.

### 2. Avro Serializer

```python
avro_serializer = await AsyncAvroSerializer(sr_client, schema_str=schema_str)
```

The serializer wraps the Schema Registry client. When called, it:

1. Registers the schema (or fetches its ID if already registered)
2. Serializes the Python dict into Avro binary format
3. Prepends a **5-byte header**: `\x00` (magic byte) + 4-byte schema ID

That wire format is the **Confluent Schema Registry wire format** — every Kafka message produced with Schema Registry follows it.

> **Why `await` the constructor?**
> This is unusual — normally you don't `await` a constructor. `AsyncAvroSerializer` eagerly contacts Schema Registry during construction to register/validate the schema. This avoids lazy registration on the first message.

### 3. AIOProducer

```python
producer = AIOProducer({'bootstrap.servers': args.bootstrap_servers})
```

The async wrapper around the standard `confluent_kafka.Producer`. Key difference from the sync producer:

| Sync Producer | AIOProducer |
|---|---|
| `produce(topic, value, on_delivery=callback)` | `await producer.produce(...)` returns a `Future` |
| Delivery callback fires when broker acks | `await future` gives you the `RecordMetadata` |

---

## The Message Flow

```
Python dict {'name': 'alice'}
        ↓
AsyncAvroSerializer
  → register schema with Schema Registry → get schema_id=1
  → serialize dict to Avro binary bytes
  → prepend [0x00][0x00 0x00 0x00 0x01]  ← magic byte + schema ID
        ↓
serialized bytes (5-byte header + Avro payload)
        ↓
AIOProducer.produce(topic, value=serialized_bytes)
        ↓
await future  ← blocks until broker acknowledges
        ↓
print topic/partition/offset
```

---

## The SerializationContext

```python
SerializationContext(args.topic, MessageField.VALUE)
```

Tells the serializer *where* in the message this data lives — `VALUE` vs `KEY`. This matters because Schema Registry uses separate subjects for keys and values: `my-topic-key` vs `my-topic-value`. The serializer uses this to look up or register the schema under the right subject name.

---

## Running

### Prerequisites

You need a running Kafka broker and Schema Registry. The easiest way is the `cp-all-in-one-kraft` Docker Compose stack from Confluent:

```bash
# Download and start Kafka + Schema Registry
curl -O https://raw.githubusercontent.com/confluentinc/cp-all-in-one/main/cp-all-in-one-kraft/docker-compose.yml
docker compose up -d broker schema-registry
```

Wait ~30 seconds for services to be healthy.

### Option A — Docker examples container

Start the examples container (from the `confluent-kafka-python` repo root):

```bash
docker compose up -d
docker compose exec examples bash
```

Run the script inside the container using the pre-set env vars:

```bash
python examples/asyncio_avro_producer.py \
  -b $BOOTSTRAP_SERVERS \
  -s $SCHEMA_REGISTRY_URL
```

### Option B — Dev container (VSCode)

Add the Kafka network to your `.devcontainer/devcontainer.json`:

```json
{
  "runArgs": ["--network=cp-all-in-one-kraft_default"]
}
```

Then run from the terminal inside the dev container:

```bash
python examples/asyncio_avro_producer.py \
  -b broker:29092 \
  -s http://schema-registry:8081
```

### Option C — Confluent Cloud

```bash
python examples/asyncio_avro_producer.py \
  -b pkc-xxxx.region.provider.confluent.cloud:9092 \
  -s https://psrc-xxxx.region.provider.confluent.cloud \
  --sr-api-key YOUR_SR_KEY \
  --sr-api-secret YOUR_SR_SECRET
```

> Note: Confluent Cloud Kafka broker auth (`sasl.*` settings) is not wired into this script — it only handles Schema Registry auth via CLI args.

### CLI Arguments

| Flag | Description | Default |
|---|---|---|
| `-b` | Bootstrap broker(s) `host[:port]` | required |
| `-s` | Schema Registry URL `http(s)://host[:port]` | required |
| `--sr-api-key` | Confluent Cloud SR API key | optional |
| `--sr-api-secret` | Confluent Cloud SR API secret | optional |
| `-t` | Topic name | `example_asyncio_avro` |

### Expected Output

```
Produced to example_asyncio_avro [0] @ 0
```

---

## Real-World Use Cases

This pattern (async produce + Avro schema enforcement) is useful anywhere you need:

- **Schema governance** — enforce a contract between producers and consumers; incompatible schema changes are rejected by the Registry before they reach the broker.
- **Async microservices** — produce Avro events from a FastAPI/aiohttp service on the same event loop as the web server, no extra threads needed.
- **Data pipeline ingestion** — serialize structured data with strong typing (Avro) and send it to Kafka asynchronously for high throughput.
- **Event sourcing** — store domain events in a compact binary format with a schema that evolves safely over time.
