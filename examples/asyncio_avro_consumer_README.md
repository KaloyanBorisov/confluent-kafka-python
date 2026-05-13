# AsyncIO Avro Consumer Example

Pairs with `asyncio_avro_producer.py` — run the producer first to populate the topic, then run this consumer to read the messages back.

## The Idea

The consumer mirrors the producer side exactly, but in reverse. Where the producer **serializes** a Python dict into Avro bytes and sends them to Kafka, the consumer **deserializes** those Avro bytes back into a Python dict.

```
Producer side                          Consumer side
─────────────────────────────────────────────────────
Python dict                            Python dict
    │                                      ▲
    ▼                                      │
AsyncAvroSerializer                  AsyncAvroDeserializer
    │  (schema → Schema Registry)          │  (schema ID → Schema Registry)
    ▼                                      │
Avro bytes ──► Kafka topic ──────────► Avro bytes
```

Both sides are async — the Schema Registry lookup (register or fetch schema) is a non-blocking `await`, so it never blocks the event loop.

## Producer vs Consumer — Side by Side

| Step | Producer (`asyncio_avro_producer.py`) | Consumer (`asyncio_avro_consumer.py`) |
|---|---|---|
| Schema Registry client | `AsyncSchemaRegistryClient` | `AsyncSchemaRegistryClient` |
| Serializer / Deserializer | `await AsyncAvroSerializer(sr_client, schema_str=...)` | `await AsyncAvroDeserializer(sr_client)` |
| Schema source | You provide the schema string | Fetched automatically from Schema Registry using the ID embedded in the message |
| Kafka client | `AIOProducer` | `AIOConsumer` |
| Core operation | `await avro_serializer(value, ctx)` → bytes | `await avro_deserializer(msg.value(), ctx)` → dict |
| Send / Receive | `await producer.produce(topic, value=serialized_value)` | `await consumer.poll(1.0)` |
| Await delivery | `await delivery_future` | n/a |
| Cleanup | `await producer.flush()` + `await producer.close()` | `await consumer.unsubscribe()` + `await consumer.close()` |

## Key Difference: No Schema Needed on Consumer Side

The producer registers the schema with Schema Registry and embeds the schema ID in every message:

```
[ magic byte ][ schema ID (4 bytes) ][ avro payload ]
```

The consumer reads that schema ID from the message and fetches the schema automatically — you don't need to provide it:

```python
# Producer: must provide schema
avro_serializer = await AsyncAvroSerializer(sr_client, schema_str=schema_str)

# Consumer: no schema needed — fetched from Schema Registry by ID
avro_deserializer = await AsyncAvroDeserializer(sr_client)
```

## Method by Method

### Schema Registry setup (same on both sides)

```python
sr_conf = {'url': args.schema_registry}
sr_client = AsyncSchemaRegistryClient(sr_conf)
```

Creates an async Schema Registry client. All HTTP calls to Schema Registry are non-blocking awaits.

### `await AsyncAvroDeserializer(sr_client)`

Instantiates the deserializer. The `await` here allows the deserializer to perform any async initialization (e.g. prefetching schemas). Compare with the producer:

```python
# Producer
avro_serializer = await AsyncAvroSerializer(sr_client, schema_str=schema_str)

# Consumer
avro_deserializer = await AsyncAvroDeserializer(sr_client)
```

### `await consumer.subscribe([topic])`

Async subscribe — registers the consumer group and partition assignment with the broker.

### `await consumer.poll(1.0)`

Non-blocking poll. Yields to the event loop for up to 1 second while waiting for a message. Compare with the producer's `await producer.produce()` which also yields while waiting for broker acknowledgement.

### `await avro_deserializer(msg.value(), SerializationContext(...))`

Async deserialization — reads the schema ID from the message bytes, fetches the schema from Schema Registry if not cached, and deserializes the Avro bytes into a Python dict. The Schema Registry fetch is an async HTTP call that yields to the event loop. Compare with the producer:

```python
# Producer: Python dict → Avro bytes (registers schema if needed)
serialized_value = await avro_serializer(value, SerializationContext(topic, MessageField.VALUE))

# Consumer: Avro bytes → Python dict (fetches schema if not cached)
user = await avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
```

### Signal handling + `running` flag

```python
def signal_handler(*_):
    global running
    running = False
```

Same pattern as `asyncio_example.py` — `Ctrl+C` sets `running = False`, the poll loop exits cleanly, and the `finally` block runs. Compare with the producer which uses `try/finally` around a single produce operation.

### `finally` cleanup

```python
await consumer.unsubscribe()  # leaves consumer group gracefully
await consumer.close()        # closes connections, stops background tasks
```

Consumer cleanup has one extra step compared to the producer — `unsubscribe()` explicitly leaves the consumer group so other consumers can immediately rebalance and take over the partitions. The producer equivalent is:

```python
await producer.flush()   # ensures all buffered messages are delivered
await producer.close()   # closes connections
```

## Running

Run the producer first to put messages in the topic:

```bash
python examples/asyncio_avro_producer.py -b $BOOTSTRAP_SERVERS -s $SCHEMA_REGISTRY_URL
```

Then run the consumer to read them back:

```bash
python examples/asyncio_avro_consumer.py -b $BOOTSTRAP_SERVERS -s $SCHEMA_REGISTRY_URL
```

Press `Ctrl+C` to stop the consumer gracefully.
