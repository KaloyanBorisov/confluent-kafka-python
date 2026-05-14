# Context Manager Example

Demonstrates how to use Python context managers (`with` statements) with `AdminClient`, `Producer`, and `Consumer` to ensure proper resource cleanup without manual teardown calls.

## Usage

```sh
python context_manager_example.py <bootstrap-brokers>
```

Example:

```sh
python context_manager_example.py localhost:9092
```

## What it does

The script runs three sequential examples against the topic `context-manager-example`.

### 1. AdminClient

```python
with AdminClient(conf) as admin:
    ...
```

Creates the topic `context-manager-example` with 1 partition and replication factor 1. The `AdminClient` is automatically destroyed when the `with` block exits — no manual cleanup needed.

### 2. Producer

```python
with Producer(conf) as producer:
    ...
```

Produces 5 messages (`Message 0` through `Message 4`) with keys `key-0` through `key-4`. A delivery callback prints the topic, partition, and offset for each successfully delivered message. When the `with` block exits, the producer **automatically flushes** all pending messages before being destroyed — no need to call `producer.flush()` manually.

### 3. Consumer

```python
with Consumer(conf) as consumer:
    ...
```

Subscribes to the topic and consumes the 5 messages produced in the previous step, printing key, value, partition, and offset for each. When the `with` block exits, `consumer.close()` is called automatically — the consumer leaves its group and commits final offsets cleanly.

## Resource cleanup summary

| Client | What happens on `with` block exit |
|---|---|
| `AdminClient` | Client destroyed |
| `Producer` | Pending messages flushed, then client destroyed |
| `Consumer` | `close()` called (leaves group, commits offsets), then client destroyed |
