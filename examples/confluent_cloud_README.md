# Confluent Cloud Example

A simple end-to-end example that produces 10 messages to a Confluent Cloud topic and consumes them back using SASL/SSL authentication.

## Prerequisites

- A [Confluent Cloud](https://www.confluent.io/confluent-cloud/) account
- Bootstrap servers, API key, and API secret from the Confluent Cloud web interface
- The topic `python-test-topic` created in advance (auto-creation is disabled in Confluent Cloud)

Create the topic using the Confluent CLI:

```sh
confluent kafka topic create python-test-topic
```

## Setup

```sh
python -m venv ccloud_example
source ccloud_example/bin/activate
pip install confluent_kafka
```

## Configuration

Edit `confluent_cloud.py` and replace the placeholder values in both the Producer and Consumer configs:

| Placeholder | Description |
|---|---|
| `<ccloud bootstrap servers>` | Bootstrap server URL from Confluent Cloud |
| `<ccloud key>` | API key (SASL username) |
| `<ccloud secret>` | API secret (SASL password) |

## Run

```sh
python confluent_cloud.py
```

## What it does

1. **Produces** 10 messages (`python test value nr 0` through `9`) to `python-test-topic`, printing a delivery confirmation for each.
2. **Consumes** messages from `python-test-topic` starting from the earliest offset, printing each message value.
3. Press `Ctrl+C` to stop the consumer. It will cleanly leave the consumer group and commit final offsets.

## Key behaviors

- **Error handling:** Fatal errors (`_ALL_BROKERS_DOWN`, `_AUTHENTICATION`) raise an exception and terminate the app. All other errors are logged and the client attempts automatic recovery.
- **Consumer group:** A random UUID is used as `group.id` on each run, so the consumer always starts from the beginning of the topic (`auto.offset.reset: earliest`).
- **Delivery reports:** The producer uses a callback (`acked`) to confirm each message was successfully delivered or log a failure.
