# EOS Transactions Example

Demonstrates a **consume-transform-produce** loop with **Exactly-Once Semantics (EOS)** using Kafka's transactional API. Messages are read from an input topic, Base64-encoded, and written to an output topic — with consumer offsets and produced messages committed atomically in the same transaction.

For background, see: [Transactions in Apache Kafka](https://www.confluent.io/blog/transactions-apache-kafka/)

## Usage

```sh
python eos_transactions.py -b <brokers> -t <input-topic> [options]
```

### Arguments

| Flag | Required | Default | Description |
|---|---|---|---|
| `-b` | yes | — | Bootstrap broker(s), e.g. `localhost:9092` |
| `-t` | yes | — | Input topic to consume from |
| `-o` | no | `output_topic` | Output topic to produce transformed messages to |
| `-p` | no | `0` | Input partition to consume from |
| `-g` | no | random UUID | Consumer group ID |

### Example

```sh
python eos_transactions.py -b localhost:9092 -t input_topic -o output_topic -p 0
```

## What it does

1. **Consumer** subscribes to a single partition of the input topic via `assign()` (not `subscribe()`). Auto-commit is disabled — offsets are committed only within transactions.
2. **Producer** is initialized with a `transactional.id`, calls `init_transactions()`, then `begin_transaction()` before the main loop.
3. **Transform:** Each message's key and value are Base64-encoded by `process_input()`.
4. **Produce:** The transformed message is written to the output topic within the open transaction.
5. **Commit every 100 messages:** Consumer positions are sent to the transaction via `send_offsets_to_transaction()`, then `commit_transaction()` is called and a new transaction begins. Committing both the output messages and the input offsets in a single transaction is what provides EOS.
6. **Final commit:** When all partitions reach EOF, the remaining messages are committed in a final transaction and the consumer is closed.

## Key design decisions

- **`assign()` instead of `subscribe()`** — prior to KIP-447, each input partition requires its own transactional producer. Using `assign()` pins the consumer to a single partition to keep this example simple.
- **`enable.auto.commit: False`** — offsets must only advance inside a committed transaction, never independently.
- **`enable.partition.eof: True`** — allows the consumer to detect when all input has been consumed so the loop can exit cleanly.
- **Assumption:** No duplicate records in the input topic. EOS guarantees begin at the producer boundary; deduplication of source data is the application's responsibility.
