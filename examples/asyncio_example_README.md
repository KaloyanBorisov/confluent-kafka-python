# AsyncIO Example

## The Idea

What happens when your Kafka producer and consumer need to share the same thread without blocking each other?

In a traditional synchronous app you'd need two threads — one blocking on `poll()` for the consumer, another calling `flush()` for the producer. With AsyncIO you run both in a **single thread** using cooperative multitasking:

```
Event Loop (single thread)
├── producer task  → produces 10 msgs → commits tx → sleeps → repeats
└── consumer task  → polls for msgs → commits offsets → repeats
```

When the producer does `await producer.flush()`, it yields control to the event loop, which immediately switches to the consumer task. They take turns — no threads, no locks, no blocking.

## Why Transactions?

The example pairs AsyncIO with exactly-once semantics (EOS). In a real app this pattern is used for **stream processing** — consume a message, transform it, produce the result — all as one atomic transaction. If anything fails, the transaction is aborted and nothing is committed on either side:

```
[input topic] → consume → transform → produce → [output topic]
                          (all in one transaction)
```

This is the foundation of building a Kafka Streams-style processor in pure Python with AsyncIO — high throughput, single thread, exactly-once guarantees.

## What is EOS?

**Exactly-Once Semantics (EOS)** — a guarantee that each message is processed and delivered exactly once, even if failures occur.

Without it you get one of two weaker guarantees:

| Guarantee | What happens on failure |
|---|---|
| **At-most-once** | Message may be lost (never retried) |
| **At-least-once** | Message may be duplicated (retried too many times) |
| **Exactly-once** | Message is processed once, no loss, no duplicates |

Two mechanisms work together:

1. **Idempotent producer** — Kafka assigns each message a sequence number. If the broker receives a duplicate due to a retry, it silently discards it. Enabled automatically when you set `transactional.id`.
2. **Transactions** — a group of produce + consumer offset commits are wrapped in a single atomic operation. Either everything commits or nothing does.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        asyncio.run(main())                      │
│                                                                 │
│   signal_handler (SIGINT/SIGTERM) ──► running = False           │
│                                                                 │
│   asyncio.gather()                                              │
│   ┌─────────────────────────┬─────────────────────────────┐    │
│   │    producer_task        │      consumer_task          │    │
│   │                         │                             │    │
│   │  init_transactions()    │  subscribe(topic)           │    │
│   │         │               │      │                      │    │
│   │         ▼               │      ▼                      │    │
│   │  ┌─────────────┐        │  ┌──────────────┐           │    │
│   │  │ loop        │        │  │ loop         │           │    │
│   │  │             │        │  │              │           │    │
│   │  │ begin_tx()  │        │  │ poll(1.0)    │           │    │
│   │  │     │       │        │  │    │         │           │    │
│   │  │ produce()   │        │  │    ▼         │           │    │
│   │  │  x10 msgs   │◄──────►│  │ message?     │           │    │
│   │  │     │  await│  event │  │    │         │           │    │
│   │  │ flush()     │  loop  │  │ store_offset │           │    │
│   │  │     │       │ yields │  │    │         │           │    │
│   │  │ gather()    │        │  │ every 100:   │           │    │
│   │  │  (futures)  │        │  │  commit()    │           │    │
│   │  │     │       │        │  │  position()  │           │    │
│   │  │ commit_tx() │        │  └──────────────┘           │    │
│   │  │     │       │        │                             │    │
│   │  │ sleep(1)◄───┼────────┼── yields to consumer       │    │
│   │  └─────────────┘        │                             │    │
│   │                         │  on_assign  → incremental   │    │
│   │  on failure:            │              assign()       │    │
│   │  abort_tx()             │            → pause()        │    │
│   │  close()                │            → resume()       │    │
│   │                         │  on_revoke → commit()       │    │
│   │                         │  on_lost   → log            │    │
│   │                         │                             │    │
│   │                         │  finally:                   │    │
│   │                         │  unsubscribe() + close()    │    │
│   └─────────────────────────┴─────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘

                        ┌──────────────┐
                        │  Kafka Broker│
                        │              │
              produce ──►  test-topic  │
              consume ◄──              │
                        │  tx offsets  │
                        └──────────────┘
```

Every `await` is a yield point — when the producer awaits `flush()` or `sleep()`, the event loop immediately runs the consumer's `poll()`, and vice versa. No blocking, no threads needed.

## How asyncio.gather() Works

`asyncio.gather()` does **not** make the tasks wait for each other:

```python
producer_task = asyncio.create_task(run_producer())  # schedules on loop, doesn't run yet
consumer_task = asyncio.create_task(run_consumer())  # schedules on loop, doesn't run yet
await asyncio.gather(producer_task, consumer_task)   # waits for BOTH to finish
```

`create_task()` registers both tasks in the loop's ready queue. `gather()` only means: *"don't return until all these tasks are done"*. It doesn't coordinate them.

The loop drives everything:

```
Loop iteration 1: run producer → hits await flush() → suspend producer
Loop iteration 2: run consumer → hits await poll()  → suspend consumer
Loop iteration 3: producer I/O ready → resume producer → hits await sleep() → suspend
Loop iteration 4: consumer I/O ready → resume consumer → hits await poll() → suspend
...
```

The tasks don't wait for each other and don't yield to each other — each yields to the **loop**, and the loop decides what runs next:

```
producer ──await──► LOOP ◄──await── consumer
                      │
                   picks whoever
                   is ready next
```

Only one task runs at a time. `gather()` wakes up `main()` only when both are done.

## Method by Method

### `error_cb`, `throttle_cb`, `stats_cb`

Async callbacks registered with the Kafka client. The broker calls these automatically when errors, throttling, or periodic stats occur. Being `async` means they run safely on the event loop without blocking it.

### `configure_common(conf)`

Shared config builder used by both producer and consumer. Takes a partial config dict, injects the broker address and the three callbacks above, and returns the merged config. Avoids repeating connection settings in two places.

### `run_producer()`

| Step | What happens |
|---|---|
| `AIOProducer(...)` | Creates producer with `transactional.id` — enables EOS |
| `await init_transactions()` | Registers this producer with the broker as a transactional participant |
| `await begin_transaction()` | Opens a new transaction |
| `await producer.produce(...)` x10 | Queues 10 messages, each returns a `Future` |
| `await producer.flush()` | Forces messages into flight before waiting for delivery |
| `await asyncio.gather(*futures)` | Waits for all 10 delivery confirmations concurrently |
| `await commit_transaction()` | Atomically commits all 10 messages + any offset commits |
| `await asyncio.sleep(1)` | Yields to the event loop (consumer runs here) |
| `abort_transaction()` in finally | Rolls back if anything raised an exception |
| `await producer.close()` | Stops background threads, flushes, closes connections |

### `run_consumer()`

| Step | What happens |
|---|---|
| `AIOConsumer(...)` | Creates consumer with manual commit (`enable.auto.commit=false`) |
| `on_assign(consumer, partitions)` | Called on rebalance — manually calls `incremental_assign()`, then demos `pause()`/`resume()` |
| `on_revoke(consumer, partitions)` | Called before losing partitions — commits offsets so no work is lost |
| `on_lost(consumer, partitions)` | Called when partitions are lost unexpectedly — just logs (can't commit) |
| `await consumer.subscribe(...)` | Subscribes with the three rebalance callbacks above |
| `await consumer.poll(1.0)` | Non-blocking poll — yields to event loop while waiting |
| `await consumer.store_offsets(message)` | Marks this offset for the next commit (doesn't commit yet) |
| every 100 msgs: `await consumer.commit()` | Flushes stored offsets to the broker |
| every 100 msgs: `await consumer.position(...)` | Reads current position for logging |
| `await consumer.unsubscribe()` | Leaves the consumer group gracefully |
| `await consumer.close()` | Closes connections, stops background tasks |

### `signal_handler(*_)`

Handles `Ctrl+C` (`SIGINT`) and `SIGTERM`. Sets `running = False` — both loops check this flag on every iteration and exit cleanly, allowing their `finally` blocks to run.

### `main()`

Registers signal handlers, launches both tasks with `create_task()`, then waits for both with `asyncio.gather()`. The two tasks run interleaved on a single thread.

### Entry point

```python
asyncio.run(main())
```

Creates a fresh event loop, runs `main()` until completion, then closes the loop. The `CancelledError` catch handles tasks cancelled during shutdown.

## What are "AsyncIO Patterns"?

`# AsyncIO Pattern:` is a label used in the code comments — not an official term. The author added it to mark lines that demonstrate a specific AsyncIO technique worth copying in your own app:

| Label | What it points at |
|---|---|
| Non-blocking producer with thread pool | `AIOProducer` uses a `ThreadPoolExecutor` internally |
| Async transaction lifecycle | All tx operations are `await`-able |
| Batched async produce with concurrent futures | Collect futures first, then `gather()` them |
| Event loop safe callbacks | Callbacks are `async def`, scheduled on the loop |
| Signal handling for graceful shutdown | `signal.signal()` + a boolean flag |
| Proper async cleanup | `finally` blocks with `await close()` |

## Real-World Use Cases

**Stream Processing Pipeline** — read from one topic, transform, write to another, atomically:
```
[orders topic] → validate/enrich → [processed-orders topic]
```

**Event-Driven Microservices** — a FastAPI/aiohttp service that both consumes commands and produces events on the same event loop as the web server, no extra threads needed.

**Real-Time Aggregation** — consume a high-volume stream, aggregate in memory, flush results transactionally so partial aggregates are never published.

**Dead Letter Queue (DLQ) Handler** — consume failed messages, reprocess, produce to main topic or DLQ as one transaction so a message never disappears without a trace.

**Change Data Capture (CDC) Relay** — consume database change events, filter/transform, produce to downstream topics with exactly-once guarantees.

The common thread: anywhere you need to **consume + produce atomically** in an async Python app, this is the pattern.

## Running

```bash
python examples/asyncio_example.py $BOOTSTRAP_SERVERS test-topic
```

Press `Ctrl+C` to shut down both tasks gracefully.
