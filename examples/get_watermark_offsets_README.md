# Get Watermark Offsets Example

Shows the **committed offsets** and **consumer lag** for a given consumer group across one or more topics. Useful for monitoring how far behind a consumer group is from the latest messages.

## Usage

```sh
python3 get_watermark_offsets.py <brokers> <group.id> <topic> [topic2 ...]
```

### Example

```sh
python3 get_watermark_offsets.py localhost:9092 my-consumer-group test-topic
```

### Example output

```
Topic [Partition]                                   Committed        Lag
========================================================================
test-topic [0]                                             42         58
test-topic [1]                                              -        100
```

## What it does

For each topic and partition:

1. Fetches the topic's partition metadata.
2. Queries the **committed offset** for the given consumer group via `committed()`.
3. Fetches the **low and high watermark offsets** via `get_watermark_offsets()` (live, not cached).
4. Calculates and prints lag:

| Committed offset | Lag shown |
|---|---|
| Valid offset | `high watermark − committed offset` |
| No committed offset | `high watermark − low watermark` (total message count as a proxy) |
| No high watermark | `no hwmark` |

Lag is simply `high watermark − committed offset` — how many messages the consumer group still hasn't read. That's the main thing this script is for: a quick health check on whether your consumers are keeping up with producers.

## Key notes

- The consumer created here **does not join the consumer group** — it only uses the `group.id` to query committed offsets via `committed()`.
- Multiple topics can be passed as additional arguments and will all be reported in the same table.
- Lag may be slightly overstated when compaction or record deletions have reduced the actual message count.
