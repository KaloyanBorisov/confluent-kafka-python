# adminapi_logger.py — Kafka AdminClient Logger Example

A minimal script demonstrating how to wire a custom Python logger into a `confluent-kafka` `AdminClient` so that internal librdkafka log messages are routed through Python's `logging` module.

## Usage

```
python adminapi_logger.py <broker>
```

## What it demonstrates

### Custom logger setup

A standard `logging.Logger` named `'AdminClient'` is created at `DEBUG` level with a timestamped formatter writing to stderr:

```python
logger = logging.getLogger('AdminClient')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)
```

### Passing the logger to AdminClient

The logger is passed via the `logger=` constructor argument, which redirects all internal librdkafka log messages through your Python logger:

```python
a = AdminClient({'bootstrap.servers': broker, 'debug': 'all'}, logger=logger)
```

`'debug': 'all'` enables verbose librdkafka output so log traffic is visible. Alternatively, the logger can be passed inside the config dict:

```python
a = AdminClient({'bootstrap.servers': broker, 'debug': 'all', 'logger': logger})
```

When both are supplied, the keyword argument takes precedence.

### The polling loop — the essential piece

`AdminClient` operations return a `Future` immediately. While waiting for the result, `a.poll()` must be called to deliver buffered log messages from librdkafka to your Python logger. Without polling, no log output appears until the future resolves (or at all).

```python
future = a.list_consumer_groups(request_timeout=10)

while not future.done():
    a.poll(0.1)   # delivers pending log messages on each iteration

result = future.result()

a.poll(0)         # flush any remaining log messages after the future is done
```

## Example run

The following is an annotated walkthrough of a real run against a single-broker Confluent Platform cluster.

### Phase 1 — Bootstrap connection

librdkafka creates a temporary bootstrap broker handle (NodeId `-1`). Its only job is to discover the real cluster topology.

```
DEBUG BROKER   broker:29092/bootstrap: Added new broker with NodeId -1
DEBUG CONNECT  broker:29092/bootstrap: Selected for cluster connection: bootstrap servers added
DEBUG STATE    broker:29092/bootstrap: Broker changed state INIT -> TRY_CONNECT -> CONNECT
DEBUG CONNECT  broker:29092/bootstrap: Connecting to ipv4#172.20.0.4:29092 (plaintext) with socket 8
DEBUG CONNECT  broker:29092/bootstrap: Connected to ipv4#172.20.0.4:29092
```

Connection is plaintext (no TLS/SASL). The bootstrap handle is always ephemeral and torn down once metadata is received.

### Phase 2 — API version negotiation

The bootstrap broker sends `ApiVersionRequest` and gets a response in ~1.24ms. librdkafka enables the features it needs based on what the broker supports:

```
DEBUG SEND         broker:29092/bootstrap: Sent ApiVersionRequest (v3, 68 bytes, CorrId 1)
DEBUG RECV         broker:29092/bootstrap: Received ApiVersionResponse (v3, 495 bytes, CorrId 1, rtt 1.24ms)
DEBUG APIVERSION   broker:29092/bootstrap: Enabling feature MsgVer1
DEBUG APIVERSION   broker:29092/bootstrap: Enabling feature MsgVer2
DEBUG APIVERSION   broker:29092/bootstrap: Enabling feature BrokerBalancedConsumer
DEBUG APIVERSION   broker:29092/bootstrap: Enabling feature LZ4
DEBUG APIVERSION   broker:29092/bootstrap: Enabling feature ZSTD
DEBUG APIVERSION   broker:29092/bootstrap: Enabling feature IdempotentProducer
DEBUG APIVERSION   broker:29092/bootstrap: Enabling feature SaslAuthReq
DEBUG STATE        broker:29092/bootstrap: Broker changed state APIVERSION_QUERY -> UP
```

The broker also advertised `ConsumerGroupHeartbeat` (key 68) and `ConsumerGroupDescribe` (key 69), which are the KIP-848 next-gen consumer group protocol APIs. Unknown keys 74, 75, 80, 81 are newer Confluent-specific APIs not yet recognized by this librdkafka version — harmless.

### Phase 3 — Metadata fetch and real broker registration

Now UP, the bootstrap broker fetches cluster metadata (brokers only, no topics) in ~0.28ms:

```
DEBUG SEND      broker:29092/bootstrap: Sent MetadataRequest (v12, 26 bytes, CorrId 2)
DEBUG RECV      broker:29092/bootstrap: Received MetadataResponse (v12, 52 bytes, CorrId 2, rtt 0.28ms)
DEBUG METADATA  broker:29092/bootstrap: ClusterId: MkU3OEVBNTcwNTJENDM2Qk, ControllerId: 1
DEBUG METADATA  broker:29092/bootstrap: 1 brokers, 0 topics
DEBUG METADATA  broker:29092/bootstrap:   Broker #0/1: broker:29092 NodeId 1
DEBUG BROKER    broker:29092/1: Added new broker with NodeId 1
DEBUG DESTROY   Sending TERMINATE to broker:29092/bootstrap
```

librdkafka creates a permanent broker handle `/1` (NodeId 1) and terminates the bootstrap handle — it has served its purpose.

### Phase 4 — LISTCONSUMERGROUPS state machine

The admin op cycles through states on the main thread while the broker connection is established:

| State | What's happening |
|---|---|
| `initializing` | Op created, no broker ready yet |
| `waiting for a valid list of brokers` | Bootstrap UP but no metadata yet |
| `waiting for a valid list of brokers` | Metadata arrived, controller known |
| `initializing` | Permanent broker `/1` registered, op restarted |
| `waiting for broker` (×3) | `/1` going through `INIT → TRY_CONNECT → CONNECT` |

This retry loop is normal — the admin op polls until its target broker reaches `UP`.

### Phase 5 — Result

The `ListGroups` request resolves and the result is printed before the permanent broker even finishes its own ApiVersion handshake (the future was answered during the connection window):

```
========================= List consumer groups result Start =========================
9 consumer groups
    id: example_serde_json                        is_simple: False state: ConsumerGroupState.EMPTY
    id: example_serde_avro                        is_simple: False state: ConsumerGroupState.EMPTY
    id: example_asyncio_avro                      is_simple: False state: ConsumerGroupState.EMPTY
    id: example_serde_protobuf                    is_simple: False state: ConsumerGroupState.EMPTY
    id: test-group                                is_simple: False state: ConsumerGroupState.EMPTY
    id: test-topic_545                            is_simple: False state: ConsumerGroupState.EMPTY
    id: context-manager-example-group             is_simple: False state: ConsumerGroupState.EMPTY
    id: _confluent-controlcenter-7-9-6-1          is_simple: False state: ConsumerGroupState.STABLE
    id: _confluent-controlcenter-7-9-6-1-command  is_simple: False state: ConsumerGroupState.STABLE
0 errors
========================= List consumer groups result End =========================
```

The two `_confluent-controlcenter-*` groups are `STABLE` because Control Center is running. All others are `EMPTY` — leftover group metadata from previous example script runs. Kafka retains them until they expire (default 7 days via `offsets.retention.minutes`).

**Total wall time: ~10ms** — bootstrap connect + metadata + ListGroups all completed within a single `poll(0.1)` tick. No errors anywhere.
