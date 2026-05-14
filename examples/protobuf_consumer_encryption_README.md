# Protobuf Consumer with Field-Level Encryption

A Kafka consumer example that deserializes Protobuf-encoded `User` records and transparently **decrypts PII-tagged fields** using the KEK stored in Schema Registry.

<span style="color:red">

## Prerequisites

### Kafka broker

A running Kafka broker is required. For local development the `cp-all-in-one-kraft` Docker Compose stack provides one at `broker:29092` (internal) or `localhost:9092` (host).

### Schema Registry — licensing restrictions

Field-level encryption relies on **schema rules** and the **DEK Registry**, both of which are gated behind a paid tier. The restrictions differ depending on where Schema Registry is running.

#### Confluent Cloud

The default **Essentials** Stream Governance package does **not** support schema rules. Attempting to register a schema with a `RuleSet` will fail immediately with:

```
SchemaRegistryError: Upgrade to Stream Governance Advanced package to use schema rules
(HTTP status code 403, SR code 40302)
```

This affects every encryption script in this repo (`json_*`, `avro_*`, `protobuf_*` — both producer and consumer variants). The fix is a single package upgrade:

> Confluent Cloud console → **Environments → your environment → Stream Governance → Upgrade → Advanced**

#### Self-hosted Docker (`cp-schema-registry`)

Two separate conditions must both be met:

1. **DEK Registry extension must be enabled.** Without it the `/dek-registry` REST endpoint returns 404 and KEKs cannot be registered. Add this to your `docker-compose.yml`:
   ```yaml
   SCHEMA_REGISTRY_RESOURCE_EXTENSION_CLASS: io.confluent.dekregistry.DekRegistryResourceExtension
   ```

2. **A valid Confluent Platform license is required.** Even with the extension enabled, the open-source image enforces a license check at runtime. Without a license, Schema Registry silently drops the `ruleSet` from every schema registration — no error is thrown, but the encryption rule is never stored, and fields are written to Kafka in plaintext. You can verify this by checking Schema Registry logs for:
   ```
   WARN RuleSets are only supported by Confluent Enterprise and Confluent Cloud
   ```

### Protobuf generated classes

The script imports `protobuf/user_pb2.py` which must be generated from the `.proto` definition before running. From the `examples/` directory:

```bash
make
```

This requires the `protoc` compiler to be installed. See the [Protocol Buffers Python tutorial](https://developers.google.com/protocol-buffers/docs/pythontutorial) for installation instructions.

### KMS key

The consumer does not register any KEK or rule — it reads the encryption rule from the schema fetched from Schema Registry and uses the KMS key referenced in that rule to decrypt. The KMS credentials must be available to the process via environment variables (e.g. `~/.aws/credentials` for AWS) or mounted into the container.

### Python dependencies

```bash
pip install confluent-kafka[schemaregistry,encryption,protobuf]
```

</span>

## Overview

This script demonstrates how to use `ProtobufDeserializer` together with Confluent's Data Contract encryption rules to transparently decrypt PII-tagged fields when consuming messages. The decryption rule and schema are fetched automatically from Schema Registry via the schema ID embedded in each message.

## How it works

### 1. KMS driver registration

All supported Key Management Service drivers are registered at startup so the deserializer can use whichever KMS type is referenced in the schema rule.

### 2. Deserializer with schema registry client

The `ProtobufDeserializer` is initialised with the generated Protobuf class and the Schema Registry client. The client is used to fetch the schema and its associated encryption rule on first message consumption:

```python
protobuf_deserializer = ProtobufDeserializer(
    user_pb2.User,
    {'use.deprecated.format': False},
    schema_registry_client
)
```

### 3. Transparent decryption

On each message the deserializer:
1. Fetches the schema (and its `RuleSet`) from Schema Registry using the schema ID embedded in the message
2. Identifies fields tagged `PII`
3. Calls the KMS to unwrap the DEK
4. Decrypts the field value in place before deserializing the Protobuf message

The `name` field arrives encrypted from Kafka and is returned in plaintext to your application.

## Usage

```bash
python protobuf_consumer_encryption.py \
  -b <bootstrap-servers> \
  -s <schema-registry-url> \
  -t <topic-name> \
  -g <consumer-group>
```

### Arguments

| Flag | Required | Description |
|---|---|---|
| `-b` | Yes | Bootstrap broker(s), e.g. `localhost:9092` |
| `-s` | Yes | Schema Registry URL, e.g. `http://localhost:8081` |
| `-t` | No | Topic name (default: `example_serde_protobuf`) |
| `-g` | No | Consumer group (default: `example_serde_protobuf`) |

### Example

```bash
python protobuf_consumer_encryption.py \
  -b localhost:9092 \
  -s http://localhost:8081 \
  -t my-topic \
  -g my-group
```

## Related examples

- `protobuf_producer_encryption.py` — produces the encrypted messages this script consumes
- `protobuf_consumer.py` — same consumer without encryption
