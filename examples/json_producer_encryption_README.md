# JSON Producer with Field-Level Encryption

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

### KMS key

A KMS key must exist before running the script. The script references it via three parameters:

| Parameter | Description | Example |
|---|---|---|
| `-kn` | KEK name — logical name stored in Schema Registry | `my-kek` |
| `-kt` | KMS type | `aws-kms`, `azure-kms`, `gcp-kms`, `hcvault`, `local-kms` |
| `-ki` | KMS key ID — the actual key reference in the KMS | `arn:aws:kms:us-east-2:123456789012:key/abc-123` |

The KMS credentials must be available to the process via environment variables (e.g. `~/.aws/credentials` for AWS) or mounted into the container. Placeholder credentials in `.env` will silently override the credential chain and cause KMS calls to fail, resulting in unencrypted output.

### Python dependencies

```bash
pip install confluent-kafka[schemaregistry,encryption]
```

</span>

A Kafka producer example that serializes `User` records as JSON and applies **field-level encryption** to PII-tagged fields before publishing to a topic.

## Overview

This script demonstrates how to use `JSONSerializer` together with Confluent's Data Contract encryption rules to transparently encrypt sensitive fields in Kafka messages. The encryption is enforced via Schema Registry rules, so any consumer must have access to the KMS key to decrypt the data.

## How it works

### 1. KMS driver registration

All supported Key Management Service drivers are registered at startup:

- AWS KMS (`aws-kms`)
- Azure Key Vault (`azure-kms`)
- GCP Cloud KMS (`gcp-kms`)
- HashiCorp Vault (`hcvault`)
- Local KMS (for testing)

The `FieldEncryptionExecutor` is also registered — it performs the actual field-level encryption during serialization.

### 2. Schema with PII tags

The `User` JSON Schema marks the `name` field with `"confluent:tags": ["PII"]`:

```json
"name": {
  "description": "User's name",
  "type": "string",
  "confluent:tags": ["PII"]
}
```

Only fields carrying the `PII` tag will be encrypted.

### 3. Encryption rule

A `TRANSFORM` rule (`WRITEREAD` mode) is registered against the topic's value subject in Schema Registry. It targets all `PII`-tagged fields and uses the provided KEK (Key Encryption Key) and KMS credentials to encrypt them on write and decrypt on read.

### 4. Schema registration

The schema, with the encryption rule attached, is registered in Schema Registry under `<topic>-value`. The serializer is configured with `use.latest.version: True` so it picks up the pre-registered schema rather than creating a new one.

### 5. Producer loop

The script prompts interactively for user input:

| Field | Serialized | Notes |
|---|---|---|
| `name` | Yes — encrypted | Tagged `PII`; encrypted before writing to Kafka |
| `favorite_number` | Yes — plaintext | Not tagged |
| `favorite_color` | Yes — plaintext | Not tagged |
| `address` | **No** | Stored as `_address`; excluded from `user_to_dict` intentionally |

The `address` field is collected but never serialized — it is a privacy guard at the object level, independent of the KMS encryption.

## Schema Registry requirements

Schema rules (used for field-level encryption) require a **Stream Governance Advanced** package. This affects all encryption example scripts:

| Script | Affected |
|---|---|
| `json_producer_encryption.py` | Yes |
| `json_consumer_encryption.py` | Yes |
| `avro_producer_encryption.py` | Yes |
| `avro_consumer_encryption.py` | Yes |
| `protobuf_producer_encryption.py` | Yes |
| `protobuf_consumer_encryption.py` | Yes |

### Confluent Cloud

The default **Essentials** package does not support schema rules. Running any of the above scripts against an Essentials Schema Registry will fail with:

```
SchemaRegistryError: Upgrade to Stream Governance Advanced package to use schema rules (HTTP status code 403, SR code 40302)
```

**Fix:** In the Confluent Cloud console go to **Environments → your environment → Stream Governance → Upgrade** and select **Advanced**.

### Self-hosted (Docker)

The open-source `cp-schema-registry` image does not include the DEK Registry or rule enforcement. You must:

1. Enable the DEK Registry extension in `docker-compose.yml`:
   ```yaml
   SCHEMA_REGISTRY_RESOURCE_EXTENSION_CLASS: io.confluent.dekregistry.DekRegistryResourceExtension
   ```
2. Have a valid **Confluent Platform license** — without it the rule set is silently dropped and fields are written in plaintext.


## Usage

```bash
python json_producer_encryption.py \
  -b <bootstrap-servers> \
  -s <schema-registry-url> \
  -t <topic-name> \
  -kn <kek-name> \
  -kt <kms-type> \
  -ki <kms-key-id>
```

### Arguments

| Flag | Required | Description |
|---|---|---|
| `-b` | Yes | Bootstrap broker(s), e.g. `localhost:9092` |
| `-s` | Yes | Schema Registry URL, e.g. `http://localhost:8081` |
| `-t` | No | Topic name (default: `example_serde_json`) |
| `-kn` | Yes | KEK name registered in Schema Registry |
| `-kt` | Yes | KMS type: `aws-kms`, `azure-kms`, `gcp-kms`, `hcvault` |
| `-ki` | Yes | KMS key ID, e.g. an AWS ARN |

### Example (AWS KMS)

```bash
python json_producer_encryption.py \
  -b localhost:9092 \
  -s http://localhost:8081 \
  -t my-topic \
  -kn my-kek \
  -kt aws-kms \
  -ki arn:aws:kms:us-east-1:123456789012:key/abc-123
```

### Example (local KMS, for testing)

```bash
python json_producer_encryption.py \
  -b localhost:9092 \
  -s http://localhost:8081 \
  -t my-topic \
  -kn my-kek \
  -kt local-kms \
  -ki secret
```

## KMS credentials

Credentials can be supplied in two ways:

**Via environment variables** (recommended):

```bash
# AWS
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=yyy

# GCP
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json

# Azure
export AZURE_TENANT_ID=xxx
export AZURE_CLIENT_ID=yyy
export AZURE_CLIENT_SECRET=zzz
```

**Via `rule_conf` in the script** (for programmatic use):

```python
rule_conf = {'secret.access.key': 'xxx', 'access.key.id': 'yyy'}
```

## Related examples

- `json_consumer_encryption.py` — consumes and decrypts the messages produced by this script
- `json_producer.py` — same producer without encryption
