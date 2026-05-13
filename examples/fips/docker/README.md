# FIPS Compliance — Docker Setup

## What is FIPS?

FIPS (Federal Information Processing Standards) 140-2/140-3 are US government cryptographic standards required for federal procurement. When enabled, only FIPS-approved algorithms (e.g. AES, SHA-2, RSA) are permitted — non-compliant ciphers like ChaCha20 are rejected at the TLS level.

This client supports both standards. Use OpenSSL 3.x with the matching FIPS provider:

| Standard | FIPS Provider Version |
|---|---|
| FIPS 140-2 | 3.0.8 |
| FIPS 140-3 | 3.1.2 |

For new deployments use **FIPS 140-3** — FIPS 140-2 certificates issued after September 21, 2026 will no longer be accepted for federal procurement.

---

## How FIPS mode works

FIPS is enforced at the OpenSSL level, not in Kafka config. The Confluent Kafka Python client uses librdkafka which uses OpenSSL under the hood. Two steps are required:

**1. Enable the OpenSSL FIPS provider**

Point OpenSSL at a compiled FIPS module and a FIPS-enabled config file via environment variables:

```bash
OPENSSL_CONF="/path/to/fips/openssl.cnf"
OPENSSL_MODULES="/path/to/fips/module/lib/"
```

The `openssl.cnf` must include the `fipsmodule.cnf` and set `fips=yes` as the default property so OpenSSL only uses FIPS-approved algorithm implementations:

```
config_diagnostics = 1
openssl_conf = openssl_init

.include /usr/local/ssl/fipsmodule.cnf

[openssl_init]
providers = provider_sect
alg_section = algorithm_sect

[provider_sect]
fips = fips_sect

[algorithm_sect]
default_properties = fips=yes
```

**2. Enable `fips` and `base` providers in the client**

OpenSSL requires the `base` provider for non-crypto utilities (encoding etc.) that are not included in the FIPS provider. Both must be enabled in the Kafka client config:

```python
{'ssl.providers': 'fips,base'}
```

---

## Building the FIPS provider module

```bash
git clone https://github.com/openssl/openssl
cd openssl
git checkout openssl-3.1.2   # or openssl-3.0.8 for FIPS 140-2
./Configure enable-fips
make install_fips
```

This produces two files:
- `fips.so` (Linux) / `fips.dylib` (Mac) / `fips.dll` (Windows) — the FIPS module
- `fipsmodule.cnf` — the FIPS config to include in `openssl.cnf`

---

## Docker setup

The Docker environment spins up a two-container KRaft cluster (no ZooKeeper) with a SASL_SSL broker, providing a realistic FIPS test target.

### Architecture

```
controller  (cp-kafka:7.9.6)  — KRaft quorum, plaintext internal listener
broker      (cp-kafka:7.9.6)  — SASL_SSL listener, mutual TLS + SASL/PLAIN auth
```

### 1. Generate TLS certificates

```bash
cd secrets && bash generate_certificates.sh
```

This creates a self-signed CA, a server keystore/truststore (JKS), and a client certificate — all using RSA 2048 (FIPS-approved).

### 2. Start the cluster

```bash
docker-compose up -d
```

### 3. Run the FIPS producer or consumer

```bash
OPENSSL_CONF="/path/to/fips/openssl.cnf" \
OPENSSL_MODULES="/path/to/fips/module/lib/" \
python ../fips_producer.py localhost:9092 test-topic
```

---

## Verifying FIPS enforcement

Uncomment `KAFKA_SSL_CIPHER_SUITES: TLS_CHACHA20_POLY1305_SHA256` in `docker-compose.yml` to add a non-FIPS cipher to the broker. A correctly configured FIPS client must reject the connection, confirming that only FIPS-approved algorithms are used.

---

## References

- [OpenSSL FIPS 140-2 (v3.0.8) build guide](https://github.com/openssl/openssl/blob/openssl-3.0.8/README-FIPS.md)
- [OpenSSL FIPS 140-3 (v3.1.2) build guide](https://github.com/openssl/openssl/blob/openssl-3.1.2/README-FIPS.md)
- [How to use the OpenSSL FIPS Module](https://www.openssl.org/docs/man3.0/man7/fips_module.html)
- [librdkafka SSL documentation](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#ssl)
