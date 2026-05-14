# adminapi.py — Kafka AdminClient Example

A CLI tool demonstrating the full range of Kafka `AdminClient` operations from `confluent-kafka-python`.

## Usage

```
python adminapi.py <bootstrap-brokers> <operation> [args...]
```

Creates a single `AdminClient` connected to the given broker, then dispatches to one of ~20 operation functions.

---

## Operations

### Topic Management

| Operation | Description |
|---|---|
| `create_topics <topic1> <topic2> ..` | Creates topics with 3 partitions and replication factor 1 |
| `delete_topics <topic1> <topic2> ..` | Deletes topics, waiting up to 30s for cluster propagation |
| `create_partitions <topic1> <new_total_count1> ..` | Increases partition count for existing topics |

### Config Management

| Operation | Description |
|---|---|
| `describe_configs <resource_type> <resource_name> ..` | Shows config for any resource (topic, broker, etc.) with metadata: source, read-only, sensitive, synonyms |
| `alter_configs <resource_type> <resource_name> <config=val,..> ..` | Atomic replacement of all config values — unspecified keys revert to defaults |
| `incremental_alter_configs <resource_type> <resource_name> <config=op:val;..> ..` | Fine-grained SET/ADD/DELETE ops on individual config keys without affecting others |
| `delta_alter_configs <resource_type> <resource_name> <config=val,..> ..` | Two-step approach: reads current config first, merges only supplied keys, then writes back. Demonstrates async future chaining |

**`incremental_alter_configs` format:**
```
TOPIC my-topic compression.type=SET:lz4;cleanup.policy=ADD:compact;retention.ms=DELETE
```

### ACL Management

| Operation | Description |
|---|---|
| `create_acls <restype> <resname> <pattern_type> <principal> <host> <operation> <permission_type> ..` | Creates ACL rules |
| `describe_acls <restype> <resname> <pattern_type> <principal> <host> <operation> <permission_type> ..` | Lists ACLs matching a filter |
| `delete_acls <restype> <resname> <pattern_type> <principal> <host> <operation> <permission_type> ..` | Deletes ACLs matching a filter |

### Consumer Group Operations

| Operation | Description |
|---|---|
| `list_consumer_groups [-states <s1>,<s2>] [-types <t1>,<t2>]` | Lists groups, filterable by state (STABLE, EMPTY, …) and type |
| `describe_consumer_groups <include_auth_ops> <group1> ..` | Detailed member/assignment info per group |
| `delete_consumer_groups <group1> <group2> ..` | Removes inactive groups |
| `list_consumer_group_offsets <group> [<topic> <partition> ..]` | Reads committed offsets per topic-partition |
| `alter_consumer_group_offsets <group> <topic> <partition> <offset> ..` | Resets committed offsets per topic-partition |

### Cluster & Topic Inspection

| Operation | Description |
|---|---|
| `list [all\|topics\|brokers\|groups]` | Metadata summary: brokers, topics, partitions, ISRs, consumer groups |
| `describe_topics <include_auth_ops> <topic1> ..` | Per-partition leader/replica/ISR details |
| `describe_cluster <include_auth_ops>` | Cluster ID, controller node, all broker nodes |

### Advanced Operations

| Operation | Description |
|---|---|
| `describe_user_scram_credentials [<user1> ..]` | Lists SASL/SCRAM credentials; omit users to describe all |
| `alter_user_scram_credentials UPSERT <user> <mechanism> <iterations> <password> <salt> \| DELETE <user> <mechanism> ..` | Creates or removes SCRAM credentials per user/mechanism |
| `list_offsets <isolation_level> <topic> <partition> <offset_spec> ..` | Fetches offsets by spec: `EARLIEST`, `LATEST`, `MAX_TIMESTAMP`, or `TIMESTAMP <ms>` |
| `delete_records <topic> <partition> <offset> ..` | Purges all records before the given offset in a partition |
| `elect_leaders <election_type> [<topic> <partition> ..]` | Triggers `PREFERRED` or `UNCLEAN` leader elections; omit partitions to target the entire cluster |

---

## Async Pattern

Every operation follows the same pattern:

```python
fs = a.some_admin_operation(...)   # returns dict[resource, Future]
for resource, f in fs.items():
    try:
        result = f.result()        # blocks until done, raises on error
    except KafkaException as e:
        print(f"Failed: {e}")
```

The `delta_alter_configs` function is the most complex — it uses `Future.add_done_callback()` to chain a second async `alter_configs` call after `describe_configs` completes, and uses a thread-safe `WaitZero` counter to know when all callbacks are done.
