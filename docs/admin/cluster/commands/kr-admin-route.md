---
title: "KR.ADMIN ROUTE"
description: "Manages shard-to-member routing for primary and standby assignments."
---

Manages shard-to-member routing for primary and standby assignments.

## Syntax

```kronotop
KR.ADMIN ROUTE <operation-kind> <route-kind> <shard-kind> <shard-id> <member-id>
```

## Parameters

| Parameter        | Type           | Description                                                                                                         |
|------------------|----------------|---------------------------------------------------------------------------------------------------------------------|
| `operation-kind` | string         | `SET` or `UNSET`. Case-insensitive.                                                                                 |
| `route-kind`     | string         | `PRIMARY` or `STANDBY`. Case-insensitive.                                                                           |
| `shard-kind`     | string         | `STASH` or `BUCKET`. Case-insensitive.                                                                              |
| `shard-id`       | integer or `*` | Zero-based shard index, or `*` to target all shards atomically.                                                     |
| `member-id`      | string         | Full 40-character hex member ID or a 4-character prefix. A prefix is resolved against currently registered members. |

> **Note:** `STASH` is an experimental shard kind, disabled by default. See [Stash](../../../config.md#stash-optional)
> in the configuration reference.

## Return Value

Simple string. Returns `OK` on success.

## Behavior

Requires cluster initialization.

### SET PRIMARY

Assigns a member as the primary owner of a shard.

When no primary is assigned yet, the member is written directly as the primary.

Reassignment, when a primary already exists, enforces strict pre-conditions:

1. The shard status must not be `READWRITE`.
2. The new primary must currently be a standby on that shard.
3. The volume status must be `READONLY`.
4. The standby must be fully caught up with the primary's replication state.

On successful promotion the member is removed from the standby set.

### SET STANDBY

Adds a member to the shard's standby set. A primary must already be assigned. The member must not be the current primary
and must not already be a standby.

### UNSET STANDBY

Removes a member from the standby set. A primary must be assigned and the member must be in the standby set.

### UNSET PRIMARY

Not supported. Always returns an error.

### Wildcard shard-id

When `shard-id` is `*`, the operation applies to every shard of the specified kind within a single FoundationDB
transaction. If any shard fails a pre-condition, the entire transaction is rolled back.

### Topology notification

Every successful mutation triggers the cluster topology watcher, causing other members to refresh their routing tables.

## Errors

| Error                                                  | Condition                                                                                                                                                          |
|--------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ERR cluster has not been initialized yet`             | The cluster must be initialized first.                                                                                                                             |
| `ERR invalid number of parameters`                     | Exactly 5 arguments are required after `ROUTE`.                                                                                                                    |
| `ERR Invalid operation kind: <value>`                  | The operation kind must be `SET` or `UNSET`.                                                                                                                       |
| `ERR Invalid route kind: <value>`                      | The route kind must be `PRIMARY` or `STANDBY`.                                                                                                                     |
| `ERR invalid shard kind`                               | The shard kind must be `STASH` or `BUCKET`.                                                                                                                        |
| `ERR invalid shard id`                                 | The shard ID is not a valid integer, or is out of the configured range.                                                                                            |
| `ERR Invalid memberId: <value>`                        | The value is not a valid 40-character hex ID or a 4-character prefix.                                                                                              |
| `ERR no member found with prefix: <prefix>`            | No registered member ID starts with the given 4-character prefix.                                                                                                  |
| `ERR more than one member found with prefix: <prefix>` | Multiple members match the prefix. Use a longer prefix or the full ID.                                                                                             |
| `ERR member not found`                                 | The member ID is not registered in the cluster.                                                                                                                    |
| `ERR no primary member assigned yet`                   | SET or UNSET STANDBY requires an existing primary.                                                                                                                 |
| `ERR primary cannot be assigned as a standby`          | The current primary cannot also be a standby.                                                                                                                      |
| `ERR already assigned as a standby`                    | The member is already in the standby set.                                                                                                                          |
| `ERR member is not a standby`                          | The UNSET STANDBY target is not in the standby set.                                                                                                                |
| `ERR UNSET PRIMARY is not supported`                   | A primary cannot be unset.                                                                                                                                         |
| `ERR Shard status must not be READWRITE`               | Reassigning a primary requires the shard to be non-READWRITE first.                                                                                                |
| `ERR Member could not be found: <id>`                  | The target member could not be resolved during reassignment.                                                                                                       |
| `ERR Primary shard owner could not be found: <id>`     | The current primary could not be resolved during reassignment.                                                                                                     |
| `ERR Member id: <id> is not a standby`                 | The new primary must be a current standby during reassignment.                                                                                                     |
| `ERR Volume status must be READONLY`                   | The volume must be READONLY before primary reassignment.                                                                                                           |
| `ERR Standby is not caught up: <details>`              | The standby has not finished replicating from the primary. The detail indicates whether the lag is in the replication stage, segment position, or sequence number. |

## Examples

**Assign a primary to a shard:**

```kronotop
127.0.0.1:3320> KR.ADMIN ROUTE SET PRIMARY BUCKET 0 006cdc459c59e600c76494e8388857fc3cba2fa8
OK
```

**Add a standby to a shard:**

```kronotop
127.0.0.1:3320> KR.ADMIN ROUTE SET STANDBY BUCKET 0 a3f18b2e74d9c5601f82e4a7b390d612c8f7e149
OK
```

**Assign a standby to all bucket shards:**

```kronotop
127.0.0.1:3320> KR.ADMIN ROUTE SET STANDBY BUCKET * a3f18b2e74d9c5601f82e4a7b390d612c8f7e149
OK
```

**Remove a standby from a shard:**

```kronotop
127.0.0.1:3320> KR.ADMIN ROUTE UNSET STANDBY BUCKET 0 a3f18b2e74d9c5601f82e4a7b390d612c8f7e149
OK
```

**Error: assigning standby without a primary**

```kronotop
127.0.0.1:3320> KR.ADMIN ROUTE SET STANDBY BUCKET 0 a3f18b2e74d9c5601f82e4a7b390d612c8f7e149
(error) ERR no primary member assigned yet
```

**Error: unsetting primary**

```kronotop
127.0.0.1:3320> KR.ADMIN ROUTE UNSET PRIMARY BUCKET 0 006cdc459c59e600c76494e8388857fc3cba2fa8
(error) ERR UNSET PRIMARY is not supported
```
