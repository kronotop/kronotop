---
title: "KR.ADMIN SET-SHARD-STATUS"
description: "Changes the operational status of a shard."
---

Changes the operational status of a shard.

## Syntax

```kronotop
KR.ADMIN SET-SHARD-STATUS <shard-id> <shard-status>
```

## Arguments

| Argument       | Type           | Description                                                     |
|----------------|----------------|-----------------------------------------------------------------|
| `shard-id`     | integer or `*` | Zero-based shard index, or `*` to target all shards atomically. |
| `shard-status` | string         | `READWRITE`, `READONLY`, or `INOPERABLE`. Case-insensitive.     |

## Return Value

Simple string. Returns `OK` on success.

## Behavior

Updates the operational status of a shard (or all shards when `*` is used) atomically. It only affects the logical
access-control layer and does not alter replication roles or volume state.

**Shard statuses:**

- `READWRITE`: the shard accepts reads and writes.
- `READONLY`: the shard accepts reads but rejects writes.
- `INOPERABLE`: the shard is unavailable and all access is rejected.

When `*` is passed as the shard ID, the status is applied to every shard atomically.

Changes propagate to other cluster members automatically.

Requires cluster initialization.

**Typical workflows:** maintenance (set `READONLY` before failover), incident response (`INOPERABLE` to isolate a
shard), post-initialization (set `READWRITE` after assigning routes).

## Errors

Argument errors:

| Error Code | Error message                     | Cause                                                                   |
|------------|-----------------------------------|-------------------------------------------------------------------------|
| `ERR`      | `Invalid number of arguments`     | -                                                                       |
| `ERR`      | `Invalid shard id`                | The shard ID is not a valid integer, or is out of the configured range. |
| `ERR`      | `Unknown shard status: '<value>'` | The status must be `READWRITE`, `READONLY`, or `INOPERABLE`.            |

Cluster errors:

| Error Code | Error message                          | Cause |
|------------|----------------------------------------|-------|
| `ERR`      | `Cluster has not been initialized yet` | -     |

## Examples

**Set all shards to READWRITE:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS * READWRITE
OK
```

**Set a specific shard to READONLY:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS 2 READONLY
OK
```

**Set a shard to INOPERABLE:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS 0 INOPERABLE
OK
```

**Invalid shard status:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS 0 DISABLED
(error) ERR Unknown shard status: 'DISABLED'
```

**Shard ID out of range:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS 999999 READONLY
(error) ERR Invalid shard id
```
