---
title: "KR.ADMIN SET-SHARD-STATUS"
description: "Changes the operational status of a shard."
---

Changes the operational status of a shard.

## Syntax

```kronotop
KR.ADMIN SET-SHARD-STATUS <shard-kind> <shard-id> <shard-status>
```

## Parameters

| Parameter      | Type           | Description                                                     |
|----------------|----------------|-----------------------------------------------------------------|
| `shard-kind`   | string         | `STASH` or `BUCKET`. Case-insensitive.                          |
| `shard-id`     | integer or `*` | Zero-based shard index, or `*` to target all shards atomically. |
| `shard-status` | string         | `READWRITE`, `READONLY`, or `INOPERABLE`. Case-insensitive.     |

> **Note:** `STASH` is an experimental shard kind, disabled by default. See [Stash](../../../config.md#stash-optional)
> in the configuration reference.

## Return Value

Simple string. Returns `OK` on success.

## Behavior

Updates the operational status of a shard (or all shards when `*` is used) atomically. It only affects the logical
access-control layer and does not alter replication roles or volume state.

**Shard statuses:**

- `READWRITE`: the shard accepts reads and writes.
- `READONLY`: the shard accepts reads but rejects writes.
- `INOPERABLE`: the shard is unavailable and all access is rejected.

When `*` is passed as the shard ID, the status is applied to every shard of the specified kind atomically.

Changes propagate to other cluster members automatically.

Requires cluster initialization.

**Typical workflows:** maintenance (set `READONLY` before failover), incident response (`INOPERABLE` to isolate a
shard), post-initialization (set `READWRITE` after assigning routes).

## Errors

| Error                                      | Condition                                                               |
|--------------------------------------------|-------------------------------------------------------------------------|
| `ERR cluster has not been initialized yet` | The cluster must be initialized first.                                  |
| `ERR invalid number of parameters`         | Exactly 3 arguments are required after `SET-SHARD-STATUS`.              |
| `ERR invalid shard kind`                   | The shard kind must be `STASH` or `BUCKET`.                             |
| `ERR invalid shard id`                     | The shard ID is not a valid integer, or is out of the configured range. |
| `ERR Invalid shard status <value>`         | The status must be `READWRITE`, `READONLY`, or `INOPERABLE`.            |

## Examples

**Set all bucket shards to READWRITE:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS BUCKET * READWRITE
OK
```

**Set a specific bucket shard to READONLY:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS BUCKET 2 READONLY
OK
```

**Set a shard to INOPERABLE:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS BUCKET 0 INOPERABLE
OK
```

**Invalid shard status:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS BUCKET 0 DISABLED
(error) ERR Invalid shard status DISABLED
```

**Shard ID out of range:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS BUCKET 999999 READONLY
(error) ERR invalid shard id
```
