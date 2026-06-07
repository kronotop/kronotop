---
title: "KR.ADMIN DESCRIBE-SHARD"
description: "Returns detailed metadata for a single shard."
---

Returns detailed metadata for a single shard.

## Syntax

```kronotop
KR.ADMIN DESCRIBE-SHARD <shard-kind> <shard-id>
```

## Parameters

| Parameter    | Type    | Description                            |
|--------------|---------|----------------------------------------|
| `shard-kind` | string  | `STASH` or `BUCKET`. Case-insensitive. |
| `shard-id`   | integer | Zero-based shard index.                |

> **Note:** `STASH` is an experimental shard kind, disabled by default. See [Stash](../../../config.md#stash-optional)
> in the configuration reference.

## Return Value

RESP3 map with the following fields:

| Field            | Type             | Description                                                            |
|------------------|------------------|------------------------------------------------------------------------|
| `primary`        | string           | Member ID of the primary route, or empty string if unassigned          |
| `standbys`       | array of strings | Member IDs of standby routes                                           |
| `status`         | string           | `READWRITE`, `READONLY`, or `INOPERABLE`                               |
| `linked_volumes` | array of strings | Volume names in the format `<kind>-shard-<id>` (e.g. `bucket-shard-1`) |

## Behavior

Reads and returns shard metadata: primary route, standbys, status, and linked volumes. Returns the same per-shard
structure used by `DESCRIBE-CLUSTER`, but for a single shard.

Requires cluster initialization.

## Errors

| Error                                      | Condition                                                               |
|--------------------------------------------|-------------------------------------------------------------------------|
| `ERR cluster has not been initialized yet` | The cluster must be initialized first.                                  |
| `ERR invalid number of parameters`         | Exactly 2 arguments are required after `DESCRIBE-SHARD`.                |
| `ERR invalid shard kind`                   | The shard kind must be `STASH` or `BUCKET`.                             |
| `ERR invalid shard id`                     | The shard ID is not a valid integer, or is out of the configured range. |

## Examples

**Describe a bucket shard (primary assigned, no standbys):**

```kronotop
127.0.0.1:3320> KR.ADMIN DESCRIBE-SHARD BUCKET 1
1# "primary" => "006cdc459c59e600c76494e8388857fc3cba2fa8"
2# "standbys" => (empty array)
3# "status" => "READWRITE"
4# "linked_volumes" =>
   1) "bucket-shard-1"
```

**Describe a shard with standbys:**

```kronotop
127.0.0.1:3320> KR.ADMIN DESCRIBE-SHARD BUCKET 0
1# "primary" => "006cdc459c59e600c76494e8388857fc3cba2fa8"
2# "standbys" =>
   1) "a3f18b2e74d9c5601f82e4a7b390d612c8f7e149"
   2) "e72b4f91d8a36c05429e1fd7b583a0e64d912c37"
3# "status" => "READWRITE"
4# "linked_volumes" =>
   1) "bucket-shard-0"
```

**Invalid shard kind:**

```kronotop
127.0.0.1:3320> KR.ADMIN DESCRIBE-SHARD FOOBAR 0
(error) ERR invalid shard kind
```
