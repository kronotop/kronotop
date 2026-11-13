---
title: "KR.ADMIN DESCRIBE-SHARD"
description: "Returns detailed metadata for a single shard."
---

Returns detailed metadata for a single shard.

## Syntax

```kronotop
KR.ADMIN DESCRIBE-SHARD <shard-id>
```

## Arguments

| Argument     | Type    | Description                            |
|--------------|---------|----------------------------------------|
| `shard-id`   | integer | Zero-based shard index.                |

## Return Value

RESP3 map with the following fields:

| Field           | Type             | Description                                                   |
|-----------------|------------------|---------------------------------------------------------------|
| `primary`       | string           | Member ID of the primary route, or empty string if unassigned |
| `standbys`      | array of strings | Member IDs of standby routes                                  |
| `status`        | string           | `READWRITE`, `READONLY`, or `INOPERABLE`                      |
| `linked_volume` | string           | Volume name in the format `shard-<id>` (e.g. `shard-1`)       |

## Behavior

Reads and returns shard metadata: primary route, standbys, status, and linked volume. Returns the same per-shard
structure used by `DESCRIBE-CLUSTER`, but for a single shard.

Requires cluster initialization.

## Errors

Argument errors:

| Error Code | Error message                                                     | Cause                                                                   |
|------------|-------------------------------------------------------------------|-------------------------------------------------------------------------|
| `ERR`      | `Wrong number of arguments for 'KR.ADMIN DESCRIBE-SHARD' command` | -                                                                       |
| `ERR`      | `Invalid shard id: '<value>'`                                     | The shard ID is not a valid integer, or is out of the configured range. |

Cluster errors:

| Error Code | Error message                          | Cause |
|------------|----------------------------------------|-------|
| `ERR`      | `Cluster has not been initialized yet` | -     |

## Examples

**Describe a shard (primary assigned, no standbys):**

```kronotop
127.0.0.1:3320> KR.ADMIN DESCRIBE-SHARD 1
1# "primary" => "006cdc459c59e600c76494e8388857fc3cba2fa8"
2# "standbys" => (empty array)
3# "status" => "READWRITE"
4# "linked_volume" => "shard-1"
```

**Describe a shard with standbys:**

```kronotop
127.0.0.1:3320> KR.ADMIN DESCRIBE-SHARD 0
1# "primary" => "006cdc459c59e600c76494e8388857fc3cba2fa8"
2# "standbys" =>
   1) "a3f18b2e74d9c5601f82e4a7b390d612c8f7e149"
   2) "e72b4f91d8a36c05429e1fd7b583a0e64d912c37"
3# "status" => "READWRITE"
4# "linked_volume" => "shard-0"
```
