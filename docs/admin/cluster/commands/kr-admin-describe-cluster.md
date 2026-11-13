---
title: "KR.ADMIN DESCRIBE-CLUSTER"
description: "Returns the full cluster topology including cluster name, metadata version, and per-shard details."
---

Returns the full cluster topology including cluster name, metadata version, and per-shard details.

## Syntax

```kronotop
KR.ADMIN DESCRIBE-CLUSTER
```

## Arguments

None.

## Return Value

RESP3 nested map with the following top-level keys:

| Key                | Type   | Description                                                                                                                                                                                                                                 |
|--------------------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `metadata_version` | string | Cluster metadata version (e.g. `"1.0.0"`)                                                                                                                                                                                                   |
| `cluster_name`     | string | Name of the cluster (from configuration)                                                                                                                                                                                                    |
| `shards`           | map    | Per-shard details for all shards, keyed by shard ID (integer)                                                                                                                                                                               |

Each shard entry contains:

| Field           | Type             | Description                                                   |
|-----------------|------------------|---------------------------------------------------------------|
| `primary`       | string           | Member ID of the primary route, or empty string if unassigned |
| `standbys`      | array of strings | Member IDs of standby routes                                  |
| `status`        | string           | `READWRITE`, `READONLY`, or `INOPERABLE`                      |
| `linked_volume` | string           | Volume name in the format `shard-<id>` (e.g. `shard-0`)       |

## Behavior

Returns the cluster name, metadata version, and per-shard details for every shard: primary route, standbys,
status, and linked volume. Each shard entry uses the same structure as `DESCRIBE-SHARD`.

Requires cluster initialization.

## Errors

Argument errors:

| Error Code | Error message                                                       | Cause |
|------------|---------------------------------------------------------------------|-------|
| `ERR`      | `Wrong number of arguments for 'KR.ADMIN DESCRIBE-CLUSTER' command` | -     |

Cluster errors:

| Error Code | Error message                          | Cause |
|------------|----------------------------------------|-------|
| `ERR`      | `Cluster has not been initialized yet` | -     |

## Examples

**Freshly initialized cluster (no routes assigned):**

```kronotop
127.0.0.1:3320> KR.ADMIN DESCRIBE-CLUSTER
1# "metadata_version" => "1.0.0"
2# "cluster_name" => "my-cluster"
3# "shards" =>
   1# (integer) 0 =>
      1# "primary" => ""
      2# "standbys" => (empty array)
      3# "status" => "INOPERABLE"
      4# "linked_volume" => "shard-0"
```

**Cluster with routes assigned:**

```kronotop
127.0.0.1:3320> KR.ADMIN DESCRIBE-CLUSTER
1# "metadata_version" => "1.0.0"
2# "cluster_name" => "my-cluster"
3# "shards" =>
   1# (integer) 0 =>
      1# "primary" => "006cdc459c59e600c76494e8388857fc3cba2fa8"
      2# "standbys" =>
         1) "a3f18b2e74d9c5601f82e4a7b390d612c8f7e149"
      3# "status" => "READWRITE"
      4# "linked_volume" => "shard-0"
```
