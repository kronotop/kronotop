---
title: "KR.ADMIN DESCRIBE-CLUSTER"
description: "Returns the full cluster topology including cluster name, metadata version, and per-shard details."
---

Returns the full cluster topology including cluster name, metadata version, and per-shard details.

## Syntax

```kronotop
KR.ADMIN DESCRIBE-CLUSTER
```

## Parameters

None.

## Return Value

RESP3 nested map with the following top-level keys:

| Key                | Type   | Description                                                                                                                                                                                                                                 |
|--------------------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `metadata_version` | string | Cluster metadata version (e.g. `"1.0.0"`)                                                                                                                                                                                                   |
| `cluster_name`     | string | Name of the cluster (from configuration)                                                                                                                                                                                                    |
| `bucket`           | map    | Per-shard details for all bucket shards, keyed by shard ID (integer)                                                                                                                                                                        |
| `stash`            | map    | Per-shard details for all stash shards, keyed by shard ID (integer). Present only when the experimental stash subsystem is enabled (`stash.enabled = true`). See [Stash](../../../config.md#stash-optional) in the configuration reference. |

Each shard entry contains:

| Field            | Type             | Description                                                            |
|------------------|------------------|------------------------------------------------------------------------|
| `primary`        | string           | Member ID of the primary route, or empty string if unassigned          |
| `standbys`       | array of strings | Member IDs of standby routes                                           |
| `status`         | string           | `READWRITE`, `READONLY`, or `INOPERABLE`                               |
| `linked_volumes` | array of strings | Volume names in the format `<kind>-shard-<id>` (e.g. `bucket-shard-0`) |

## Behavior

Returns the cluster name, metadata version, and per-shard details for every bucket shard: primary route, standbys,
status, and linked volumes. Each shard entry uses the same structure as `DESCRIBE-SHARD`. When the experimental stash
subsystem is enabled, a `stash` map with the same structure is also returned.

Requires cluster initialization.

## Errors

| Error                                      | Condition                              |
|--------------------------------------------|----------------------------------------|
| `ERR cluster has not been initialized yet` | The cluster must be initialized first. |

## Examples

**Freshly initialized cluster (no routes assigned):**

```kronotop
127.0.0.1:3320> KR.ADMIN DESCRIBE-CLUSTER
1# "metadata_version" => "1.0.0"
2# "cluster_name" => "my-cluster"
3# "bucket" =>
   1# (integer) 0 =>
      1# "primary" => ""
      2# "standbys" => (empty array)
      3# "status" => "INOPERABLE"
      4# "linked_volumes" =>
         1) "bucket-shard-0"
```

**Cluster with routes assigned:**

```kronotop
127.0.0.1:3320> KR.ADMIN DESCRIBE-CLUSTER
1# "metadata_version" => "1.0.0"
2# "cluster_name" => "my-cluster"
3# "bucket" =>
   1# (integer) 0 =>
      1# "primary" => "006cdc459c59e600c76494e8388857fc3cba2fa8"
      2# "standbys" =>
         1) "a3f18b2e74d9c5601f82e4a7b390d612c8f7e149"
      3# "status" => "READWRITE"
      4# "linked_volumes" =>
         1) "bucket-shard-0"
```
