---
title: "KR.ADMIN LIST-MEMBERS"
description: "Lists all members in the cluster with their metadata."
---

Lists all members in the cluster with their metadata.

## Syntax

```kronotop
KR.ADMIN LIST-MEMBERS
```

## Parameters

None.

## Return Value

RESP3 map where each key is a member ID (string) and each value is a map of member properties:

| Field              | Type    | Description                                                                                                                          |
|--------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------|
| `status`           | string  | Member status: `RUNNING`, `UNAVAILABLE`, `STOPPED`, or `UNKNOWN`                                                                     |
| `process_id`       | string  | Base32-hex encoded Versionstamp, unique per process lifetime                                                                         |
| `external_host`    | string  | Client-facing hostname                                                                                                               |
| `external_port`    | integer | Client-facing port                                                                                                                   |
| `internal_host`    | string  | Cluster-internal hostname                                                                                                            |
| `internal_port`    | integer | Cluster-internal port                                                                                                                |
| `latest_heartbeat` | integer | Monotonically increasing heartbeat counter, incremented by one on each heartbeat interval. `0` if no heartbeat has been recorded yet |

## Behavior

Returns all known cluster members regardless of their status. Members are sorted by `process_id`.

Requires cluster initialization.

## Errors

| Error                                      | Condition                              |
|--------------------------------------------|----------------------------------------|
| `ERR cluster has not been initialized yet` | The cluster must be initialized first. |

## Examples

**Single-member cluster:**

```kronotop
127.0.0.1:3320> KR.ADMIN LIST-MEMBERS
1# "006cdc459c59e600c76494e8388857fc3cba2fa8" =>
   1# "status" => "RUNNING"
   2# "process_id" => "A1B2C3D4E5F6G7H8I9J0"
   3# "external_host" => "127.0.0.1"
   4# "external_port" => (integer) 5484
   5# "internal_host" => "127.0.0.1"
   6# "internal_port" => (integer) 3320
   7# "latest_heartbeat" => (integer) 31404
```

**Multi-member cluster:**

```kronotop
127.0.0.1:3320> KR.ADMIN LIST-MEMBERS
1# "006cdc459c59e600c76494e8388857fc3cba2fa8" =>
   1# "status" => "RUNNING"
   2# "process_id" => "A1B2C3D4E5F6G7H8I9J0"
   3# "external_host" => "10.0.0.1"
   4# "external_port" => (integer) 5484
   5# "internal_host" => "10.0.0.1"
   6# "internal_port" => (integer) 3320
   7# "latest_heartbeat" => (integer) 31404
2# "a3f18b2e74d9c5601f82e4a7b390d612c8f7e149" =>
   1# "status" => "UNAVAILABLE"
   2# "process_id" => "K1L2M3N4O5P6Q7R8S9T0"
   3# "external_host" => "10.0.0.2"
   4# "external_port" => (integer) 5484
   5# "internal_host" => "10.0.0.2"
   6# "internal_port" => (integer) 3320
   7# "latest_heartbeat" => (integer) 31390
```
