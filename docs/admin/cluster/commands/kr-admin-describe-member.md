---
title: "KR.ADMIN DESCRIBE-MEMBER"
description: "Returns metadata for the local cluster member."
---

Returns metadata for the local cluster member.

## Syntax

```kronotop
KR.ADMIN DESCRIBE-MEMBER
```

## Parameters

None.

## Return Value

RESP3 map containing the local member's properties:

| Field              | Type    | Description                                                                                                                          |
|--------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------|
| `member_id`        | string  | ID of this member                                                                                                                    |
| `status`           | string  | Member status: `RUNNING`, `UNAVAILABLE`, `STOPPED`, or `UNKNOWN`                                                                     |
| `process_id`       | string  | Base32-hex encoded Versionstamp, unique per process lifetime                                                                         |
| `external_host`    | string  | Client-facing hostname                                                                                                               |
| `external_port`    | integer | Client-facing port                                                                                                                   |
| `internal_host`    | string  | Cluster-internal hostname                                                                                                            |
| `internal_port`    | integer | Cluster-internal port                                                                                                                |
| `latest_heartbeat` | integer | Monotonically increasing heartbeat counter, incremented by one on each heartbeat interval. `0` if no heartbeat has been recorded yet |

Unlike `FIND-MEMBER`, this command includes the `member_id` field, since the caller may not know the local member's
identity.

## Behavior

Returns the local member's properties as a RESP3 map. Useful for health checks and operational scripts.

Requires cluster initialization.

## Errors

| Error                                      | Condition                                          |
|--------------------------------------------|----------------------------------------------------|
| `ERR cluster has not been initialized yet` | The cluster must be initialized first.             |
| `ERR invalid number of parameters`         | No arguments are accepted after `DESCRIBE-MEMBER`. |

## Examples

**Describe the local member:**

```kronotop
127.0.0.1:3320> KR.ADMIN DESCRIBE-MEMBER
1# "member_id" => "006cdc459c59e600c76494e8388857fc3cba2fa8"
2# "status" => "RUNNING"
3# "process_id" => "A1B2C3D4E5F6G7H8I9J0"
4# "external_host" => "10.0.0.1"
5# "external_port" => (integer) 5484
6# "internal_host" => "10.0.0.1"
7# "internal_port" => (integer) 3320
8# "latest_heartbeat" => (integer) 31404
```

**Error: extra parameters**

```kronotop
127.0.0.1:3320> KR.ADMIN DESCRIBE-MEMBER foo
(error) ERR invalid number of parameters
```
