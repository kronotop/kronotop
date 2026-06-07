---
title: "KR.ADMIN FIND-MEMBER"
description: "Returns metadata for a specific cluster member."
---

Returns metadata for a specific cluster member.

## Syntax

```kronotop
KR.ADMIN FIND-MEMBER <member-id>
```

## Parameters

| Parameter   | Type   | Description                                                                                                         |
|-------------|--------|---------------------------------------------------------------------------------------------------------------------|
| `member-id` | string | Full 40-character hex member ID or a 4-character prefix. A prefix is resolved against currently registered members. |

## Return Value

RESP3 map containing the member's properties:

| Field              | Type    | Description                                                                                                                          |
|--------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------|
| `status`           | string  | Member status: `RUNNING`, `UNAVAILABLE`, `STOPPED`, or `UNKNOWN`                                                                     |
| `process_id`       | string  | Base32-hex encoded Versionstamp, unique per process lifetime                                                                         |
| `external_host`    | string  | Client-facing hostname                                                                                                               |
| `external_port`    | integer | Client-facing port                                                                                                                   |
| `internal_host`    | string  | Cluster-internal hostname                                                                                                            |
| `internal_port`    | integer | Cluster-internal port                                                                                                                |
| `latest_heartbeat` | integer | Monotonically increasing heartbeat counter, incremented by one on each heartbeat interval. `0` if no heartbeat has been recorded yet |

Unlike `LIST-MEMBERS`, the response does not include the member ID as a key, since the caller already knows it.

## Behavior

Looks up the member identified by `member-id` and returns its properties as a RESP3 map.

Requires cluster initialization.

## Errors

| Error                                                  | Condition                                                                |
|--------------------------------------------------------|--------------------------------------------------------------------------|
| `ERR cluster has not been initialized yet`             | The cluster must be initialized first.                                   |
| `ERR member id is required`                            | No member ID was provided.                                               |
| `ERR Invalid memberId: <id>`                           | The value is not a valid member ID and is not exactly 4 characters long. |
| `ERR Member: <member-id> not registered`               | No member with the given ID exists in the cluster.                       |
| `ERR Member: <member-id> not registered properly`      | The member directory exists but contains no member data.                 |
| `ERR no member found with prefix: <prefix>`            | No registered member ID starts with the given 4-character prefix.        |
| `ERR more than one member found with prefix: <prefix>` | The 4-character prefix is ambiguous.                                     |

## Examples

**Look up a member by full ID:**

```kronotop
127.0.0.1:3320> KR.ADMIN FIND-MEMBER 006cdc459c59e600c76494e8388857fc3cba2fa8
1# "status" => "RUNNING"
2# "process_id" => "A1B2C3D4E5F6G7H8I9J0"
3# "external_host" => "10.0.0.1"
4# "external_port" => (integer) 5484
5# "internal_host" => "10.0.0.1"
6# "internal_port" => (integer) 3320
7# "latest_heartbeat" => (integer) 31404
```

**Look up a member by 4-character prefix:**

```kronotop
127.0.0.1:3320> KR.ADMIN FIND-MEMBER 006c
1# "status" => "RUNNING"
2# "process_id" => "A1B2C3D4E5F6G7H8I9J0"
3# "external_host" => "10.0.0.1"
4# "external_port" => (integer) 5484
5# "internal_host" => "10.0.0.1"
6# "internal_port" => (integer) 3320
7# "latest_heartbeat" => (integer) 31404
```

**Member not found:**

```kronotop
127.0.0.1:3320> KR.ADMIN FIND-MEMBER a3f18b2e74d9c5601f82e4a7b390d612c8f7e149
(error) ERR Member: a3f18b2e74d9c5601f82e4a7b390d612c8f7e149 not registered
```
