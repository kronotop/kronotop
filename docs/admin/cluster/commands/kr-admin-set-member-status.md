---
title: "KR.ADMIN SET-MEMBER-STATUS"
description: "Overrides the status of a cluster member."
---

Overrides the status of a cluster member.

## Syntax

```kronotop
KR.ADMIN SET-MEMBER-STATUS <member-id> <status>
```

## Parameters

| Parameter   | Type   | Description                                                                                                         |
|-------------|--------|---------------------------------------------------------------------------------------------------------------------|
| `member-id` | string | Full 40-character hex member ID or a 4-character prefix. A prefix is resolved against currently registered members. |
| `status`    | string | Target status. One of `RUNNING`, `UNAVAILABLE`, `STOPPED`, `UNKNOWN`. Case-insensitive.                             |

## Return Value

Simple string. Returns `OK` on success.

## Behavior

Updates the status of the specified member and notifies the cluster so that other members observe the change promptly.

**Typical workflow:** set a member to `STOPPED` before calling `REMOVE-MEMBER` to cleanly decommission it.

## Errors

| Error                                                  | Condition                                                                |
|--------------------------------------------------------|--------------------------------------------------------------------------|
| `ERR cluster has not been initialized yet`             | The cluster must be initialized first.                                   |
| `ERR Invalid number of parameters`                     | The wrong number of arguments was supplied.                              |
| `ERR Invalid member status <value>`                    | The value does not match any valid status.                               |
| `ERR Member: <member-id> not registered`               | No member with the given ID exists in the cluster.                       |
| `ERR Member: <member-id> not registered properly`      | The member exists but its record is incomplete.                          |
| `ERR Invalid memberId: <id>`                           | The value is not a valid member ID and is not exactly 4 characters long. |
| `ERR no member found with prefix: <prefix>`            | No registered member ID starts with the given 4-character prefix.        |
| `ERR more than one member found with prefix: <prefix>` | The 4-character prefix is ambiguous.                                     |

## Examples

**Mark a member as STOPPED:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-MEMBER-STATUS 006cdc459c59e600c76494e8388857fc3cba2fa8 STOPPED
OK
```

**Using a 4-character prefix:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-MEMBER-STATUS 006c STOPPED
OK
```

**Invalid status:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-MEMBER-STATUS 006cdc459c59e600c76494e8388857fc3cba2fa8 some-status
(error) ERR Invalid member status some-status
```

**Member not found:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-MEMBER-STATUS a3f18b2e74d9c5601f82e4a7b390d612c8f7e149 RUNNING
(error) ERR Member: a3f18b2e74d9c5601f82e4a7b390d612c8f7e149 not registered
```
