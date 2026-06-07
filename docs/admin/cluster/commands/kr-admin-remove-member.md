---
title: "KR.ADMIN REMOVE-MEMBER"
description: "Removes a member from the cluster."
---

Removes a member from the cluster.

## Syntax

```kronotop
KR.ADMIN REMOVE-MEMBER <member-id>
```

## Parameters

| Parameter   | Type   | Description                                                                                                                                  |
|-------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `member-id` | string | Exactly 40 characters for a full member ID, or exactly 4 characters for a prefix. A prefix is resolved against currently registered members. |

## Return Value

Simple string. Returns `OK` on success.

## Behavior

Members in `RUNNING` status cannot be removed. This safety guard prevents accidental removal of active members.

After a successful removal, other cluster members observe the change promptly.

Requires cluster initialization.

**Typical workflow:** set the member to `STOPPED` with `SET-MEMBER-STATUS` first, then call `REMOVE-MEMBER` to
decommission it.

## Errors

| Error                                                  | Condition                                                                    |
|--------------------------------------------------------|------------------------------------------------------------------------------|
| `ERR cluster has not been initialized yet`             | The cluster must be initialized first.                                       |
| `ERR Member in RUNNING status cannot be removed`       | The member is still active and must be stopped first.                        |
| `ERR Member: <member-id> not registered`               | No member with the given ID exists in the cluster.                           |
| `ERR Member: <member-id> not registered properly`      | The member's directory exists but its data is missing or corrupted.          |
| `ERR Invalid memberId: <id>`                           | The value is neither a 40-character full member ID nor a 4-character prefix. |
| `ERR no member found with prefix: <prefix>`            | No registered member ID starts with the given 4-character prefix.            |
| `ERR more than one member found with prefix: <prefix>` | The 4-character prefix is ambiguous.                                         |

## Examples

**Remove a stopped member:**

```kronotop
127.0.0.1:3320> KR.ADMIN SET-MEMBER-STATUS 006cdc459c59e600c76494e8388857fc3cba2fa8 STOPPED
OK
127.0.0.1:3320> KR.ADMIN REMOVE-MEMBER 006cdc459c59e600c76494e8388857fc3cba2fa8
OK
```

**Using a 4-character prefix:**

```kronotop
127.0.0.1:3320> KR.ADMIN REMOVE-MEMBER 006c
OK
```

**Attempting to remove a running member:**

```kronotop
127.0.0.1:3320> KR.ADMIN REMOVE-MEMBER 006cdc459c59e600c76494e8388857fc3cba2fa8
(error) ERR Member in RUNNING status cannot be removed
```

**Member not found:**

```kronotop
127.0.0.1:3320> KR.ADMIN REMOVE-MEMBER a3f18b2e74d9c5601f82e4a7b390d612c8f7e149
(error) ERR Member: a3f18b2e74d9c5601f82e4a7b390d612c8f7e149 not registered
```
