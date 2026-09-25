---
title: "KR.ADMIN DROP-CLUSTER"
description: "Removes an entire cluster's metadata from FoundationDB."
---

Removes an entire cluster's metadata from FoundationDB.

## Syntax

```kronotop
KR.ADMIN DROP-CLUSTER <cluster-name>
KR.ADMIN DROP-CLUSTER <cluster-name> <token>
```

## Parameters

| Parameter      | Type   | Description                                                                                   |
|----------------|--------|-----------------------------------------------------------------------------------------------|
| `cluster-name` | string | Name of the cluster to drop. Must match the cluster name in the running node's configuration. |
| `token`        | string | Confirmation token returned by Phase 1. Required to execute the actual deletion (Phase 2).    |

## Return Value

- **Phase 1** (no token): Bulk string. A UUID confirmation token valid for 60 seconds.
- **Phase 2** (with token): Simple string. Returns `OK` on success.

## Behavior

This is a two-phase command designed to prevent accidental deletion of an entire cluster.

**Phase 1: Request token**

Calling `DROP-CLUSTER` with only the cluster name validates that the name matches the running node's configured cluster
name and returns a single-use UUID token. The token expires after 60 seconds.

If a valid token already exists for the cluster, Phase 1 returns that same token. If the existing token has expired,
Phase 1 issues a new one.

**Phase 2: Confirm deletion**

Calling `DROP-CLUSTER` with both the cluster name and the token validates the token and, if valid, recursively removes
the cluster's entire directory tree from the FoundationDB directory layer. This includes all shard metadata, member
records, namespace directories, volume metadata, and any other data stored under the cluster's directory prefix.

The token is removed after a successful deletion or when Phase 2 finds it expired. A mismatched token does not remove
the pending token. After an expiry error, the operator must request a new token from Phase 1.

**Important notes:**

- This command only removes metadata from FoundationDB. Local disk files (volume segments) are not affected and must be
  cleaned up manually by the administrator.
- Multiple Kronotop clusters can coexist on a single FoundationDB instance. This command removes only the specified
  cluster; other clusters are unaffected.
- After dropping a cluster, the node is in an inconsistent state. The administrator should stop all nodes that belonged
  to the dropped cluster.
- Requires cluster initialization.

## Errors

Argument errors:

| Error Code | Error message                  | Cause |
|------------|--------------------------------|-------|
| `ERR`      | `cluster name is required`     | -     |
| `ERR`      | `invalid number of parameters` | -     |

Cluster errors:

| Error Code | Error message                                    | Cause                                                           |
|------------|--------------------------------------------------|-----------------------------------------------------------------|
| `ERR`      | `cluster has not been initialized yet`           | -                                                               |
| `ERR`      | `cluster name does not match`                    | -                                                               |
| `ERR`      | `no pending drop-cluster token for this cluster` | No token has been requested, or the token was already consumed. |
| `ERR`      | `drop-cluster token has expired`                 | The token's 60-second TTL has elapsed.                          |
| `ERR`      | `invalid drop-cluster token`                     | The token does not match the pending one.                       |

## Examples

**Phase 1: Request a confirmation token**

```kronotop
127.0.0.1:3320> KR.ADMIN DROP-CLUSTER my-cluster
"a1b2c3d4-e5f6-7890-abcd-ef1234567890"
```

**Phase 2: Confirm and drop the cluster**

```kronotop
127.0.0.1:3320> KR.ADMIN DROP-CLUSTER my-cluster a1b2c3d4-e5f6-7890-abcd-ef1234567890
OK
```

**Wrong cluster name:**

```kronotop
127.0.0.1:3320> KR.ADMIN DROP-CLUSTER wrong-name
(error) ERR cluster name does not match
```

**Reusing a consumed token:**

```kronotop
127.0.0.1:3320> KR.ADMIN DROP-CLUSTER my-cluster a1b2c3d4-e5f6-7890-abcd-ef1234567890
(error) ERR no pending drop-cluster token for this cluster
```
