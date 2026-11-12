---
title: "KR.ADMIN ADD-SHARD"
description: "Adds a new shard to the cluster."
---

Adds a new shard to the cluster.

## Syntax

```kronotop
KR.ADMIN ADD-SHARD
```

## Arguments

None.

## Return Value

Integer. The ID of the new shard.

## Behavior

The command creates one shard. Its ID is one higher than the last shard ID the cluster assigned.

The new shard starts as `INOPERABLE` with no routes assigned.

If two `ADD-SHARD` commands run at the same time, only one of them adds a shard. The other fails with an error, and the
client can send it again.

Requires cluster initialization.

## Errors

Argument errors:

| Error Code | Error message                                                | Cause |
|------------|--------------------------------------------------------------|-------|
| `ERR`      | `Wrong number of arguments for 'KR.ADMIN ADD-SHARD' command` | -     |

Cluster errors:

| Error Code | Error message                                                     | Cause                                                       |
|------------|-------------------------------------------------------------------|-------------------------------------------------------------|
| `ERR`      | `Cluster has not been initialized yet`                            | -                                                           |
| `ERR`      | `Another shard is being added at the same time`                   | Another `ADD-SHARD` finished first. Send the command again. |
| `ERR`      | `Shard id counter has not been set, cluster state may be corrupt` | The cluster metadata has no record of the last shard ID.    |

## Examples

**Adding a shard to a cluster that has one shard:**

```kronotop
127.0.0.1:3320> KR.ADMIN ADD-SHARD
(integer) 1
```

**Two commands at the same time:**

```kronotop
127.0.0.1:3320> KR.ADMIN ADD-SHARD
(error) ERR Another shard is being added at the same time
```
