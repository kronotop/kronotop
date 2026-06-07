---
title: "KR.ADMIN INITIALIZE-CLUSTER"
description: "Initializes a new Kronotop cluster."
---

Initializes a new Kronotop cluster.

## Syntax

```kronotop
KR.ADMIN INITIALIZE-CLUSTER
```

## Parameters

None.

## Return Value

Simple string. Returns `OK` on success.

## Behavior

This command sets up the foundational metadata structures required before a Kronotop cluster can serve traffic. It must
be executed **exactly once** on the management interface (port 3320 by default).

`INITIALIZE-CLUSTER` is the only `KR.ADMIN` subcommand that does not require a pre-initialized cluster.

The command creates bucket shards (count defined by `bucket.shards`). It also provisions shards for the experimental
stash subsystem (count defined by `stash.shards`), which is disabled by default. All shards start as `INOPERABLE`.

Operators must explicitly set shard status and assign routes before the cluster can accept data.

## Errors

| Error                                                      | Condition                                                                                                             |
|------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| `ERR cluster has already been initialized`                 | The cluster has already been initialized.                                                                             |
| `ERR KronotopDirectory: '<path>' has already been created` | A directory created during initialization already exists, indicating a partial or conflicting initialization attempt. |

## Examples

**First-time initialization:**

```kronotop
127.0.0.1:3320> KR.ADMIN INITIALIZE-CLUSTER
OK
```

**Duplicate initialization:**

```kronotop
127.0.0.1:3320> KR.ADMIN INITIALIZE-CLUSTER
(error) ERR cluster has already been initialized
```
