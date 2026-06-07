---
title: "BUCKET.REMOVE"
sidebar:
  order: 14
description: "Marks a bucket for removal (soft delete)."
---

Marks a bucket for removal (soft delete). This is the first phase of bucket deletion.

## Overview

Bucket deletion uses a two-phase approach to ensure safe removal in a distributed cluster:

1. **Phase 1 - Soft Delete (`BUCKET.REMOVE`)**: Marks the bucket as "removed" in metadata. The bucket immediately
   becomes inaccessible for normal operations (queries, inserts, updates). This phase signals background tasks (such as
   index maintenance) to stop processing the bucket.

2. **Phase 2 - Hard Delete (`BUCKET.PURGE`)**: Permanently deletes all bucket data, indexes, and metadata.

This two-phase approach is necessary because in a Kronotop cluster, multiple nodes may have active operations or
background
tasks referencing the bucket. The soft delete phase gives these operations time to gracefully stop before the data is
permanently
removed, preventing errors and ensuring consistency across the cluster.

Between the two phases, Kronotop uses a **distributed sync barrier** to coordinate cluster-wide acknowledgment. When
`BUCKET.PURGE`
is called, it waits for all shards across the cluster to confirm they have observed the bucket's "removed" status.
This barrier mechanism prevents data races where a node might still be writing to or reading from a bucket that is being
deleted on another node. If the barrier is not satisfied within the timeout, the purge fails with `BARRIERNOTSATISFIED`,
and you should retry the command.

## Syntax

```kronotop
BUCKET.REMOVE <bucket>
```

## Parameters

| Parameter | Type   | Required | Description                             |
|-----------|--------|----------|-----------------------------------------|
| `bucket`  | string | Yes      | Name of the bucket to mark for removal. |

## Return Value

Returns `OK` on success.

## Errors

| Error Code              | Description                               |
|-------------------------|-------------------------------------------|
| `NOSUCHBUCKET`          | The specified bucket does not exist.      |
| `BUCKETBEINGREMOVED`    | The bucket is already marked for removal. |
| `NOSUCHNAMESPACE`       | The namespace does not exist.             |
| `NAMESPACEBEINGREMOVED` | The namespace is being removed.           |

## Examples

**Mark a bucket for removal:**

```kronotop
> BUCKET.REMOVE users
OK
```

**Attempting to remove a non-existent bucket:**

```kronotop
> BUCKET.REMOVE nonexistent
(error) NOSUCHBUCKET No such bucket: 'nonexistent'
```

**Attempting to remove an already-removed bucket:**

```kronotop
> BUCKET.REMOVE users
(error) BUCKETBEINGREMOVED Bucket 'users' is being removed
```

**Two-phase deletion workflow:**

```kronotop
> BUCKET.REMOVE users
OK

> BUCKET.PURGE users
OK
```
