---
title: "BUCKET.PURGE"
sidebar:
  order: 15
description: "Permanently deletes a bucket and all its data (hard delete)."
---

Permanently deletes a bucket and all its data (hard delete). This is the second phase of bucket deletion.

## Overview

Before permanently deleting a bucket, `BUCKET.PURGE` enforces a **distributed sync barrier** to ensure cluster-wide
consistency. This barrier waits for all shards to confirm they have observed the bucket's "removed" status (set by
`BUCKET.REMOVE`).

The barrier mechanism prevents data races in a distributed environment:

- Background workers (index maintenance, replication) may still be processing the bucket
- Other cluster nodes may have pending operations or cached references
- Without coordination, purging could cause errors or inconsistent state

If any shard has not yet observed the removal, the barrier fails with `BARRIERNOTSATISFIED`. When this happens, the
command automatically notifies all cluster members of the removal to accelerate propagation, and you should retry the
purge. In most cases, a single retry is sufficient.

## Syntax

```kronotop
BUCKET.PURGE <bucket>
```

## Parameters

| Parameter | Type   | Required | Description                                                                                                  |
|-----------|--------|----------|--------------------------------------------------------------------------------------------------------------|
| `bucket`  | string | Yes      | Name of the bucket to permanently delete. The bucket must be marked for removal first using `BUCKET.REMOVE`. |

## Return Value

Returns `OK` on success.

## Errors

| Error Code            | Description                                                                |
|-----------------------|----------------------------------------------------------------------------|
| `NOSUCHBUCKET`        | The specified bucket does not exist.                                       |
| `ERR`                 | The bucket is not marked for removal. You must call `BUCKET.REMOVE` first. |
| `BARRIERNOTSATISFIED` | Not all shards have observed the removal. Retry the command.               |

## Examples

**Permanently delete a removed bucket:**

```kronotop
> BUCKET.REMOVE users
OK

> BUCKET.PURGE users
OK
```

**Attempting to purge without removing first:**

```kronotop
> BUCKET.PURGE users
(error) ERR Bucket 'users' is not removed
```

**Handling barrier not satisfied:**

```kronotop
> BUCKET.REMOVE users
OK

> BUCKET.PURGE users
(error) BARRIERNOTSATISFIED Barrier not satisfied: not all shards observed version ...

> BUCKET.PURGE users
OK
```
