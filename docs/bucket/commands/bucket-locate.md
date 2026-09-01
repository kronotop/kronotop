---
title: "BUCKET.LOCATE"
sidebar:
  order: 13
description: "Returns the routing information for a bucket: the member that owns each shard and the addresses of those members."
---

Returns the routing information for a bucket. It lists which shards hold the bucket's data, the member that owns each
shard, and the addresses of those members.

## Syntax

```kronotop
BUCKET.LOCATE <bucket>
```

## Parameters

| Parameter | Type   | Required | Description                   |
|-----------|--------|----------|-------------------------------|
| `bucket`  | string | Yes      | Name of the bucket to locate. |

## Return Value

Returns an array with two elements: the route table and the member table.

The route table is a flat array with 3 elements per shard:

| Position | Type    | Description                                                              |
|----------|---------|--------------------------------------------------------------------------|
| 0        | integer | Shard ID.                                                                |
| 1        | string  | Member ID of the primary owner.                                          |
| 2        | array   | Member IDs of the standby replicas. Empty array if no standbys exist.    |

This pattern repeats for each shard the bucket spans. For a bucket on 2 shards, the route table contains 6 elements.

Shards without a known route are silently omitted from the result.

The member table is a flat array with 2 elements per member:

| Position | Type   | Description                                                       |
|----------|--------|-------------------------------------------------------------------|
| 0        | string | Member ID.                                                        |
| 1        | array  | Addresses clients connect to, `host:port`, preferred entry first. |

The member table lists each member once, even when that member owns several shards. Members that do not appear in the
route table are left out. Clients can use the member ID as a cache key for connections.

## Errors

| Error Code     | Description                          |
|----------------|--------------------------------------|
| `NOSUCHBUCKET` | The specified bucket does not exist. |

## Examples

**Locate a single-shard bucket:**

```kronotop
> BUCKET.LOCATE users
1) 1) (integer) 0
   2) "6ce1a1f0"
   3) (empty array)
2) 1) "6ce1a1f0"
   2) 1) "127.0.0.1:5484"
```

**Locate a multi-shard bucket with a standby:**

```kronotop
> BUCKET.LOCATE events
1) 1) (integer) 0
   2) "6ce1a1f0"
   3) 1) "b47d9c25"
   4) (integer) 1
   5) "6ce1a1f0"
   6) (empty array)
2) 1) "6ce1a1f0"
   2) 1) "10.0.0.1:5484"
      2) "[fd00::1]:5484"
   3) "b47d9c25"
   4) 1) "10.0.0.2:5484"
```

Member `6ce1a1f0` owns both shards, so it appears once in the member table.

**Non-existent bucket:**

```kronotop
> BUCKET.LOCATE nonexistent
(error) NOSUCHBUCKET No such bucket: 'nonexistent'
```
