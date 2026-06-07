---
title: "BUCKET.LOCATE"
sidebar:
  order: 13
description: "Returns the routing information for a bucket, showing which shards hold its data and the addresses of primary and standby replicas."
---

Returns the routing information for a bucket, showing which shards hold its data and the addresses of primary and
standby replicas.

## Syntax

```kronotop
BUCKET.LOCATE <bucket>
```

## Parameters

| Parameter | Type   | Required | Description                   |
|-----------|--------|----------|-------------------------------|
| `bucket`  | string | Yes      | Name of the bucket to locate. |

## Return Value

Returns a flat array with 3 elements per shard:

| Position | Type    | Description                                                                              |
|----------|---------|------------------------------------------------------------------------------------------|
| 0        | integer | Shard ID.                                                                                |
| 1        | string  | Primary owner address in `host:port` format.                                             |
| 2        | array   | Standby replica addresses, each in `host:port` format. Empty array if no standbys exist. |

This pattern repeats for each shard the bucket spans. For a bucket on 2 shards, the array contains 6 elements.

Shards without a known route are silently omitted from the result.

## Errors

| Error Code     | Description                          |
|----------------|--------------------------------------|
| `NOSUCHBUCKET` | The specified bucket does not exist. |

## Examples

**Locate a single-shard bucket:**

```kronotop
> BUCKET.LOCATE users
1) (integer) 0
2) "127.0.0.1:5484"
3) (empty array)
```

**Locate a multi-shard bucket:**

```kronotop
> BUCKET.LOCATE events
1) (integer) 0
2) "10.0.0.1:5484"
3) (empty array)
4) (integer) 1
5) "10.0.0.2:5484"
6) (empty array)
```

**Non-existent bucket:**

```kronotop
> BUCKET.LOCATE nonexistent
(error) NOSUCHBUCKET No such bucket: 'nonexistent'
```
