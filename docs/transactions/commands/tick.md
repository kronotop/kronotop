---
title: "TICK"
description: "Returns a monotonically increasing 64-bit integer."
---

Returns a monotonically increasing 64-bit integer.

## Syntax

```kronotop
TICK <FRESH | CACHED>
```

## Parameters

| Parameter | Description                                                                                          |
|-----------|------------------------------------------------------------------------------------------------------|
| `FRESH`   | Fetches the latest value from the cluster. One round trip per call.                                  |
| `CACHED`  | Serves the value from a node-local cache that may be up to one second behind. No cluster round trip. |

The mode parameter is mandatory and case-insensitive.

## Return Value

Integer: the current value.

## Guarantees

- The value never decreases. `FRESH` values are monotonic across the whole cluster. `CACHED` values are monotonic
  per node: a client that switches nodes within one second may observe a smaller value.
- The same value can be returned more than once, even with `FRESH`. TICK is not a unique identifier generator.
- The value does not advance at a fixed rate. Consecutive calls can return the same value; the only ordering
  guarantee is that it never goes backwards.

## Choosing a mode

`FRESH` gives the strongest guarantee at the cost of one cluster round trip per call. Use it when a decision depends
on the latest value, such as fencing checks or watermarks that must not lag.

`CACHED` targets high-volume, low-precision use. It serves callers that need a monotonic number frequently and can
tolerate up to one second of slack, without putting load on the cluster. Typical examples are coarse-grained ordering
markers, periodic checkpoints, and staleness checks.

## Behavior

The value is a FoundationDB read version. The read version reflects the latest committed state of the database and
advances only when new commits arrive. FoundationDB advances it at a rate of roughly one million per second of
wall-clock time. On an idle cluster nothing commits, so the read version stands still and consecutive calls return
the same value, even with `FRESH`. The next commit moves it forward by the amount of time that has passed, which is
why the value appears to jump.

With `CACHED`, all connections to a node share a single cache. If the cached value is younger than one second, it is
returned without contacting the cluster. Otherwise, the latest value is fetched and the cache is refreshed. Cache
refreshes are monotonic: a refresh never replaces a cached value with a smaller one.

This command does not require an active transaction.

## Errors

| Error Code | Description                                                         |
|------------|---------------------------------------------------------------------|
| `ERR`      | `illegal argument for TICK: '<value>'`: The mode is not recognized. |

## Examples

**Get the latest value:**

```kronotop
> TICK FRESH
(integer) 1377052400154
```

**Consecutive calls can return the same value, even with FRESH:**

```kronotop
> TICK FRESH
(integer) 1377275302623

> TICK FRESH
(integer) 1377275302623

> TICK FRESH
(integer) 1377275302623
```

**Cached calls within one second return the same value without a cluster round trip:**

```kronotop
> TICK CACHED
(integer) 1377275302623

> TICK CACHED
(integer) 1377275302623
```

**Attempt with an invalid mode:**

```kronotop
> TICK FOO
(error) ERR illegal argument for TICK: 'FOO'
```
