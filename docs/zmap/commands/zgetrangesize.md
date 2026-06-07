---
title: "ZGETRANGESIZE"
description: "Returns the estimated byte size of a key range in the ZMap ordered key-value store."
---

Returns the estimated byte size of a key range in the ZMap ordered key-value store.

## Syntax

```kronotop
ZGETRANGESIZE <begin> <end>
```

## Parameters

| Parameter | Type  | Required | Description                                                                                   |
|-----------|-------|----------|-----------------------------------------------------------------------------------------------|
| `begin`   | bytes | Yes      | The start key of the range. Use `*` for unbounded start (from the beginning of the subspace). |
| `end`     | bytes | Yes      | The end key of the range. Use `*` for unbounded end (to the end of the subspace).             |

## Return Value

Integer: the estimated size in bytes of the key range.

## Behavior

`ZGETRANGESIZE` returns the estimated byte size of a key range from the ZMap subspace of the session's current
namespace, backed by FoundationDB's `getEstimatedRangeSizeBytes` API.

The returned value is an **estimate**, not an exact count. It is useful for capacity planning and understanding data
distribution without materializing the range.

The special value `*` can be used as a wildcard to represent an unbounded boundary:

- `*` as `begin`: starts the range from the very first key in the subspace.
- `*` as `end`: extends the range to the very last key in the subspace.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the read,
  and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the read is performed within the current transaction.

`ZGETRANGESIZE` also supports **snapshot reads**. When snapshot mode is enabled on the session, the read does not
conflict with concurrent writes, allowing higher throughput for read-heavy workloads.

All data is scoped to the session's active namespace. The same keys in different namespaces refer to different entries.

## Errors

| Error Code | Description                                    |
|------------|------------------------------------------------|
| `ERR`      | Wrong number of arguments or internal failure. |

## Examples

**Basic range size estimation:**

```kronotop
> ZSET key-0 "alpha"
OK
> ZSET key-1 "bravo"
OK
> ZSET key-2 "charlie"
OK

> ZGETRANGESIZE key-0 key-2
(integer) 186
```

**Full subspace size estimation with `* *`:**

```kronotop
> ZGETRANGESIZE * *
(integer) 372
```

**Use within an explicit transaction:**

```kronotop
> BEGIN
OK

> ZSET mykey-a "alpha"
OK

> ZSET mykey-b "bravo"
OK

> COMMIT
OK

> BEGIN
OK

> ZGETRANGESIZE mykey-a mykey-b
(integer) 124

> COMMIT
OK
```
