---
title: "ZDELRANGE"
description: "Deletes a range of keys from the ZMap ordered key-value store."
---

Deletes a range of keys from the ZMap ordered key-value store.

## Syntax

```kronotop
ZDELRANGE <begin> <end>
```

## Parameters

| Parameter | Type  | Required | Description                                                                                   |
|-----------|-------|----------|-----------------------------------------------------------------------------------------------|
| `begin`   | bytes | Yes      | The start key of the range. Use `*` for unbounded start (from the beginning of the subspace). |
| `end`     | bytes | Yes      | The end key of the range (exclusive). Use `*` for unbounded end (to the end of the subspace). |

## Return Value

Simple string: `OK` on success.

## Behavior

`ZDELRANGE` removes all keys in the half-open interval [begin, end) from the ZMap subspace of the session's current
namespace. The begin key is inclusive, and the end key is exclusive: a key equal to `end` is **not** deleted.

This follows FoundationDB's native range-clear semantics, which always operate on half-open intervals.

The special value `*` can be used as a wildcard to represent an unbounded boundary:

- `*` as `begin`: starts the range from the very first key in the subspace.
- `*` as `end`: extends the range to the very last key in the subspace.

The operation is idempotent: clearing a range that contains no keys returns `OK` without raising an error.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the range
  delete, and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the range delete is staged in the current transaction and
  only takes effect when `COMMIT` is called.

All data is scoped to the session's active namespace. The same keys in different namespaces refer to different entries.

## Errors

| Error Code | Description                                    |
|------------|------------------------------------------------|
| `ERR`      | Wrong number of arguments or internal failure. |

## Examples

**Delete an explicit key range (end-exclusive):**

```kronotop
> ZSET key-0 "a"
OK
> ZSET key-1 "b"
OK
> ZSET key-2 "c"
OK
> ZSET key-3 "d"
OK
> ZSET key-4 "e"
OK
> ZSET key-5 "f"
OK

> ZDELRANGE key-0 key-5
OK

> ZGET key-0
(nil)

> ZGET key-4
(nil)

> ZGET key-5
"f"
```

**Delete from the start of the subspace with `*`:**

```kronotop
> ZDELRANGE * key-5
OK
```

**Delete to the end of the subspace with `*`:**

```kronotop
> ZDELRANGE key-5 *
OK
```

**Use within an explicit transaction:**

```kronotop
> ZSET mykey "Hello"
OK

> BEGIN
OK

> ZDELRANGE mykey *
OK

> COMMIT
OK

> ZGET mykey
(nil)
```
