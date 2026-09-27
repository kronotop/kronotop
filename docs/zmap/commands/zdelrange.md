---
title: "ZDELRANGE"
description: "Deletes a range of keys from the ZMap ordered key-value store."
---

Deletes a range of keys from the ZMap ordered key-value store.

## Syntax

```kronotop
ZDELRANGE <begin> <end> [NAMESPACE <path>]
```

## Arguments

`begin` and `end` are positional. `NAMESPACE` is a keyword argument and comes after them.

| Argument    | Type   | Required | Description                                                                                                                                              |
|-------------|--------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `begin`     | bytes  | Yes      | The start key of the range. Use `*` for unbounded start (from the beginning of the subspace).                                                            |
| `end`       | bytes  | Yes      | The end key of the range (exclusive). Use `*` for unbounded end (to the end of the subspace).                                                            |
| `NAMESPACE` | string | No       | Run this command in the given namespace instead of the session's current one. The namespace must exist. The session's current namespace does not change. |

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

Inside an explicit transaction a range with the begin key larger than the end key is accepted when the command runs,
and the error is reported at `COMMIT`.

All data is scoped to a namespace: the session's active one, or the one given with `NAMESPACE`. The same keys in different namespaces refer to different entries.

## Errors

Argument errors:

| Error Code | Error message                                        | Cause |
|------------|------------------------------------------------------|-------|
| `ERR`      | `wrong number of arguments for 'ZDELRANGE' command`  | -     |
| `ERR`      | `Unknown '<keyword>' argument`                       | -     |
| `ERR`      | `NAMESPACE argument must be followed by a namespace` | -     |

Transaction errors:

| Error Code       | Error message                         | Cause                                     |
|------------------|---------------------------------------|-------------------------------------------|
| `INVERTED_RANGE` | `Range begin key larger than end key` | The begin key is larger than the end key. |

Namespace errors:

| Error Code              | Error message                         | Cause |
|-------------------------|---------------------------------------|-------|
| `NOSUCHNAMESPACE`       | `No such namespace: '<path>'`         | -     |
| `NAMESPACEBEINGREMOVED` | `Namespace '<path>' is being removed` | -     |

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
