---
title: "ZSET.I64"
description: "Sets a key to a signed 64-bit integer value in the ZMap ordered key-value store."
---

Sets a key to a signed 64-bit integer value in the ZMap ordered key-value store.

## Syntax

```kronotop
ZSET.I64 <key> <value> [NAMESPACE <path>]
```

## Arguments

`key` and `value` are positional. `NAMESPACE` is a keyword argument and comes after them.

| Argument    | Type    | Required | Description                                                                                                                                              |
|-------------|---------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `key`       | bytes   | Yes      | The key to set.                                                                                                                                          |
| `value`     | integer | Yes      | A signed 64-bit integer to store.                                                                                                                        |
| `NAMESPACE` | string  | No       | Run this command in the given namespace instead of the session's current one. The namespace must exist. The session's current namespace does not change. |

## Return Value

Simple string: `OK` on success.

## Behavior

`ZSET.I64` writes a typed 64-bit integer value for a key in the ZMap subspace of the session's current namespace. The
value is encoded as 8 bytes in little-endian two's complement format, the same encoding used by `ZINC.I64` and read by
`ZGET.I64`.

If the key already exists, its value is overwritten silently.

The full signed 64-bit range is supported: from `-9223372036854775808` to `9223372036854775807`.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the write,
  and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the write is staged in the current transaction and only
  persists when `COMMIT` is called.

All data is scoped to a namespace: the session's active one, or the one given with `NAMESPACE`. The same key in different namespaces refers to different entries.

## Errors

Argument errors:

| Error Code | Error message                                        | Cause                                     |
|------------|------------------------------------------------------|-------------------------------------------|
| `ERR`      | `wrong number of arguments for 'ZSET.I64' command`   | -                                         |
| `ERR`      | `value is not a long or out of range`                | The value is not a signed 64-bit integer. |
| `ERR`      | `Unknown '<keyword>' argument`                       | -                                         |
| `ERR`      | `NAMESPACE argument must be followed by a namespace` | -                                         |

Namespace errors:

| Error Code              | Error message                         | Cause |
|-------------------------|---------------------------------------|-------|
| `NOSUCHNAMESPACE`       | `No such namespace: '<path>'`         | -     |
| `NAMESPACEBEINGREMOVED` | `Namespace '<path>' is being removed` | -     |

## Examples

**Set an integer value:**

```kronotop
> ZSET.I64 counter 42
OK

> ZGET.I64 counter
(integer) 42
```

**Overwrite an existing value:**

```kronotop
> ZSET.I64 counter 100
OK

> ZSET.I64 counter 200
OK

> ZGET.I64 counter
(integer) 200
```

**Set a negative value:**

```kronotop
> ZSET.I64 balance -500
OK

> ZGET.I64 balance
(integer) -500
```

**Use within an explicit transaction:**

```kronotop
> BEGIN
OK

> ZSET.I64 tx-counter 42
OK

> COMMIT
OK

> ZGET.I64 tx-counter
(integer) 42
```
