---
title: "ZSET.F64"
description: "Sets a key to a double-precision floating-point value in the ZMap ordered key-value store."
---

Sets a key to a double-precision floating-point value in the ZMap ordered key-value store.

## Syntax

```kronotop
ZSET.F64 <key> <value>
```

## Parameters

| Parameter | Type   | Required | Description                               |
|-----------|--------|----------|-------------------------------------------|
| `key`     | bytes  | Yes      | The key to set.                           |
| `value`   | double | Yes      | A finite IEEE-754 double-precision value. |

## Return Value

Simple string: `OK` on success.

## Behavior

`ZSET.F64` writes a typed 64-bit floating-point value for a key in the ZMap subspace of the session's current namespace.
The value is encoded as 8 bytes in little-endian IEEE-754 double format, the same encoding used by `ZINC.F64` and read
by `ZGET.F64`.

If the key already exists, its value is overwritten silently.

The value must be a finite double. NaN and Infinity are rejected. Negative zero (`-0.0`) is normalized to `0.0`.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the write,
  and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the write is staged in the current transaction and only
  persists when `COMMIT` is called.

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries.

## Errors

**`ERR`** is returned when:

- `Invalid value: must be a finite IEEE-754 double`: The provided value is NaN or Infinity.
- Wrong number of arguments, value is not a valid double, or internal failure.

## Examples

**Set a floating-point value:**

```kronotop
> ZSET.F64 price 19.99
OK

> ZGET.F64 price
(double) 19.99
```

**Overwrite an existing value:**

```kronotop
> ZSET.F64 price 19.99
OK

> ZSET.F64 price 24.99
OK

> ZGET.F64 price
(double) 24.99
```

**NaN rejection:**

```kronotop
> ZSET.F64 price NaN
(error) ERR Invalid value: must be a finite IEEE-754 double
```

**Use within an explicit transaction:**

```kronotop
> BEGIN
OK

> ZSET.F64 tx-price 42.42
OK

> COMMIT
OK

> ZGET.F64 tx-price
(double) 42.42
```
