---
title: "ZINC.F64"
description: "Increments a 64-bit floating-point value in the ZMap ordered key-value store."
---

Increments a 64-bit floating-point value in the ZMap ordered key-value store.

## Syntax

```kronotop
ZINC.F64 <key> <value>
```

## Parameters

| Parameter | Type   | Required | Description                                                                               |
|-----------|--------|----------|-------------------------------------------------------------------------------------------|
| `key`     | bytes  | Yes      | The key to increment.                                                                     |
| `value`   | double | Yes      | A finite IEEE-754 double to add to the current value. Use a negative number to decrement. |

## Return Value

Simple string: `OK` on success.

## Behavior

`ZINC.F64` performs a floating-point increment on a key in the ZMap subspace of the session's current namespace. Unlike
`ZINC.I64`, which uses FoundationDB's conflict-free `ADD` mutation, `ZINC.F64` performs a read-modify-write cycle
because FoundationDB has no atomic double-add mutation. This means concurrent `ZINC.F64` calls on the same key **can**
cause transaction conflicts.

If the key does not exist, it is created with an implicit starting value of zero and then incremented by `value`.

The value is encoded as an 8-byte little-endian IEEE-754 double. Three validation guards protect against invalid states:

1. **Finite input**: the delta must be a finite double; NaN and Infinity are rejected.
2. **Valid stored size**: if the key already exists, the stored value must be exactly 8 bytes.
3. **Finite result**: the computed sum must be finite; overflow to Infinity is rejected.

The command normalizes negative zero: if the result is `-0.0`, it is stored as `0.0`.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the
  operation, and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the operation is staged in the current transaction and only
  persists when `COMMIT` is called.

`ZINC.F64` is a write operation and does not support snapshot reads.

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries.

## Errors

**`ERR`** is returned when:

- `Invalid delta: value must be a finite IEEE-754 double`: The provided value is NaN or Infinity.
- `Invalid stored value: expected 8-byte IEEE-754 double`: The existing value at the key is not 8 bytes.
- `Resulting value is not a finite IEEE-754 double (overflow or invalid operation)`: The sum overflows to Infinity.
- Wrong number of arguments, value is not a valid double, or internal failure.

## Examples

**Increment a new key (implicit zero start):**

```kronotop
> ZINC.F64 counter 10.5
OK

> ZGET.F64 counter
(double) 10.5
```

**Accumulate multiple increments:**

```kronotop
> ZINC.F64 counter 1.5
OK

> ZINC.F64 counter 3.0
OK

> ZINC.F64 counter 4.5
OK

> ZGET.F64 counter
(double) 9.0
```

**Decrement with a negative value:**

```kronotop
> ZINC.F64 counter 100.0
OK

> ZINC.F64 counter -30.5
OK

> ZGET.F64 counter
(double) 69.5
```

**Overflow rejection:**

```kronotop
> ZINC.F64 counter 1.7976931348623157E308
OK

> ZINC.F64 counter 1.7976931348623157E308
(error) ERR Resulting value is not a finite IEEE-754 double (overflow or invalid operation)
```

**Use within an explicit transaction:**

```kronotop
> BEGIN
OK

> ZINC.F64 tx-counter 42.42
OK

> COMMIT
OK

> ZGET.F64 tx-counter
(double) 42.42
```
