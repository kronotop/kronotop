---
title: "ZINC.I64"
description: "Atomically increments a 64-bit integer value in the ZMap ordered key-value store."
---

Atomically increments a 64-bit integer value in the ZMap ordered key-value store.

## Syntax

```kronotop
ZINC.I64 <key> <value>
```

## Parameters

| Parameter | Type    | Required | Description                                                                              |
|-----------|---------|----------|------------------------------------------------------------------------------------------|
| `key`     | bytes   | Yes      | The key to increment.                                                                    |
| `value`   | integer | Yes      | A signed 64-bit integer to add to the current value. Use a negative number to decrement. |

## Return Value

Simple string: `OK` on success.

## Behavior

`ZINC.I64` performs an atomic, conflict-free increment on a key in the ZMap subspace of the session's current namespace.
It is a typed convenience wrapper around FoundationDB's `ADD` mutation, equivalent to
`ZMUTATE <key> <little-endian-bytes> ADD` but accepting a plain integer argument instead of raw bytes.

Because the underlying `ADD` mutation does not read before writing, concurrent `ZINC.I64` calls on the same key do not
cause transaction conflicts. This makes the command well-suited for counters, rate limiters, and accumulators.

If the key does not exist, it is created with an implicit starting value of zero and then incremented by `value`.

The value is encoded as an 8-byte little-endian signed integer. Overflow follows two's complement arithmetic:
incrementing `Long.MAX_VALUE` by 1 wraps to `Long.MIN_VALUE`.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the
  mutation, and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the mutation is staged in the current transaction and only
  persists when `COMMIT` is called.

`ZINC.I64` is a write operation and does not support snapshot reads.

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries.

## Errors

| Error Code | Description                                                                   |
|------------|-------------------------------------------------------------------------------|
| `ERR`      | Wrong number of arguments, value is not a valid integer, or internal failure. |

## Examples

**Increment a new key (implicit zero start):**

```kronotop
> ZINC.I64 counter 10
OK

> ZGET.I64 counter
(integer) 10
```

**Accumulate multiple increments:**

```kronotop
> ZINC.I64 counter 10
OK

> ZINC.I64 counter 20
OK

> ZINC.I64 counter 30
OK

> ZGET.I64 counter
(integer) 60
```

**Decrement with a negative value:**

```kronotop
> ZINC.I64 counter 100
OK

> ZINC.I64 counter -30
OK

> ZGET.I64 counter
(integer) 70
```

**Use within an explicit transaction:**

```kronotop
> BEGIN
OK

> ZINC.I64 tx-counter 42
OK

> COMMIT
OK

> ZGET.I64 tx-counter
(integer) 42
```
