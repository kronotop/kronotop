---
title: "ZINC.D128"
description: "Increments a 128-bit decimal value in the ZMap ordered key-value store."
---

Increments a 128-bit decimal value in the ZMap ordered key-value store.

## Syntax

```kronotop
ZINC.D128 <key> <value>
```

## Parameters

| Parameter | Type   | Required | Description                                                                                                                                         |
|-----------|--------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `key`     | bytes  | Yes      | The key to increment.                                                                                                                               |
| `value`   | string | Yes      | A decimal number to add to the current value. Accepts plain decimals and scientific notation (e.g., `1.5E+10`). Use a negative number to decrement. |

## Return Value

Simple string: `OK` on success.

## Behavior

`ZINC.D128` performs a high-precision decimal increment on a key in the ZMap subspace of the session's current
namespace. It uses IEEE-754 decimal128 (BID encoding), which provides 34 significant digits and an exponent range of
±6144, far exceeding the ~15 significant digits of `ZINC.F64`.

Like `ZINC.F64`, this command performs a read-modify-write cycle because FoundationDB has no atomic decimal128-add
mutation. This means concurrent `ZINC.D128` calls on the same key **can** cause transaction conflicts.

If the key does not exist, it is created with an implicit starting value of zero and then incremented by `value`.

The value is encoded as 16 bytes in little-endian IEEE-754 BID format (low 8 bytes first, high 8 bytes second). Three
validation guards protect against invalid states:

1. **Valid input**: the delta must be a parseable decimal string; unparseable strings are rejected.
2. **Valid stored size**: if the key already exists, the stored value must be exactly 16 bytes.
3. **Range check**: the computed result must be representable as a Decimal128; values that exceed the Decimal128 range
   are rejected.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the
  operation, and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the operation is staged in the current transaction and only
  persists when `COMMIT` is called.

`ZINC.D128` is a write operation and does not support snapshot reads.

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries.

## Errors

| Error Code                                                             | Description                                            |
|------------------------------------------------------------------------|--------------------------------------------------------|
| `ERR invalid decimal`                                                  | The provided value is not a valid decimal number.      |
| `ERR Invalid stored value: expected 16-byte Decimal128 (IEEE-754 BID)` | The existing value at the key is not 16 bytes.         |
| `ERR Exponent is out of range for Decimal128 encoding ...`             | The result exceeds the Decimal128 representable range. |
| `ERR`                                                                  | Wrong number of arguments or internal failure.         |

## Examples

**Increment a new key (implicit zero start):**

```kronotop
> ZINC.D128 counter 10.5
OK

> ZGET.D128 counter
"10.5"
```

**Accumulate multiple increments:**

```kronotop
> ZINC.D128 counter 1.5
OK

> ZINC.D128 counter 3.0
OK

> ZINC.D128 counter 4.5
OK

> ZGET.D128 counter
"9.0"
```

**Decrement with a negative value:**

```kronotop
> ZINC.D128 counter 100.0
OK

> ZINC.D128 counter -30.5
OK

> ZGET.D128 counter
"69.5"
```

**High-precision decimal (34 significant digits):**

```kronotop
> ZINC.D128 counter 0.123456789012345678901234567890123
OK

> ZGET.D128 counter
"0.123456789012345678901234567890123"
```

**Use within an explicit transaction:**

```kronotop
> BEGIN
OK

> ZINC.D128 tx-counter 42.42
OK

> COMMIT
OK

> ZGET.D128 tx-counter
"42.42"
```
