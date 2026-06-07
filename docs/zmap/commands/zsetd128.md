---
title: "ZSET.D128"
description: "Sets a key to a 128-bit decimal value in the ZMap ordered key-value store."
---

Sets a key to a 128-bit decimal value in the ZMap ordered key-value store.

## Syntax

```kronotop
ZSET.D128 <key> <value>
```

## Parameters

| Parameter | Type   | Required | Description                                                                                     |
|-----------|--------|----------|-------------------------------------------------------------------------------------------------|
| `key`     | bytes  | Yes      | The key to set.                                                                                 |
| `value`   | string | Yes      | A decimal number as a string. Accepts plain decimals and scientific notation (e.g., `1.5E+10`). |

## Return Value

Simple string: `OK` on success.

## Behavior

`ZSET.D128` writes a typed 128-bit decimal value for a key in the ZMap subspace of the session's current namespace. The
input string is parsed as a BigDecimal and then converted to IEEE-754 decimal128 (BID encoding), stored as 16 bytes in
little-endian format, the same encoding used by `ZINC.D128` and read by `ZGET.D128`.

Decimal128 provides 34 significant digits of precision, making it suitable for financial calculations and other use
cases where exact decimal representation matters.

If the key already exists, its value is overwritten silently.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the write,
  and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the write is staged in the current transaction and only
  persists when `COMMIT` is called.

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries.

## Errors

**`ERR`** is returned when:

- Invalid decimal: the value is not a parseable decimal string.
- `Exponent is out of range for Decimal128 encoding ...`: The value exceeds the Decimal128 representable range.
- Wrong number of arguments or internal failure.

## Examples

**Set a decimal value:**

```kronotop
> ZSET.D128 price 19.99
OK

> ZGET.D128 price
"19.99"
```

**Overwrite an existing value:**

```kronotop
> ZSET.D128 price 19.99
OK

> ZSET.D128 price 24.99
OK

> ZGET.D128 price
"24.99"
```

**High-precision decimal (34 significant digits):**

```kronotop
> ZSET.D128 pi 3.141592653589793238462643383279502
OK

> ZGET.D128 pi
"3.141592653589793238462643383279502"
```

**Invalid string rejection:**

```kronotop
> ZSET.D128 price notanumber
(error) ERR invalid decimal
```

**Use within an explicit transaction:**

```kronotop
> BEGIN
OK

> ZSET.D128 tx-price 42.42
OK

> COMMIT
OK

> ZGET.D128 tx-price
"42.42"
```
