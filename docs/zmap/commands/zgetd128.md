---
title: "ZGET.D128"
description: "Retrieves the value for a key from the ZMap ordered key-value store as an IEEE-754 decimal128 number, returning a plain decimal string."
---

Retrieves the value for a key from the ZMap ordered key-value store as
an [IEEE-754 decimal128](https://en.wikipedia.org/wiki/Decimal128_floating-point_format) number, returning a plain
decimal string.

## Syntax

```kronotop
ZGET.D128 <key>
```

## Parameters

| Parameter | Type  | Required | Description         |
|-----------|-------|----------|---------------------|
| `key`     | bytes | Yes      | The key to look up. |

## Return Value

Bulk string: the stored value decoded as a plain decimal string, or `nil` if the key does not exist.

## Behavior

`ZGET.D128` reads the value for a given key from the ZMap subspace of the session's current namespace and interprets it
as an IEEE-754 decimal128 (BID encoding). It is the typed read companion to `ZINC.D128`. While `ZGET` returns raw bytes,
`ZGET.D128` decodes the stored 16-byte little-endian value and returns it as a plain decimal string (no scientific
notation).

Decimal128 provides 34 significant digits of precision, making it suitable for financial calculations and other use
cases where exact decimal representation matters.

* If the key does not exist, the command returns `nil`.
* If the stored value is not exactly 16 bytes, the command returns an error. This can happen when a key was written with
  `ZSET` using a value that is not a valid 16-byte Decimal128 encoding.
* If the stored Decimal128 value is not representable as a BigDecimal (e.g., NaN or Infinity), the command returns an
  error.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the read,
  and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the read is performed within the current transaction.

`ZGET.D128` also supports **snapshot reads**. When snapshot mode is enabled on the session, the read does not conflict
with concurrent writes, allowing higher throughput for read-heavy workloads.

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries.

## Errors

**`ERR`** is returned when:

- Wrong number of arguments or internal failure.
- `Invalid stored value: expected 16-byte Decimal128 (IEEE-754 BID)`: The stored value is not exactly 16 bytes and
  cannot be decoded as a Decimal128.
- `Invalid stored value: Decimal128 is not representable as BigDecimal`: The stored value is a valid Decimal128 but
  cannot be converted to a BigDecimal (e.g., NaN or Infinity).

## Examples

**Read a value set by ZINC.D128:**

```kronotop
> ZINC.D128 counter 123.456
OK

> ZGET.D128 counter
"123.456"
```

**Read a non-existent key:**

```kronotop
> ZGET.D128 nosuchkey
(nil)
```

**Read after multiple increments:**

```kronotop
> ZINC.D128 counter 2.5
OK

> ZINC.D128 counter 2.5
OK

> ZINC.D128 counter 2.5
OK

> ZGET.D128 counter
"7.5"
```

**Read a high-precision value (34 digits):**

```kronotop
> ZINC.D128 pi 3.141592653589793238462643383279502
OK

> ZGET.D128 pi
"3.141592653589793238462643383279502"
```

**Read an invalid (non-16-byte) value:**

```kronotop
> ZSET badkey "abc"
OK

> ZGET.D128 badkey
(error) ERR Invalid stored value: expected 16-byte Decimal128 (IEEE-754 BID)
```
