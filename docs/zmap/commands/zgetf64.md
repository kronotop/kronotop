---
title: "ZGET.F64"
description: "Retrieves the value for a key from the ZMap ordered key-value store as an IEEE-754 double-precision floating-point number."
---

Retrieves the value for a key from the ZMap ordered key-value store as an IEEE-754 double-precision floating-point
number.

## Syntax

```kronotop
ZGET.F64 <key>
```

## Parameters

| Parameter | Type  | Required | Description         |
|-----------|-------|----------|---------------------|
| `key`     | bytes | Yes      | The key to look up. |

## Return Value

Double: the stored value decoded as an IEEE-754 double, or `nil` if the key does not exist.

## Behavior

`ZGET.F64` reads the value for a given key from the ZMap subspace of the session's current namespace and interprets it
as an IEEE-754 double-precision floating-point number. It is the typed read companion to `ZINC.F64`. While `ZGET`
returns raw bytes, `ZGET.F64` decodes the stored 8-byte little-endian value and returns it as a double type in the RESP3
protocol.

* If the key does not exist, the command returns `nil`.
* If the stored value is not exactly 8 bytes, the command returns an error. This can happen when a key was written with
  `ZSET` using a value that is not a valid 8-byte double encoding.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the read,
  and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the read is performed within the current transaction.

`ZGET.F64` also supports **snapshot reads**. When snapshot mode is enabled on the session, the read does not conflict
with concurrent writes, allowing higher throughput for read-heavy workloads.

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries.

## Errors

**`ERR`** is returned when:

- Wrong number of arguments or internal failure.
- `Invalid stored value: expected 8-byte IEEE-754 double`: The stored value is not exactly 8 bytes and cannot be decoded
  as a double.

## Examples

**Read a value set by ZINC.F64:**

```kronotop
> ZINC.F64 counter 123.456
OK

> ZGET.F64 counter
(double) 123.456
```

**Read a non-existent key:**

```kronotop
> ZGET.F64 nosuchkey
(nil)
```

**Read after multiple increments:**

```kronotop
> ZINC.F64 counter 2.5
OK

> ZINC.F64 counter 2.5
OK

> ZINC.F64 counter 2.5
OK

> ZGET.F64 counter
(double) 7.5
```

**Read an invalid (non-8-byte) value:**

```kronotop
> ZSET badkey "abc"
OK

> ZGET.F64 badkey
(error) ERR Invalid stored value: expected 8-byte IEEE-754 double
```
