---
title: "ZGET.I64"
description: "Retrieves the value for a key from the ZMap ordered key-value store as a signed 64-bit integer."
---

Retrieves the value for a key from the ZMap ordered key-value store as a signed 64-bit integer.

## Syntax

```kronotop
ZGET.I64 <key>
```

## Parameters

| Parameter | Type  | Required | Description         |
|-----------|-------|----------|---------------------|
| `key`     | bytes | Yes      | The key to look up. |

## Return Value

Integer: the stored value decoded as a signed 64-bit integer, or `nil` if the key does not exist.

## Behavior

`ZGET.I64` reads the value for a given key from the ZMap subspace of the session's current namespace and interprets it
as a signed 64-bit integer. It is the typed read companion to `ZINC.I64`. While `ZGET` returns raw bytes, `ZGET.I64`
decodes the stored 8-byte little-endian value and returns it as an integer type in the RESP3 protocol.

If the key does not exist, the command returns `nil`.

If the stored value is not exactly 8 bytes, the command returns an error. This can happen when a key was written with
`ZSET` using a value that is not a valid 8-byte integer encoding.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the read,
  and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the read is performed within the current transaction.

`ZGET.I64` also supports **snapshot reads**. When snapshot mode is enabled on the session, the read does not conflict
with concurrent writes, allowing higher throughput for read-heavy workloads.

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries.

## Errors

**`ERR`** is returned when:

- Wrong number of arguments or internal failure.
- `Invalid stored value: expected 8-byte two's-complement int64`: The stored value is not exactly 8 bytes and cannot be
  decoded as a 64-bit integer.

## Examples

**Read a value set by ZINC.I64:**

```kronotop
> ZINC.I64 counter 12345
OK

> ZGET.I64 counter
(integer) 12345
```

**Read a non-existent key:**

```kronotop
> ZGET.I64 nosuchkey
(nil)
```

**Read after multiple increments:**

```kronotop
> ZINC.I64 counter 5
OK

> ZINC.I64 counter 5
OK

> ZINC.I64 counter 5
OK

> ZGET.I64 counter
(integer) 15
```

**Read an invalid (non-8-byte) value:**

```kronotop
> ZSET badkey "abc"
OK

> ZGET.I64 badkey
(error) ERR Invalid stored value: expected 8-byte two's-complement int64
```
