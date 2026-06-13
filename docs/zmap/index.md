---
title: "ZMap"
sidebar:
  label: "Overview"
description: "ZMap is a RESP protocol proxy over FoundationDB's ordered key-value API, with convenience commands for typed numeric operations, atomic mutations, and range queries."
---

## Overview

ZMap is a RESP protocol proxy over FoundationDB's ordered key-value API, with convenience commands for typed numeric
operations, atomic mutations, and range queries. All operations are scoped to the session's active namespace.

Keys are stored in lexicographic order, enabling efficient range reads, range deletes, and key selector navigation.
All operations inherit FoundationDB's ACID transaction guarantees.

## Data Model

Keys and values are raw bytes. At the storage level there is no schema and no type system. Interpretation is left to
the client. Keys are maintained in lexicographic order by FoundationDB, which enables ordered scans and range operations
without secondary indexes.

Each ZMap entry is scoped to the session's current namespace. The same key in different namespaces refers to different
entries, providing full data isolation between namespaces.

## Typed Numeric Operations

On top of the raw byte layer, ZMap provides typed set, increment, and read commands for three numeric encodings:

| Encoding   | Set         | Increment   | Read        | Size     | Conflict behavior            |
|------------|-------------|-------------|-------------|----------|------------------------------|
| int64      | `ZSET.I64`  | `ZINC.I64`  | `ZGET.I64`  | 8 bytes  | Conflict-free (atomic `ADD`) |
| float64    | `ZSET.F64`  | `ZINC.F64`  | `ZGET.F64`  | 8 bytes  | Read-modify-write            |
| decimal128 | `ZSET.D128` | `ZINC.D128` | `ZGET.D128` | 16 bytes | Read-modify-write            |

`ZINC.I64` is conflict-free because it maps directly to FoundationDB's [atomic
`ADD` mutation](https://apple.github.io/foundationdb/developer-guide.html#atomic-operations), so concurrent
increments on the same key never cause transaction conflicts. `ZINC.F64` and `ZINC.D128` perform a read-modify-write
cycle because FoundationDB has no native atomic mutation for floating-point or decimal addition; concurrent calls on the
same key can conflict.

If a key does not exist, all increment commands create it with an implicit starting value of zero.

```kronotop
> ZINC.I64 counter 10
OK

> ZINC.I64 counter 20
OK

> ZGET.I64 counter
(integer) 30
```

## Atomic Mutations

`ZMUTATE` exposes FoundationDB's conflict-free
[atomic mutation primitives](https://apple.github.io/foundationdb/developer-guide.html#atomic-operations) through a
single command. Because mutations do not read the current value before writing, concurrent mutations on the same key do
not cause transaction conflicts.

Available mutation types:

| Type                             | Description                                                        |
|----------------------------------|--------------------------------------------------------------------|
| `ADD`                            | Little-endian integer addition                                     |
| `BIT_AND` / `BIT_OR` / `BIT_XOR` | Bitwise operations                                                 |
| `MIN` / `MAX`                    | Unsigned lexicographic minimum / maximum                           |
| `BYTE_MIN` / `BYTE_MAX`          | Byte-level minimum / maximum                                       |
| `COMPARE_AND_CLEAR`              | Clears the key if its current value equals the operand             |
| `APPEND_IF_FITS`                 | Appends the operand if the result fits within the value size limit |
| `SET_VERSIONSTAMPED_VALUE`       | Sets a value with a versionstamp embedded in the value             |

The operand is the raw byte representation of the value. For `ADD`, it is a little-endian signed 64-bit integer. The
example below atomically adds 5 to `counter`. `\x05\x00\x00\x00\x00\x00\x00\x00` is the 8-byte little-endian
encoding of 5:

```kronotop
> ZMUTATE counter "\x05\x00\x00\x00\x00\x00\x00\x00" ADD
OK
```

In client SDKs, the operand is a byte array:

Java

```java
ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(5).array();
```

Python

```python
struct.pack('<q', 5)
```

JavaScript

```javascript
const buf = Buffer.alloc(8);
buf.writeBigInt64LE(5n);
```

## Range Operations

The ordered key space enables efficient range operations:

- **`ZGETRANGE`**: Read an ordered range of key-value pairs with optional `LIMIT` (default 100), `REVERSE`, and key
  selector parameters. Both endpoints are inclusive by default.
- **`ZDELRANGE`**: Delete all keys in a half-open interval `[begin, end)`.
- **`ZGETKEY`**: Resolve the key name that matches a key selector (`first_greater_or_equal`, `first_greater_than`,
  `last_less_than`, `last_less_or_equal`) relative to a reference key. Useful for cursor-like navigation.
- **`ZGETRANGESIZE`**: Return the estimated byte size of a key range, backed by FoundationDB's
  `getEstimatedRangeSizeBytes` API.

The wildcard `*` represents an unbounded boundary. Both `ZGETRANGE` and `ZDELRANGE` support `*`. For example,
`ZGETRANGE * *` scans the entire subspace and `ZDELRANGE * *` clears it.

```kronotop
> ZGETRANGE key-0 key-5 LIMIT 3 REVERSE
1) 1) "key-5"
   2) "f"
2) 1) "key-4"
   2) "e"
3) 1) "key-3"
   2) "d"
```

## Transaction Support

By default, each ZMap command auto-commits: Kronotop creates a FoundationDB transaction, executes the command, and
commits immediately. To group multiple commands into a single atomic unit, wrap them in `BEGIN` / `COMMIT`:

```kronotop
> BEGIN
OK

> ZSET key1 100
OK

> ZSET key2 200
OK

> COMMIT
OK
```

`ROLLBACK` discards all uncommitted changes. Read commands (`ZGET`, `ZGETRANGE`, `ZGETKEY`, `ZGETRANGESIZE`) support
snapshot reads via `SNAPSHOTREAD ON`, which avoids read conflict ranges for higher throughput on read-heavy workloads.

See [Transactions](../transactions/index.md) for details on explicit transactions, snapshot reads, and
FoundationDB constraints.

## Commands

| Command                                    | Description                                |
|--------------------------------------------|--------------------------------------------|
| [ZSET](commands/zset.md)                   | Set a key-value pair                       |
| [ZGET](commands/zget.md)                   | Get the value for a key                    |
| [ZDEL](commands/zdel.md)                   | Delete a key                               |
| [ZDELRANGE](commands/zdelrange.md)         | Delete a range of keys                     |
| [ZGETRANGE](commands/zgetrange.md)         | Get an ordered range of key-value pairs    |
| [ZGETKEY](commands/zgetkey.md)             | Resolve a key name by key selector         |
| [ZGETRANGESIZE](commands/zgetrangesize.md) | Get the estimated byte size of a key range |
| [ZMUTATE](commands/zmutate.md)             | Perform an atomic mutation on a key        |
| [ZWATCH](commands/zwatch.md)               | Block until the value at a key changes     |
| [ZSET.I64](commands/zseti64.md)            | Set a value as a signed 64-bit integer     |
| [ZINC.I64](commands/zinci64.md)            | Atomically increment a 64-bit integer      |
| [ZGET.I64](commands/zgeti64.md)            | Get a value as a signed 64-bit integer     |
| [ZSET.F64](commands/zsetf64.md)            | Set a value as a double-precision float    |
| [ZINC.F64](commands/zincf64.md)            | Increment a 64-bit floating-point value    |
| [ZGET.F64](commands/zgetf64.md)            | Get a value as a double-precision float    |
| [ZSET.D128](commands/zsetd128.md)          | Set a value as a decimal128 number         |
| [ZINC.D128](commands/zincd128.md)          | Increment a 128-bit decimal value          |
| [ZGET.D128](commands/zgetd128.md)          | Get a value as a decimal128 number         |
