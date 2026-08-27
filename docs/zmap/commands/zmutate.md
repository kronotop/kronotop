---
title: "ZMUTATE"
description: "Performs an atomic mutation on a key's value in the ZMap ordered key-value store."
---

Performs an atomic mutation on a key's value in the ZMap ordered key-value store.

## Syntax

```kronotop
ZMUTATE <key> <param> <mutation_type>
```

## Parameters

| Parameter       | Type   | Required | Description                                                                                                                    |
|-----------------|--------|----------|--------------------------------------------------------------------------------------------------------------------------------|
| `key`           | bytes  | Yes      | The key to mutate.                                                                                                             |
| `param`         | bytes  | Yes      | The operand value for the mutation. Interpretation depends on the mutation type (e.g. little-endian 8-byte integer for `ADD`). |
| `mutation_type` | string | Yes      | The mutation operation to apply. Case-insensitive. Must be one of the types listed below.                                      |

The `param` operand is raw bytes. For `ADD`, it must be a little-endian signed 64-bit integer (8 bytes). `kronotop-cli`
accepts `\x` hex escape notation (e.g. `"\x05\x00\x00\x00\x00\x00\x00\x00"` for integer 5).
In client SDKs, construct the operand as a byte array:

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

## Mutation Types

| Type                       | Description                                                                                                                                              |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ADD`                      | Adds `param` to the existing value. Both are read as little-endian integers, signed or unsigned. On overflow, the result is cut to the width of `param`. |
| `BIT_AND`                  | Bitwise AND of the existing value and `param`.                                                                                                           |
| `BIT_OR`                   | Bitwise OR of the existing value and `param`.                                                                                                            |
| `BIT_XOR`                  | Bitwise XOR of the existing value and `param`.                                                                                                           |
| `APPEND_IF_FITS`           | Appends `param` to the end of the existing value. See the note below about the size limit.                                                               |
| `MAX`                      | Stores the larger of the existing value and `param`. Both are read as unsigned little-endian integers.                                                   |
| `MIN`                      | Stores the smaller of the existing value and `param`. Both are read as unsigned little-endian integers.                                                  |
| `BYTE_MAX`                 | Stores the larger of the two byte strings. Bytes are compared from left to right.                                                                        |
| `BYTE_MIN`                 | Stores the smaller of the two byte strings. Bytes are compared from left to right.                                                                       |
| `COMPARE_AND_CLEAR`        | Deletes the key if its current value is equal to `param`. Writes nothing.                                                                                |
| `SET_VERSIONSTAMPED_VALUE` | Writes `param` with a commit versionstamp inside it. `param` needs a special layout, see below.                                                          |

`MAX` and `MIN` compare numbers. `BYTE_MAX` and `BYTE_MIN` compare byte strings. For the same input they can return
different results.

`APPEND_IF_FITS` appends only if the result stays under the 100,000 byte value limit. If the result is larger, the
value stays unchanged and no error is returned.

## Value Length Rules

Two rules set the length of the stored value. Both can drop bytes without an error.

### Missing key

If the key does not exist, the new value is `param`. This holds for every mutation type except `COMPARE_AND_CLEAR`,
which does nothing.

### Length matching

`ADD`, `BIT_AND`, `BIT_OR`, `BIT_XOR`, `MAX` and `MIN` first change the stored value to the length of `param`:

- If the stored value is shorter than `param`, zero bytes are appended until the lengths match.
- If the stored value is longer than `param`, it is cut down to the length of `param`. The extra bytes are gone and no
  error is returned.

An 8-byte counter mutated with a 1-byte `param` becomes a 1-byte value:

```
stored  \x01\x00\x00\x00\x00\x00\x00\x00
param   \x05
ADD  ->  \x06
```

Send a `param` with the same length as the value you want to keep. For a 64-bit counter, that means 8 bytes on every
call.

`BYTE_MAX`, `BYTE_MIN`, `APPEND_IF_FITS` and `COMPARE_AND_CLEAR` do not pad or cut.

## Versionstamped Values

A versionstamp is 10 bytes. Each committed transaction gets a different one, and the values go up over time. The server
only knows the versionstamp at commit time, so `param` has to leave space for it and mark the position.

`param` layout:

1. The value bytes, with 10 zero bytes at the position where the versionstamp goes.
2. A 4-byte little-endian integer at the end, holding the offset of those 10 bytes.

The last 4 bytes are dropped before the value is stored, so the stored value is 4 bytes shorter than `param`. `param`
must be at least 14 bytes, and the 10 reserved bytes must fit in the value part. If not, the command fails with
`CLIENT_INVALID_OPERATION`.

Python

```python
import struct
param = b'pre' + b'\x00' * 10 + struct.pack('<i', 3)
```

The stored value is `pre` followed by the 10-byte versionstamp, for example:

```
b'pre\x00\x00\x02\xd0Lu\xe2\x03\x00\x00'
```

You cannot read this key inside the same transaction. `ZGET` on it before `COMMIT` fails with `ACCESSED_UNREADABLE`.
Read the key after the transaction commits.

## Return Value

Simple string: `OK` on success.

`OK` means the mutation was accepted. It does not mean the value changed. `COMPARE_AND_CLEAR` returns `OK` when the
value does not match, and `APPEND_IF_FITS` returns `OK` when the result was too large to store.

## Behavior

`ZMUTATE` applies an atomic mutation to a key in the ZMap subspace of the session's current namespace, backed by
FoundationDB. The mutation executes without reading the current value first, making it conflict-free: concurrent
mutations on the same key do not cause transaction conflicts. This makes `ZMUTATE` ideal for counters, flags, and
lock-free data structures.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the
  mutation, and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the mutation is staged in the current transaction and only
  persists when `COMMIT` is called.

`ZMUTATE` is a write operation and does not support snapshot reads.

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries.

## Errors

| Error Code                 | Description                                                                                                                |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------|
| `ERR`                      | Wrong number of arguments, invalid mutation type, or internal failure.                                                     |
| `CLIENT_INVALID_OPERATION` | `SET_VERSIONSTAMPED_VALUE` got a `param` shorter than 14 bytes, or an offset that puts the 10 reserved bytes out of range. |
| `ACCESSED_UNREADABLE`      | A key written by `SET_VERSIONSTAMPED_VALUE` was read in the same transaction, before `COMMIT`.                             |

## Examples

**Atomic integer increment with ADD:**

```kronotop
> ZSET counter "\x01\x00\x00\x00\x00\x00\x00\x00"
OK

> ZMUTATE counter "\x01\x00\x00\x00\x00\x00\x00\x00" ADD
OK

> ZGET counter
"\x02\x00\x00\x00\x00\x00\x00\x00"

> ZGET.I64 counter
(integer) 2
```

**Truncation caused by a short param:**

```kronotop
> ZSET counter "\x01\x00\x00\x00\x00\x00\x00\x00"
OK

> ZMUTATE counter "\x05" ADD
OK

> ZGET counter
"\x06"

> ZGET.I64 counter
(error) ERR Invalid stored value: expected 8-byte two's-complement int64
```

The counter is one byte wide now, so it is no longer a valid 64-bit integer.

**MAX and BYTE_MAX return different results:**

```kronotop
> ZSET a "\x00\x01"
OK

> ZMUTATE a "\x01\x00" MAX
OK

> ZGET a
"\x00\x01"
```

As a little-endian number, the stored value is 256 and `param` is 1, so the stored value wins.

```kronotop
> ZSET b "\x00\x01"
OK

> ZMUTATE b "\x01\x00" BYTE_MAX
OK

> ZGET b
"\x01\x00"
```

Byte by byte, `\x01` is greater than `\x00` at the first position, so `param` wins.

**Compare-and-clear:**

```kronotop
> ZSET mykey "my-value"
OK

> ZMUTATE mykey "my-value" COMPARE_AND_CLEAR
OK

> ZGET mykey
(nil)
```

**Use within an explicit transaction:**

```kronotop
> BEGIN
OK

> ZSET counter "\x01\x00\x00\x00\x00\x00\x00\x00"
OK

> ZMUTATE counter "\x05\x00\x00\x00\x00\x00\x00\x00" ADD
OK

> COMMIT
OK

> ZGET.I64 counter
(integer) 6
```
