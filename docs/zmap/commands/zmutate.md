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

The `param` operand is raw bytes. For `ADD`, it must be a little-endian signed 64-bit integer (8 bytes). CLI tools
such as `kronotop-cli` or `valkey-cli` accept `\x` hex escape notation (e.g. `"\x05\x00\x00\x00\x00\x00\x00\x00"` for
integer 5).
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

| Type                       | Description                                                                                        |
|----------------------------|----------------------------------------------------------------------------------------------------|
| `ADD`                      | Interprets the existing value and param as little-endian integers and adds them.                   |
| `BIT_AND`                  | Performs a bitwise AND of the existing value and param.                                            |
| `BIT_OR`                   | Performs a bitwise OR of the existing value and param.                                             |
| `BIT_XOR`                  | Performs a bitwise XOR of the existing value and param.                                            |
| `APPEND_IF_FITS`           | Appends param to the existing value if the result fits within the value size limit.                |
| `MAX`                      | Sets the value to the larger of the existing value and param (unsigned lexicographic comparison).  |
| `MIN`                      | Sets the value to the smaller of the existing value and param (unsigned lexicographic comparison). |
| `SET_VERSIONSTAMPED_VALUE` | Sets the value with a versionstamp embedded in the value.                                          |
| `BYTE_MIN`                 | Sets the value to the byte-level minimum of the existing value and param.                          |
| `BYTE_MAX`                 | Sets the value to the byte-level maximum of the existing value and param.                          |
| `COMPARE_AND_CLEAR`        | Clears the key if its current value equals param.                                                  |

## Return Value

Simple string: `OK` on success.

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

| Error Code | Description                                                            |
|------------|------------------------------------------------------------------------|
| `ERR`      | Wrong number of arguments, invalid mutation type, or internal failure. |

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
