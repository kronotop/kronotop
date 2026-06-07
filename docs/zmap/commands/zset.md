---
title: "ZSET"
description: "Sets a key-value pair in the ZMap ordered key-value store."
---

Sets a key-value pair in the ZMap ordered key-value store.

## Syntax

```kronotop
ZSET <key> <value>
```

## Parameters

| Parameter | Type  | Required | Description                          |
|-----------|-------|----------|--------------------------------------|
| `key`     | bytes | Yes      | The key to set.                      |
| `value`   | bytes | Yes      | The value to associate with the key. |

## Return Value

Simple string: `OK` on success.

## Behavior

`ZSET` writes a key-value pair into the ZMap subspace of the session's current namespace, backed by FoundationDB. Keys
are stored in lexicographic order.

If the key already exists, its value is overwritten silently. There is no duplicate-key error.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the write,
  and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the write is staged in the current transaction and only
  persists when `COMMIT` is called.

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries.

## Errors

| Error Code | Description                                    |
|------------|------------------------------------------------|
| `ERR`      | Wrong number of arguments or internal failure. |

## Examples

**Set a key-value pair:**

```kronotop
> ZSET mykey "Hello"
OK
```

**Overwrite an existing key:**

```kronotop
> ZSET mykey "Hello"
OK

> ZSET mykey "World"
OK

> ZGET mykey
"World"
```

**Use within an explicit transaction:**

```kronotop
> BEGIN
OK

> ZSET mykey "Hello"
OK

> ZSET anotherkey "World"
OK

> COMMIT
OK
```
