---
title: "ZGET"
description: "Retrieves the value for a key from the ZMap ordered key-value store."
---

Retrieves the value for a key from the ZMap ordered key-value store.

## Syntax

```kronotop
ZGET <key>
```

## Parameters

| Parameter | Type  | Required | Description         |
|-----------|-------|----------|---------------------|
| `key`     | bytes | Yes      | The key to look up. |

## Return Value

Bulk string: the value associated with the key, or `nil` if the key does not exist.

## Behavior

`ZGET` reads the value for a given key from the ZMap subspace of the session's current namespace, backed by
FoundationDB.

If the key does not exist, the command returns `nil`.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the read,
  and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the read is performed within the current transaction.

`ZGET` also supports **snapshot reads**. When snapshot mode is enabled on the session, the read does not conflict with
concurrent writes, allowing higher throughput for read-heavy workloads.

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries.

## Errors

| Error Code | Description                                    |
|------------|------------------------------------------------|
| `ERR`      | Wrong number of arguments or internal failure. |

## Examples

**Get an existing key:**

```kronotop
> ZSET mykey "Hello"
OK

> ZGET mykey
"Hello"
```

**Get a non-existent key:**

```kronotop
> ZGET nosuchkey
(nil)
```

**Use within an explicit transaction:**

```kronotop
> BEGIN
OK

> ZSET mykey "Hello"
OK

> COMMIT
OK

> BEGIN
OK

> ZGET mykey
"Hello"

> COMMIT
OK
```
