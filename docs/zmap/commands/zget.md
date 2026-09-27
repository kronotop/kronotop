---
title: "ZGET"
description: "Retrieves the value for a key from the ZMap ordered key-value store."
---

Retrieves the value for a key from the ZMap ordered key-value store.

## Syntax

```kronotop
ZGET <key> [NAMESPACE <path>]
```

## Arguments

`key` is positional. `NAMESPACE` is a keyword argument and comes after it.

| Argument    | Type   | Required | Description                                                                                                                                              |
|-------------|--------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `key`       | bytes  | Yes      | The key to look up.                                                                                                                                      |
| `NAMESPACE` | string | No       | Run this command in the given namespace instead of the session's current one. The namespace must exist. The session's current namespace does not change. |

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

All data is scoped to a namespace: the session's active one, or the one given with `NAMESPACE`. The same key in different namespaces refers to different entries.

## Errors

Argument errors:

| Error Code | Error message                                        | Cause |
|------------|------------------------------------------------------|-------|
| `ERR`      | `wrong number of arguments for 'ZGET' command`       | -     |
| `ERR`      | `Unknown '<keyword>' argument`                       | -     |
| `ERR`      | `NAMESPACE argument must be followed by a namespace` | -     |

Namespace errors:

| Error Code              | Error message                         | Cause |
|-------------------------|---------------------------------------|-------|
| `NOSUCHNAMESPACE`       | `No such namespace: '<path>'`         | -     |
| `NAMESPACEBEINGREMOVED` | `Namespace '<path>' is being removed` | -     |

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
