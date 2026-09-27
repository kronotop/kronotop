---
title: "ZDEL"
description: "Deletes a key from the ZMap ordered key-value store."
---

Deletes a key from the ZMap ordered key-value store.

## Syntax

```kronotop
ZDEL <key> [NAMESPACE <path>]
```

## Arguments

`key` is positional. `NAMESPACE` is a keyword argument and comes after it.

| Argument    | Type   | Required | Description                                                                                                                                              |
|-------------|--------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `key`       | bytes  | Yes      | The key to delete.                                                                                                                                       |
| `NAMESPACE` | string | No       | Run this command in the given namespace instead of the session's current one. The namespace must exist. The session's current namespace does not change. |

## Return Value

Simple string: `OK` on success.

## Behavior

`ZDEL` removes a key and its associated value from the ZMap subspace of the session's current namespace, backed by
FoundationDB.

The operation is idempotent: deleting a non-existent key returns `OK` without raising an error.

The command supports two transaction modes:

- **Auto-commit (one-off):** When no explicit transaction is active, Kronotop creates a transaction, performs the
  delete, and commits it immediately. This is the default mode.
- **Explicit transaction:** When a `BEGIN` has been issued, the delete is staged in the current transaction and only
  takes effect when `COMMIT` is called.

All data is scoped to a namespace: the session's active one, or the one given with `NAMESPACE`. The same key in different namespaces refers to different entries.

## Errors

Argument errors:

| Error Code | Error message                                        | Cause |
|------------|------------------------------------------------------|-------|
| `ERR`      | `wrong number of arguments for 'ZDEL' command`       | -     |
| `ERR`      | `Unknown '<keyword>' argument`                       | -     |
| `ERR`      | `NAMESPACE argument must be followed by a namespace` | -     |

Namespace errors:

| Error Code              | Error message                         | Cause |
|-------------------------|---------------------------------------|-------|
| `NOSUCHNAMESPACE`       | `No such namespace: '<path>'`         | -     |
| `NAMESPACEBEINGREMOVED` | `Namespace '<path>' is being removed` | -     |

## Examples

**Delete an existing key:**

```kronotop
> ZSET mykey "Hello"
OK

> ZDEL mykey
OK

> ZGET mykey
(nil)
```

**Delete a non-existent key:**

```kronotop
> ZDEL nosuchkey
OK
```

**Use within an explicit transaction:**

```kronotop
> ZSET mykey "Hello"
OK

> BEGIN
OK

> ZDEL mykey
OK

> COMMIT
OK

> ZGET mykey
(nil)
```
