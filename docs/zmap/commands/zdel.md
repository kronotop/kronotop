---
title: "ZDEL"
description: "Deletes a key from the ZMap ordered key-value store."
---

Deletes a key from the ZMap ordered key-value store.

## Syntax

```kronotop
ZDEL <key>
```

## Parameters

| Parameter | Type  | Required | Description        |
|-----------|-------|----------|--------------------|
| `key`     | bytes | Yes      | The key to delete. |

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

All data is scoped to the session's active namespace. The same key in different namespaces refers to different entries.

## Errors

| Error Code | Description                                    |
|------------|------------------------------------------------|
| `ERR`      | Wrong number of arguments or internal failure. |

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
