---
title: "COMMIT"
description: "Commits the current transaction and applies all changes."
---

Commits the current transaction and applies all changes.

## Syntax

```kronotop
COMMIT [RETURNING committed-version | versionstamp]
```

## Arguments

| Argument    | Required | Description                                                                                           |
|-------------|----------|-------------------------------------------------------------------------------------------------------|
| `RETURNING` | No       | Requests a value from the committed transaction. Must be followed by exactly one of the values below. |

**RETURNING values:**

| Value               | Description                                                                                                      |
|---------------------|------------------------------------------------------------------------------------------------------------------|
| `committed-version` | The version at which the transaction was committed. Returns `-1` if the transaction contained no mutations.      |
| `versionstamp`      | The 10-byte versionstamp assigned to the transaction. The transaction must have performed at least one mutation. |

## Return Value

- **No RETURNING**: Simple string `OK`.
- **RETURNING committed-version**: Integer representing the committed version. Returns `-1` if the transaction
  contained no mutations, meaning there was nothing to commit.
- **RETURNING versionstamp**: Bulk string containing the 10-byte versionstamp.

## Behavior

The command commits the active FoundationDB transaction bound to the current session. After a successful commit, any
registered post-commit hooks are executed. The session then resets its transaction state: the BEGIN flag is cleared,
auto-commit is re-enabled, post-commit hooks are discarded, and the user version counter is reset.

When `RETURNING versionstamp` is requested, the versionstamp future is obtained before the commit call, as required by
the FoundationDB API. The resolved value is returned after the commit completes.

The session returns to its default state and is ready to accept new commands or start a new transaction with `BEGIN`.

A transaction that contains at least one write must be committed within 5 seconds of `BEGIN`. After that, the command
fails with `TRANSACTION_TOO_OLD` and no change is applied. The transaction is closed, so run `BEGIN` again and repeat
the work. See [Transaction Time Window](../index.md#transaction-time-window).

## Errors

Argument errors:

| Error Code | Error message                                                              | Cause                                                   |
|------------|----------------------------------------------------------------------------|---------------------------------------------------------|
| `ERR`      | `Unknown argument: '<argument>'`                                           | -                                                       |
| `ERR`      | `RETURNING argument must be followed by VERSIONSTAMP or COMMITTED_VERSION` | -                                                       |
| `ERR`      | `Unknown RETURNING argument: '<value>'`                                    | The value is not `committed-version` or `versionstamp`. |

Transaction errors:

| Error Code            | Error message                                             | Cause                                                                                                                           |
|-----------------------|-----------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| `TRANSACTION`         | `There is no transaction in progress`                     | -                                                                                                                               |
| `TRANSACTION_TOO_OLD` | `Transaction is too old to perform reads or be committed` | The transaction contains writes and more than 5 seconds have passed since `BEGIN`, or its read version is older than 5 seconds. |

## Examples

**Commit a transaction:**

```kronotop
> BEGIN
OK

> COMMIT
OK
```

**Commit and return the committed version:**

```kronotop
> BEGIN
OK

> ZSET mykey 42
OK

> COMMIT RETURNING committed-version
(integer) 1234567890
```

**Commit a transaction with no mutations:**

```kronotop
> BEGIN
OK

> COMMIT RETURNING committed-version
(integer) -1
```

A return value of `-1` means the transaction contained no mutations and there was nothing to commit.

**Commit and return the versionstamp:**

```kronotop
> BEGIN
OK

> ZSET mykey 42
OK

> COMMIT RETURNING versionstamp
"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
```

**Attempt to commit without an active transaction:**

```kronotop
> COMMIT
(error) TRANSACTION There is no transaction in progress
```

**Attempt to commit a transaction with writes more than 5 seconds after `BEGIN`:**

```kronotop
> BEGIN
OK

> ZSET mykey 42
OK

-- 8 seconds pass --

> COMMIT
(error) TRANSACTION_TOO_OLD Transaction is too old to perform reads or be committed
```
