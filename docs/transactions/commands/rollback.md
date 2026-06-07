---
title: "ROLLBACK"
description: "Aborts the current transaction and discards all uncommitted changes."
---

Aborts the current transaction and discards all uncommitted changes.

## Syntax

```kronotop
ROLLBACK
```

This command takes no parameters.

## Return Value

Simple string: `OK` on success.

## Behavior

The command cancels the active FoundationDB transaction bound to the current session and releases all associated
resources. After cancellation, the session resets its transaction state: the BEGIN flag is cleared, auto-commit is
re-enabled, post-commit hooks are discarded, and the user version counter is reset.

The session returns to its default state and is ready to accept new commands or start a new transaction with `BEGIN`.

## Errors

| Error Code    | Description                                         |
|---------------|-----------------------------------------------------|
| `TRANSACTION` | There is no transaction in progress on the session. |

## Examples

**Roll back a transaction:**

```kronotop
> BEGIN
OK

> ROLLBACK
OK
```

**Attempt to roll back without an active transaction:**

```kronotop
> ROLLBACK
(error) TRANSACTION there is no transaction in progress.
```
