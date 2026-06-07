---
title: "BEGIN"
description: "Opens a new FoundationDB transaction on the current session."
---

Opens a new FoundationDB transaction on the current session.

## Syntax

```kronotop
BEGIN
```

This command takes no parameters.

## Return Value

Simple string: `OK` on success.

## Behavior

The command creates a new FoundationDB transaction and binds it to the current session. The session enters explicit
transaction mode, which means all subsequent commands that touch FoundationDB will use this transaction until it is
committed or rolled back.

When a transaction is opened, the session also initializes post-commit hooks and a user version counter for the lifetime
of the transaction.

While in explicit transaction mode, auto-commit behavior is implicitly disabled: commands no longer create their own
one-off transactions.

## Errors

| Error Code    | Description                                          |
|---------------|------------------------------------------------------|
| `TRANSACTION` | A transaction is already in progress on the session. |

## Examples

**Start a transaction:**

```kronotop
> BEGIN
OK
```

**Attempt to start a second transaction:**

```kronotop
> BEGIN
OK

> BEGIN
(error) TRANSACTION there is already a transaction in progress.
```
