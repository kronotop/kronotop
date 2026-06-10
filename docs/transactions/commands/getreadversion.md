---
title: "GETREADVERSION"
description: "Returns the read version of the current transaction."
---

Returns the read version of the current transaction.

## Syntax

```kronotop
GETREADVERSION
```

This command takes no parameters.

## Return Value

Integer: the read version of the active transaction.

## Behavior

Every FoundationDB transaction is assigned a read version that determines the snapshot of the database it observes. The
read version is a monotonically increasing value that reflects causal ordering: a higher read version means the
transaction sees a later state of the database.

This command returns the read version of the active transaction on the current session. It can be used to reason about
causal ordering between transactions, coordinate across sessions, or diagnose transaction behavior.

A transaction must be started with `BEGIN` before calling this command.

## Errors

| Error Code    | Description                                                                   |
|---------------|-------------------------------------------------------------------------------|
| `TRANSACTION` | `there is no transaction in progress.`: No active transaction on the session. |

## Examples

**Get the read version of an active transaction:**

```kronotop
> BEGIN
OK

> GETREADVERSION
(integer) 1391961467874
```

**Attempt without an active transaction:**

```kronotop
> GETREADVERSION
(error) TRANSACTION there is no transaction in progress.
```
