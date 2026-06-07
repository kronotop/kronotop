---
title: "GETAPPROXIMATESIZE"
description: "Returns the approximate byte size of the current transaction."
---

Returns the approximate byte size of the current transaction.

## Syntax

```kronotop
GETAPPROXIMATESIZE
```

This command takes no parameters.

## Return Value

Integer: the approximate size of the transaction in bytes.

## Behavior

Returns the approximate number of bytes of mutations that have been performed on the current transaction. This value
includes the combined size of all keys and values that have been set, cleared, or otherwise mutated since the
transaction began.

This is useful for monitoring how much data a transaction has written, particularly when approaching FoundationDB's 10
MB transaction size limit. Clients can use this to decide when to split work across multiple transactions or for logging
and diagnostics.

A transaction must be started with `BEGIN` before calling this command.

## Errors

| Error Code    | Description                                                                   |
|---------------|-------------------------------------------------------------------------------|
| `TRANSACTION` | `there is no transaction in progress.`: No active transaction on the session. |

## Examples

**Check approximate size after performing writes:**

```kronotop
> BEGIN
OK

> ZSET key1 42
OK

> GETAPPROXIMATESIZE
(integer) 156
```

**Attempt without an active transaction:**

```kronotop
> GETAPPROXIMATESIZE
(error) TRANSACTION there is no transaction in progress.
```
