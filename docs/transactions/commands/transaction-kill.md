---
title: "TRANSACTION KILL"
description: "Cancels an open transaction of this member."
---

Cancels an open transaction of this member.

## Syntax

```kronotop
TRANSACTION KILL id
```

| Argument | Type    | Description                                    |
|----------|---------|------------------------------------------------|
| `id`     | integer | Transaction id as listed by `TRANSACTION LIST` |

## Return Value

Simple string: `OK` on success.

## Behavior

The command cancels the transaction with the given `id` on this member only. Use `TRANSACTION LIST` to find the id.

The transaction is not closed. It stays in the `TRANSACTION LIST` output until its session runs `ROLLBACK`. That session
gets a `TRANSACTION_CANCELLED` error on its next read or on `COMMIT`. A write alone does not fail, because writes
wait on the client until commit. An auto-commit transaction is closed when its command fails.

Transactions with `client_id` `-1` belong to the member itself. They cannot be killed.

## Errors

Transaction errors:

| Error Code    | Error message                            | Cause                                        |
|---------------|------------------------------------------|----------------------------------------------|
| `TRANSACTION` | `no such transaction: <id>`              | No open transaction has this id              |
| `TRANSACTION` | `internal transactions cannot be killed` | The transaction belongs to the member itself |

## Examples

**Kill the transaction of another session:**

```kronotop
> TRANSACTION KILL 42
OK
```

The owner session then sees:

```kronotop
> ZGET key
(error) TRANSACTION_CANCELLED Operation aborted because the transaction was cancelled

> ROLLBACK
OK
```
