---
title: "TRANSACTION LIST"
description: "Returns the open transactions of this member."
---

Returns the open transactions of this member.

## Syntax

```kronotop
TRANSACTION LIST
```

`LIST` takes no arguments.

## Return Value

An array with one map per open transaction, sorted by `id`. On RESP2 each map is a flat array of keys and values.

| Field           | Type           | Description                                                                    |
|-----------------|----------------|--------------------------------------------------------------------------------|
| `id`            | integer        | Transaction id, unique on this member                                          |
| `duration_ms`   | integer        | Milliseconds since the transaction was created                                 |
| `client_id`     | integer        | Client id of the session that owns the transaction, `-1` for the member's own  |
| `state`         | string         | `active` while a command runs, `idle` between two commands of a `BEGIN` block  |
| `namespace`     | string or null | Namespace of the command                                                       |
| `bucket`        | string or null | Bucket of the command, null for other data models                              |
| `model`         | string or null | Data model of the command: `Bucket` or `ZMap`, null for others                 |
| `command`       | string or null | Name of the command                                                            |
| `query`         | string or null | BQL query of the command, every value replaced by `?`                          |
| `watching`      | boolean        | True when the transaction holds a watch                                        |
| `reads`         | integer        | Point reads                                                                    |
| `range_reads`   | integer        | Range reads                                                                    |
| `writes`        | integer        | Writes                                                                         |
| `deletes`       | integer        | Deletes                                                                        |
| `range_deletes` | integer        | Range deletes                                                                  |
| `mutations`     | integer        | Atomic mutations                                                               |
| `bytes_read`    | integer        | Bytes read                                                                     |
| `bytes_written` | integer        | Bytes written                                                                  |

## Behavior

The list shows only the member that runs the command, not the whole cluster.

Each transaction shows the command that opened it. A transaction opened with `BEGIN` shows the last command that used
it. `BEGIN`, `COMMIT` and `ROLLBACK` are not shown as commands. Some commands, like `TICK` or `NAMESPACE.LIST`, open
their own transaction. Such a transaction is listed on its own, with the `client_id` of the session that sent the
command.

The member also opens transactions for its own work. These have `client_id` set to `-1` and no command, so
`namespace`, `bucket`, `model`, `command` and `query` are null.

Command arguments and keys are not stored. `query` shows only the structure of the query, with every value replaced
by `?`.

## Examples

**Another session ran a query in a `BEGIN` block and has not committed yet:**

```kronotop
> TRANSACTION LIST
1) 1# "id" => (integer) 42
   2# "duration_ms" => (integer) 1203
   3# "client_id" => (integer) 7
   4# "state" => "idle"
   5# "namespace" => "global"
   6# "bucket" => "users"
   7# "model" => "Bucket"
   8# "command" => "BUCKET.QUERY"
   9# "query" => "{\"age\": {\"$gt\": ?}}"
   10# "watching" => (false)
   11# "reads" => (integer) 3
   12# "range_reads" => (integer) 1
   13# "writes" => (integer) 0
   14# "deletes" => (integer) 0
   15# "range_deletes" => (integer) 0
   16# "mutations" => (integer) 0
   17# "bytes_read" => (integer) 512
   18# "bytes_written" => (integer) 0
```
