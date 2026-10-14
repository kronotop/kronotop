---
title: "TRANSACTIONS"
description: "Returns the open transactions of this member."
---

Returns the open transactions of this member.

## Syntax

```kronotop
TRANSACTIONS
```

This command takes no arguments.

## Return Value

Array: one map per open transaction, ordered by `id`.

| Field           | Type           | Description                                                                    |
|-----------------|----------------|--------------------------------------------------------------------------------|
| `id`            | integer        | Transaction id, unique on this member                                          |
| `duration_ms`   | integer        | Milliseconds since the transaction was created                                 |
| `client_id`     | integer        | Client id of the owning session, or `-1` for a background transaction          |
| `state`         | string         | `active` while a command runs, `idle` between two commands of a `BEGIN` block  |
| `namespace`     | string or null | Namespace of the running or last command                                       |
| `bucket`        | string or null | Bucket of the running or last command, null for other data models              |
| `model`         | string or null | Data model of the running or last command, `Bucket` or `ZMap`, null for others |
| `command`       | string or null | Name of the running or last command                                            |
| `query`         | string or null | BQL query of the command with every value replaced by `?`                      |
| `watching`      | boolean        | The transaction owns a watch                                                   |
| `reads`         | integer        | Point reads                                                                    |
| `range_reads`   | integer        | Range reads                                                                    |
| `writes`        | integer        | Writes                                                                         |
| `deletes`       | integer        | Deletes                                                                        |
| `range_deletes` | integer        | Range deletes                                                                  |
| `mutations`     | integer        | Atomic mutations                                                               |
| `bytes_read`    | integer        | Bytes read                                                                     |
| `bytes_written` | integer        | Bytes written                                                                  |

## Behavior

The list covers the member that serves the command, not the whole cluster. Background transactions opened by the
member itself appear with `client_id` set to `-1`; they have no session, so `namespace`, `bucket`, `model`,
`command` and `query` are null for them.

Only a command that runs on the session transaction is recorded. A command that opens its own transaction, such as
`TICK` or `NAMESPACE.LIST`, does not change `command`, `namespace`, `bucket` or `query`. Command arguments and keys are
never kept; `query` shows only the shape of the query.

## Examples

**List the transactions while another session runs a query in a `BEGIN` block:**

```kronotop
> TRANSACTIONS
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
