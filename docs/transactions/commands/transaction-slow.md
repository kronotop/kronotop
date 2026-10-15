---
title: "TRANSACTION SLOW"
description: "Returns the slow transactions logged by this member."
---

Returns the slow transactions logged by this member.

## Syntax

```kronotop
TRANSACTION SLOW
```

`SLOW` takes no arguments.

## Return Value

An array with one map per logged transaction, newest first. On RESP2 each map is a flat array of keys and values.

| Field               | Type           | Description                                                              |
|---------------------|----------------|--------------------------------------------------------------------------|
| `id`                | integer        | Transaction id, unique on this member                                    |
| `timestamp`         | integer        | Unix time in milliseconds when the transaction was closed                |
| `duration_ms`       | integer        | Milliseconds from creation to close                                      |
| `commit_latency_ms` | integer        | Milliseconds spent in commit, `0` when the transaction was not committed |
| `client_id`         | integer        | Client id of the session that owned the transaction, `-1` for internal   |
| `namespace`         | string or null | Namespace of the command                                                 |
| `bucket`            | string or null | Bucket of the command, null when the command has no bucket               |
| `model`             | string or null | Data model of the command: `Bucket` or `ZMap`, null for others           |
| `command`           | string or null | Name of the command                                                      |
| `query`             | string or null | BQL query of the command, every value replaced by `?`                    |
| `committed`         | boolean        | True when the transaction was committed                                  |
| `conflicted`        | boolean        | True when the commit failed with a conflict                              |
| `reads`             | integer        | Point reads                                                              |
| `range_reads`       | integer        | Range reads                                                              |
| `writes`            | integer        | Writes                                                                   |
| `deletes`           | integer        | Deletes                                                                  |
| `range_deletes`     | integer        | Range deletes                                                            |
| `mutations`         | integer        | Atomic mutations                                                         |
| `bytes_read`        | integer        | Bytes read                                                               |
| `bytes_written`     | integer        | Bytes written                                                            |

## Behavior

A transaction is logged when it is closed and its duration is at least `transaction.slow_log.threshold` milliseconds.
The log keeps the last `transaction.slow_log.max_entries` entries in memory. The oldest entry is dropped first. The log
is not written to disk. A restart clears it.

The log covers only this member, not the cluster.

A transaction that holds a watch is not logged. It stays open until the watched key changes, so its duration is not
useful.

Each entry shows the command that used the transaction. For a `BEGIN` block, this is the last command of the block.
Internal transactions have `client_id` `-1` and no command. For them `namespace`, `bucket`, `model`, `command` and
`query` are null.

Command arguments and keys are not stored. `query` shows only the structure of the query. Every value is replaced by
`?`.

## Examples

```kronotop
> TRANSACTION SLOW
1) 1# "id" => (integer) 42
   2# "timestamp" => (integer) 1791964800000
   3# "duration_ms" => (integer) 1850
   4# "commit_latency_ms" => (integer) 12
   5# "client_id" => (integer) 7
   6# "namespace" => "global"
   7# "bucket" => "users"
   8# "model" => "Bucket"
   9# "command" => "BUCKET.QUERY"
   10# "query" => "{\"age\": {\"$gt\": ?}}"
   11# "committed" => (true)
   12# "conflicted" => (false)
   13# "reads" => (integer) 3
   14# "range_reads" => (integer) 1
   15# "writes" => (integer) 0
   16# "deletes" => (integer) 0
   17# "range_deletes" => (integer) 0
   18# "mutations" => (integer) 0
   19# "bytes_read" => (integer) 512
   20# "bytes_written" => (integer) 0
```
