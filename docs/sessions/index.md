---
title: "Sessions"
sidebar:
  label: "Overview"
description: "Every TCP connection to Kronotop creates exactly one session."
---

## Overview

Every TCP connection to Kronotop creates exactly one session. The session holds all per-client state: configuration
attributes, query cursors, and the active FoundationDB transaction. When the connection closes, the session is destroyed
and
all of its state is discarded.

`SESSION.CLOSE` provides a way to reset the session without dropping the underlying connection. This is useful for
connection-pooling scenarios or for returning a session to a known-good state between logical units of work.

## Session State

A session tracks the following categories of state:

- **Configuration attributes**: reply format, input format, result limit, object ID format
- **Active FoundationDB transaction**, at most one at a time, with its post-commit hooks and version counter
- **Query cursors**, three independent pools for read, delete, and update operations
- **Cursor ID counter**, a monotonically increasing integer scoped to the session

All of this state is connection-scoped and invisible to other sessions.

## Session Attributes

Four configurable attributes control how the session processes commands and formats responses:

| Attribute          | Type    | Default | Valid Values | Description                                       |
|--------------------|---------|---------|--------------|---------------------------------------------------|
| `reply_type`       | enum    | bson    | bson, json   | Data interchange format for responses             |
| `input_type`       | enum    | bson    | bson, json   | Data interchange format for inputs                |
| `limit`            | integer | 100     | > 0          | Maximum entries returned per query response       |
| `object_id_format` | enum    | bytes   | bytes, hex   | Encoding format for object ID values in responses |

Use `SESSION.ATTRIBUTE LIST` to view current values and `SESSION.ATTRIBUTE SET` to change them:

```kronotop
> SESSION.ATTRIBUTE LIST
1# reply_type => bson
2# input_type => bson
3# limit => (integer) 100
4# object_id_format => bytes

> SESSION.ATTRIBUTE SET limit 50
OK
```

Attributes are reset to defaults by `SESSION.CLOSE` or when the connection closes.

## Cursors

Queries do not return all matching documents at once. Instead, the first response includes a batch of results together
with a `cursor_id`. The client uses `BUCKET.ADVANCE` with that cursor ID to fetch subsequent batches until the result
set is exhausted.

```kronotop
> BUCKET.QUERY users '{"age": {"$gt": 18}}'
1# cursor_id => (integer) 1
2# entries => [ ... first batch ... ]

> BUCKET.ADVANCE QUERY 1
1# cursor_id => (integer) 1
2# entries => [ ... next batch ... ]
```

Cursor IDs are integers starting at 1 and increment within the session. Three independent cursor pools exist, one each
for read, delete, and update operations, so a read cursor and a delete cursor may share the same numeric ID without
a conflict.

`BUCKET.CLOSE` releases a specific cursor before it is naturally exhausted. `SESSION.CLOSE` releases all cursors at
once.

## Interaction with Transactions

By default, sessions operate in auto-commit mode. Each command that touches FoundationDB creates, executes, and commits
its own transaction. `BEGIN` opens an explicit transaction that spans multiple commands until `COMMIT` or `ROLLBACK`.

An explicit transaction is bound to the session. If the client issues `SESSION.CLOSE` while a transaction is in
progress, the transaction is automatically rolled back:

```kronotop
> BEGIN
OK

> SESSION.CLOSE
OK

> ROLLBACK
(error) TRANSACTION there is no transaction in progress.
```

Cursors are independent of transactions. A cursor created inside an explicit transaction survives `COMMIT` or `ROLLBACK`
and can continue to be advanced afterward.

Watched keys (`WATCH`) are also session-scoped. `SESSION.CLOSE` unwatches all keys, just as `DISCARD` does.

## Session Reset

`SESSION.CLOSE` performs a full session reset without closing the network connection. The reset proceeds in order:

1. **Clear all cursors**: read, delete, and update cursor pools are emptied
2. **Roll back the active transaction**: the FoundationDB transaction is closed and all uncommitted changes are lost
3. **Reset MULTI state**: queued commands are discarded and the MULTI flag is cleared
4. **Unwatch all keys**: every key in the session's watch list is released
5. **Reset the cursor ID counter**: the next cursor will start at 1 again
6. **Restore default attributes**: all four session attributes revert to their configured defaults

After `SESSION.CLOSE` returns `OK`, the session is indistinguishable from a freshly opened connection.

```kronotop
> SESSION.ATTRIBUTE SET limit 50
OK

> SESSION.CLOSE
OK

> SESSION.ATTRIBUTE LIST
1# reply_type => bson
2# input_type => bson
3# limit => (integer) 100
4# object_id_format => bytes
```

## Commands

| Command                                            | Description                         |
|----------------------------------------------------|-------------------------------------|
| [SESSION.ATTRIBUTE](commands/session-attribute.md) | View and modify session attributes  |
| [SESSION.CLOSE](commands/session-close.md)         | Reset all session state to defaults |
