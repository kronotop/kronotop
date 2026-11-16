---
title: "Cursor-Based Streaming"
description: "Every BUCKET.QUERY, BUCKET.DELETE, and BUCKET.UPDATE command returns results in batches through a cursor."
---

`BUCKET.QUERY`, `BUCKET.DELETE`, and `BUCKET.UPDATE` return results in batches through a cursor. Kronotop does not
build the full result set up front. Each call computes the next batch and moves the cursor forward.

The total number of matching documents is not known in advance. You read the results one batch at a time and call
`BUCKET.ADVANCE` until a response carries `cursor_id` `-1`.

Outside an explicit `BEGIN`/`COMMIT` block, each `BUCKET.ADVANCE` call runs in its own transaction, so the work is
spread over many short transactions. A cursor that reaches the end of the result set closes itself. `BUCKET.CLOSE`
releases a cursor that you stop reading early.

## Why batching

Kronotop returns results in batches for two reasons:

- FoundationDB limits a transaction to about 5 seconds and to 10 MB of affected data. The keys and key ranges that a
  transaction reads count toward that size. A query that matches thousands of documents cannot fetch, decode, and
  return all of them in one transaction. Batching splits the work across many short transactions.
- Returning the whole result set at once would mean holding every matching document in memory. With batching, the
  batch size limits memory use.

## Cursor lifecycle

A cursor has three phases: creation, advancing, and closing.

`BUCKET.QUERY`, `BUCKET.DELETE`, and `BUCKET.UPDATE` create the cursor automatically. The response contains a
`cursor_id` and the first batch of results. The cursor stores the query filter, the sort configuration, and the
current position. If the result set is smaller than the batch size, the first call returns all of it with
`cursor_id` `-1` and no cursor stays in the session.

`BUCKET.ADVANCE` fetches the next batches. Pass it the operation type and the cursor ID. There is no time limit
between `BUCKET.ADVANCE` calls. A cursor stays valid as long as the session is open and the cursor is not closed. You
can fetch the first batch, wait an hour, and then call `BUCKET.ADVANCE` to continue from the same point.

The cursor closes itself in three cases: the result set ends, the cursor reaches its `LIMIT` (see [Limit](#limit)),
or the command was sent with `CLOSE` (see [Closing in the same call](#closing-in-the-same-call)). In each case the
response carries `cursor_id` `-1` and the cursor is removed from the session. A later `BUCKET.ADVANCE` with the old
ID returns an error.

The call that reaches the end of the result set returns the last results together with `cursor_id` `-1`. If the
previous batch was full, that call returns an empty batch, because the engine finds the end only when it tries to
read more. An empty batch with a positive `cursor_id` means the cursor is still open
(see [Partial and empty batches](#partial-and-empty-batches)).

Call `BUCKET.CLOSE` to release a cursor that you stop reading before the end.

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{}' BATCH 2
1# "cursor_id" => (integer) 1
2# "entries" =>
   1) {"_id": "69ce80c76597b10d87d134ff", "category": "books", "price": 19.99, "name": "The Disconnected"}
   2) {"_id": "69ce80c76597b10d87d13500", "category": "electronics", "price": 499.99, "name": "Wireless Headphones"}
```

Fetch the next batch. It holds the last document, so the response carries `cursor_id` `-1`:

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE QUERY 1
1# "cursor_id" => (integer) -1
2# "entries" =>
   1) {"_id": "69ce80c76597b10d87d13501", "category": "electronics", "price": 79.99, "name": "USB-C Hub"}
```

To stop before the end, close the cursor. The next query opens a new cursor, and `BUCKET.CLOSE` releases it after the
first batch:

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{}' BATCH 2
1# "cursor_id" => (integer) 2
2# "entries" =>
   1) {"_id": "69ce80c76597b10d87d134ff", "category": "books", "price": 19.99, "name": "The Disconnected"}
   2) {"_id": "69ce80c76597b10d87d13500", "category": "electronics", "price": 499.99, "name": "Wireless Headphones"}

127.0.0.1:5484> BUCKET.CLOSE QUERY 2
OK
```

A closed cursor cannot be advanced. See [BUCKET.ADVANCE](commands/bucket-advance.md)
and [BUCKET.CLOSE](commands/bucket-close.md) for command details.

## Batch size

`BATCH` sets how many documents (or object IDs) each batch returns. If you omit it, the session's default batch size
applies.

The default batch size is 100. You can change it for a session:

```kronotop
127.0.0.1:5484> SESSION.ATTRIBUTE SET batch 50
OK
```

After this, all queries in the session use 50 as the default batch size. An explicit `BATCH` argument overrides it.

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{}' BATCH 10
```

This query returns at most 10 documents per batch, whatever the session default is.

See [BUCKET.QUERY](commands/bucket-query.md) for the full argument reference.

## Limit

`BATCH` limits a single call. `LIMIT` limits the whole cursor: the total number of documents (or object IDs) returned
by the first call and all `BUCKET.ADVANCE` calls together. If you omit it or set it to `0`, there is no limit.

Each call returns at most the smaller of `BATCH` and the remaining `LIMIT`. The call that reaches the limit returns
`cursor_id` `-1`, and the cursor is removed from the session. You do not need to close it, and a later
`BUCKET.ADVANCE` with the old ID returns an error.

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{}' BATCH 2 LIMIT 3
1# "cursor_id" => (integer) 3
2# "entries" =>
   1) {"_id": "69ce80c76597b10d87d134ff", "category": "books", "price": 19.99, "name": "The Disconnected"}
   2) {"_id": "69ce80c76597b10d87d13500", "category": "electronics", "price": 499.99, "name": "Wireless Headphones"}
```

The second call returns the last document the limit allows:

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE QUERY 3
1# "cursor_id" => (integer) -1
2# "entries" =>
   1) {"_id": "69ce80c76597b10d87d13501", "category": "electronics", "price": 79.99, "name": "USB-C Hub"}
```

The cursor is gone:

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE QUERY 3
(error) ERR No such cursor: '3'
```

`LIMIT` works the same way for `BUCKET.DELETE` and `BUCKET.UPDATE`. For `BUCKET.UPDATE`, a document inserted by
`upsert` counts as one toward the limit.

## Closing in the same call

If you need only one batch, add `CLOSE` to `BUCKET.QUERY`, `BUCKET.DELETE`, or `BUCKET.UPDATE`. The command returns
the first batch with `cursor_id` `-1` and leaves no cursor in the session, so you do not need a separate
`BUCKET.CLOSE` call.

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{}' BATCH 2 CLOSE
1# "cursor_id" => (integer) -1
2# "entries" =>
   1) {"_id": "69ce80c76597b10d87d134ff", "category": "books", "price": 19.99, "name": "The Disconnected"}
   2) {"_id": "69ce80c76597b10d87d13500", "category": "electronics", "price": 499.99, "name": "Wireless Headphones"}
```

`CLOSE` takes no value and can be combined with `LIMIT`. In both cases the cursor is closed after the first call.

## Checkpointing

After each batch, the cursor records the position where it stopped, and the next `BUCKET.ADVANCE` call continues
from there. No documents are skipped or returned twice between batches.

## Partial and empty batches

A batch may contain fewer results than `BATCH` asked for, or no results at all, while the cursor is still open. The
cursor position is still valid, and the next `BUCKET.ADVANCE` call continues from where the previous batch stopped.

The most common cause is a very selective filter on a large dataset. The engine examines many documents, but few of
them match. It limits the work it does per call and returns the matches it has found so far.

The per-call limit comes from FoundationDB. A transaction can run only for a limited time and read a limited amount
of data (see [Why batching](#why-batching)), so one call cannot scan an unlimited number of documents.

For example, take a large bucket with no index on the filtered field, where only the last document matches the
filter. The first calls return an empty batch with a positive `cursor_id`. A later call returns the document.

The `cursor_id` tells you whether to continue. A positive `cursor_id` means the cursor is still open, even when the
batch is empty, so call `BUCKET.ADVANCE` again. A selective filter can produce several empty batches before it finds
the next matches. A `cursor_id` of `-1` means there are no documents left to scan.

## Cursors with UPDATE and DELETE

`BUCKET.UPDATE` and `BUCKET.DELETE` use the same streaming model as queries. The only difference is the response:
they return `object_ids` (the IDs of the affected documents) instead of `entries` (full documents).

```kronotop
127.0.0.1:5484> BUCKET.DELETE users '{"status": "inactive"}' BATCH 2
1# "cursor_id" => (integer) 4
2# "object_ids" =>
   1) "69ce80c76597b10d87d13510"
   2) "69ce80c76597b10d87d13511"
```

The first batch was full, so the cursor is still open. The next call finds no more matching documents and closes the
cursor:

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE DELETE 4
1# "cursor_id" => (integer) -1
2# "object_ids" => (empty array)
```

Each `BUCKET.ADVANCE DELETE` call deletes the next batch of matching documents and returns their IDs.
`BUCKET.ADVANCE UPDATE` works the same way.

See [BUCKET.DELETE](commands/bucket-delete.md) and [BUCKET.UPDATE](commands/bucket-update.md) for command details.

## Sorted cursors

With `SORTBY`, the cursor walks a sort-field index in the requested direction. The checkpoint tracks the position in
that index, so the sort order holds across all batches. Together, the results of all `BUCKET.ADVANCE` calls form one
sorted sequence.

```kronotop
BUCKET.QUERY events '{}' SORTBY created_at DESC BATCH 5
```

Each batch returns the next 5 events in descending `created_at` order.

`BUCKET.QUERY` and `BUCKET.UPDATE` support `SORTBY`. `BUCKET.DELETE` does not.

See [SORTBY](sortby.md) for sorting details, compound index support, and pagination examples.

## Multiple cursors

A session can have several active cursors at the same time. Each cursor has its own ID, operation type, filter, and
position. Advancing or closing one cursor does not affect the others.

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{"category": "books"}' BATCH 5
1# "cursor_id" => (integer) 5
2# "entries" => ...

127.0.0.1:5484> BUCKET.DELETE products '{"category": "discontinued"}' BATCH 10
1# "cursor_id" => (integer) 6
2# "object_ids" => ...
```

Use `BUCKET.CURSORS` to list all active cursors in the session:

```kronotop
127.0.0.1:5484> BUCKET.CURSORS
1# "QUERY" =>
   1# (integer) 5 => {"category": "books"}
2# "UPDATE" => (empty map)
3# "DELETE" =>
   1# (integer) 6 => {"category": "discontinued"}
```

You can also filter by operation type:

```kronotop
127.0.0.1:5484> BUCKET.CURSORS QUERY
1# "QUERY" =>
   1# (integer) 5 => {"category": "books"}
```

See [BUCKET.CURSORS](commands/bucket-cursors.md) for the full response format.

## Session binding

A cursor belongs to the session that created it. Other sessions cannot access it. When a session disconnects, all of
its cursors are released automatically.

## Best practices

- Read until `cursor_id` is `-1`, the only sign that the result set has ended. The cursor is already closed at that
  point, so you do not need to call `BUCKET.CLOSE`.
- Close the cursors you stop reading early. Open cursors keep state in the session, so release them with
  `BUCKET.CLOSE`.
- If you do not plan to call `BUCKET.ADVANCE`, send the command with `CLOSE`.
  See [Closing in the same call](#closing-in-the-same-call).
- Expect empty batches. An empty batch with a positive `cursor_id` means the cursor is still open, so keep calling
  `BUCKET.ADVANCE`. See [Partial and empty batches](#partial-and-empty-batches).
- Pick a batch size that fits your workload. Smaller batches use less memory per transaction, and larger batches need
  fewer round trips. The default of 100 works well for most workloads.
- Set `LIMIT` when you need only the first N results, or when you want to update or delete at most N documents. The
  cursor is removed automatically when it reaches the limit. See [Limit](#limit).
- When debugging, use `BUCKET.CURSORS` to check that no cursors are leaked.
- Use `SORTBY` when order matters. Without it, the order of documents across batches depends on the index the engine
  picked and has no guaranteed meaning.
