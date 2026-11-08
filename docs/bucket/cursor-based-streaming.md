---
title: "Cursor-Based Streaming"
description: "Every BUCKET.QUERY, BUCKET.DELETE, and BUCKET.UPDATE command returns results in batches through a cursor."
---

`BUCKET.QUERY`, `BUCKET.DELETE`, and `BUCKET.UPDATE` return results in batches through a cursor. Kronotop does not
build the full result set up front. Each call computes the next batch and moves the cursor forward.

The total number of matching documents is not known in advance. You read the results one batch at a time and call
`BUCKET.ADVANCE` until the batch comes back empty.

Outside an explicit `BEGIN`/`COMMIT` block, each `BUCKET.ADVANCE` call runs in its own transaction, so the work is
spread over many short transactions. When you are done, `BUCKET.CLOSE` releases the cursor.

## Why batching

Kronotop returns results in batches for two reasons:

- FoundationDB limits a transaction to about 5 seconds. A query that matches thousands of documents cannot fetch,
  decode, and return all of them in one transaction. Batching splits the work across many short transactions.
- Returning the whole result set at once would mean holding every matching document in memory. With batching, the
  batch size limits memory use.

## Cursor lifecycle

A cursor has three phases: creation, advancing, and closing.

`BUCKET.QUERY`, `BUCKET.DELETE`, and `BUCKET.UPDATE` create the cursor automatically. The response contains a
`cursor_id` and the first batch of results. The cursor stores the query filter, the sort configuration, and the
current position.

`BUCKET.ADVANCE` fetches the next batches. Pass it the operation type and the cursor ID. When the `entries` (or
`object_ids`) array keeps coming back empty, the result set is exhausted
(see [Partial and empty batches](#partial-and-empty-batches)). There is no time limit between `BUCKET.ADVANCE` calls.
A cursor stays valid as long as the session is open and the cursor is not closed. You can fetch the first batch, wait
an hour, and then call `BUCKET.ADVANCE` to continue from the same point.

`BUCKET.CLOSE` releases the cursor. Always call it when you finish paginating. A cursor created with `LIMIT` is
removed automatically when it reaches the limit (see [Limit](#limit)). A command sent with `CLOSE` closes its own
cursor (see [Closing in the same call](#closing-in-the-same-call)).

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{}' BATCH 2
1# "cursor_id" => (integer) 0
2# "entries" =>
   1) {"_id": "69ce80c76597b10d87d134ff", "category": "books", "price": 19.99, "name": "The Disconnected"}
   2) {"_id": "69ce80c76597b10d87d13500", "category": "electronics", "price": 499.99, "name": "Wireless Headphones"}
```

Fetch the next batch:

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE QUERY 0
1# "cursor_id" => (integer) 0
2# "entries" =>
   1) {"_id": "69ce80c76597b10d87d13501", "category": "electronics", "price": 79.99, "name": "USB-C Hub"}
```

No more results:

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE QUERY 0
1# "cursor_id" => (integer) 0
2# "entries" => (empty array)
```

Close the cursor:

```kronotop
127.0.0.1:5484> BUCKET.CLOSE QUERY 0
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
1# "cursor_id" => (integer) 1
2# "entries" =>
   1) {"_id": "69ce80c76597b10d87d134ff", "category": "books", "price": 19.99, "name": "The Disconnected"}
   2) {"_id": "69ce80c76597b10d87d13500", "category": "electronics", "price": 499.99, "name": "Wireless Headphones"}
```

The second call returns the last document the limit allows:

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE QUERY 1
1# "cursor_id" => (integer) -1
2# "entries" =>
   1) {"_id": "69ce80c76597b10d87d13501", "category": "electronics", "price": 79.99, "name": "USB-C Hub"}
```

The cursor is gone:

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE QUERY 1
(error) ERR No previous query context found for 'query' operation with the given cursor id
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

A batch may contain fewer results than `BATCH` asked for, or no results at all. This does not mean the result set is
exhausted. The cursor position is still valid, and the next `BUCKET.ADVANCE` call continues from where the previous
batch stopped.

The most common cause is a very selective filter on a large dataset. The engine examines many documents, but few of
them match. It limits the work it does per call and returns the matches it has found so far.

To find out when the result set is exhausted, keep calling `BUCKET.ADVANCE` until empty batches come back
consistently. A selective filter can produce several empty batches before it finds the next matches. The result set
is exhausted only when there are no documents left to scan. One empty batch is not enough to tell.

## Cursors with UPDATE and DELETE

`BUCKET.UPDATE` and `BUCKET.DELETE` use the same streaming model as queries. The only difference is the response:
they return `object_ids` (the IDs of the affected documents) instead of `entries` (full documents).

```kronotop
127.0.0.1:5484> BUCKET.DELETE users '{"status": "inactive"}' BATCH 2
1# "cursor_id" => (integer) 3
2# "object_ids" =>
   1) "69ce80c76597b10d87d13510"
   2) "69ce80c76597b10d87d13511"
```

Advance to delete the next batch:

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE DELETE 3
1# "cursor_id" => (integer) 3
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
1# "cursor_id" => (integer) 0
2# "entries" => ...

127.0.0.1:5484> BUCKET.DELETE products '{"category": "discontinued"}' BATCH 10
1# "cursor_id" => (integer) 1
2# "object_ids" => ...
```

Use `BUCKET.CURSORS` to list all active cursors in the session:

```kronotop
127.0.0.1:5484> BUCKET.CURSORS
1# "QUERY" =>
   1# 0 => "{"category": "books"}"
2# "UPDATE" => (empty map)
3# "DELETE" =>
   1# 1 => "{"category": "discontinued"}"
```

You can also filter by operation type:

```kronotop
127.0.0.1:5484> BUCKET.CURSORS QUERY
1# "QUERY" =>
   1# 0 => "{"category": "books"}"
```

See [BUCKET.CURSORS](commands/bucket-cursors.md) for the full response format.

## Session binding

A cursor belongs to the session that created it. Other sessions cannot access it. When a session disconnects, all of
its cursors are released automatically.

## Best practices

- Always close cursors. Open cursors keep state in the session, so close them with `BUCKET.CLOSE` as soon as you
  finish paginating.
- If you do not plan to call `BUCKET.ADVANCE`, send the command with `CLOSE`.
  See [Closing in the same call](#closing-in-the-same-call).
- Expect empty batches. An empty batch does not always mean the result set is exhausted. Keep calling
  `BUCKET.ADVANCE` until empty batches come back consistently. See [Partial and empty batches](#partial-and-empty-batches).
- Pick a batch size that fits your workload. Smaller batches use less memory per transaction, and larger batches need
  fewer round trips. The default of 100 works well for most workloads.
- Set `LIMIT` when you need only the first N results, or when you want to update or delete at most N documents. The
  cursor is removed automatically when it reaches the limit. See [Limit](#limit).
- When debugging, use `BUCKET.CURSORS` to check that no cursors are leaked.
- Use `SORTBY` when order matters. Without it, the order of documents across batches depends on the index the engine
  picked and has no guaranteed meaning.
