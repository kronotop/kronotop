---
title: "Cursor-Based Streaming"
description: "Every BUCKET.QUERY, BUCKET.DELETE, and BUCKET.UPDATE command returns results in batches through a cursor."
---

Every `BUCKET.QUERY`, `BUCKET.DELETE`, and `BUCKET.UPDATE` command returns results in batches through a cursor.
Rather than computing the entire result set up front, each call produces the next batch and advances the cursor's
position.
The total number of matching documents is not known in advance. You consume results batch by batch, calling
`BUCKET.ADVANCE`
until the batch comes back empty.

Outside an explicit `BEGIN`/`COMMIT` block, each `BUCKET.ADVANCE` call runs in its own transaction, so the work is
spread
across independent, short-lived transactions. When you are done, `BUCKET.CLOSE` releases the cursor.

## Why Batching

Kronotop delivers results in batches rather than all at once for two reasons:

- **Transaction time budget.** FoundationDB limits each transaction to approximately 5 seconds. A query matching
  thousands of documents cannot fetch, decode, and return them all within a single transaction. Batching splits the work
  across multiple short-lived transactions.
- **Memory efficiency.** Returning the entire result set at once would require buffering all matching documents in
  memory. Batching caps memory usage at the batch size.

## Cursor Lifecycle

A cursor goes through three phases: creation, advancing, and closing.

**Creation** happens automatically when you run `BUCKET.QUERY`, `BUCKET.DELETE`, or `BUCKET.UPDATE`. The response
includes a `cursor_id` and the first batch of results. The cursor stores the query filter, sort configuration, and
current position.

**Advancing** fetches subsequent batches. Pass the operation type and cursor ID to `BUCKET.ADVANCE`. When the
`entries` (or `object_ids`) array comes back empty consistently, the result set is exhausted (
see [Partial and Empty Batches](#partial-and-empty-batches)). There is no time limit between `BUCKET.ADVANCE` calls -- a
cursor remains valid indefinitely as long as the session is open and the cursor has not been closed. You can fetch the
first batch now, wait an hour, and call `BUCKET.ADVANCE` to pick up where you left off.

**Closing** releases the cursor. Always call `BUCKET.CLOSE` when you are done paginating.

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{}' LIMIT 2
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

## Batch Size

`LIMIT` controls how many documents (or object IDs) are returned per batch. When omitted, the session's default limit
applies.

The default limit is 100. You can change it per session:

```kronotop
127.0.0.1:5484> SESSION.ATTRIBUTE SET limit 50
OK
```

All subsequent queries in this session use 50 as the default batch size unless overridden by an explicit `LIMIT`
parameter.

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{}' LIMIT 10
```

This query returns at most 10 documents per batch, regardless of the session default.

See [BUCKET.QUERY](commands/bucket-query.md) for the full parameter reference.

## Checkpointing

After each batch, the cursor records the exact position where it stopped. Each `BUCKET.ADVANCE` call resumes from that
position.
No documents are skipped or duplicated between batches.

## Partial and Empty Batches

A batch may contain fewer results than `LIMIT` requested, or even zero results. This does **not** mean the result set is
exhausted.
The cursor's position is still valid, and the next `BUCKET.ADVANCE` call resumes from where the previous batch stopped.

A highly selective filter against a large dataset is the most common cause: many documents are examined but few match,
so the engine caps the work it performs per call and returns what it has found so far.

**When is the result set truly exhausted?** Keep calling `BUCKET.ADVANCE` until you receive empty batches consistently.
A selective filter may produce several empty batches before finding the next group of matches. The result set is
exhausted
when there are no more documents left to scan, not after a single empty batch.

## Cursors with UPDATE and DELETE

Cursors are not limited to reads. `BUCKET.UPDATE` and `BUCKET.DELETE` use the same streaming model. The only difference
is in the response shape: they return `object_ids` (the IDs of affected documents) instead of `entries` (full
documents).

```kronotop
127.0.0.1:5484> BUCKET.DELETE users '{"status": "inactive"}' LIMIT 2
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

Each `BUCKET.ADVANCE DELETE` call deletes the next batch of matching documents and returns their IDs. The same applies
to `BUCKET.ADVANCE UPDATE`.

See [BUCKET.DELETE](commands/bucket-delete.md) and [BUCKET.UPDATE](commands/bucket-update.md) for command details.

## Sorted Cursors

When `SORTBY` is used, the cursor iterates through a sort-field index in the requested direction. The checkpoint tracks
position in that index, so **global ordering is guaranteed across all batches**. The combined result of all
`BUCKET.ADVANCE`
calls forms a single, consistently sorted sequence.

```kronotop
BUCKET.QUERY events '{}' SORTBY created_at DESC LIMIT 5
```

Each batch returns the next 5 events in descending `created_at` order. No event appears out of order, even across batch
boundaries.

`SORTBY` is supported on `BUCKET.QUERY` and `BUCKET.UPDATE`, but not on `BUCKET.DELETE`.

See [SORTBY](sortby.md) for sorting details, compound index support, and pagination examples.

## Multiple Cursors

A session can have multiple cursors active at the same time. Each cursor has its own ID, operation type, filter, and
position.
Cursors are independent – advancing or closing one does not affect others.

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{"category": "books"}' LIMIT 5
1# "cursor_id" => (integer) 0
2# "entries" => ...

127.0.0.1:5484> BUCKET.DELETE products '{"category": "discontinued"}' LIMIT 10
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

## Session Binding

Cursors are bound to the session that created them. A cursor cannot be accessed from a different session. When a session
disconnects, all its cursors are released automatically.

## Best Practices

- **Always close cursors.** Open cursors hold state in the session. Close them with `BUCKET.CLOSE` as soon as you are
  done paginating.
- **Handle empty batches.** An empty batch does not always mean the result set is exhausted. Keep calling
  `BUCKET.ADVANCE` until empty batches come back consistently.
  See [Partial and Empty Batches](#partial-and-empty-batches).
- **Choose an appropriate LIMIT.** Smaller batches use less memory per transaction. Larger batches reduce round trips.
  The default (100) is a reasonable starting point for most workloads.
- **Use `BUCKET.CURSORS` for debugging.** List active cursors to verify none are leaked.
- **Use `SORTBY` when ordering matters.** Without `SORTBY`, document order across batches depends on the index the
  engine selected and is not guaranteed to be meaningful.
