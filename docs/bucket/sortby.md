---
title: "Sorting"
description: "SORTBY controls the ordering of documents returned by BUCKET.QUERY and BUCKET.UPDATE."
---

`SORTBY` controls the ordering of documents returned by `BUCKET.QUERY` and `BUCKET.UPDATE`. It accepts a field name and
a direction (`ASC` or `DESC`). `BUCKET.DELETE` does not support `SORTBY`.

```kronotop
BUCKET.QUERY users '{}' SORTBY age ASC LIMIT 10
```

`SORTBY` requires an index on the sort field. The index provides natural ordering. FoundationDB stores index entries in
sorted key order, so iterating through the index returns documents in the requested order without any in-memory sorting.

If no suitable index exists, the query is rejected at planning time with an actionable error message.

## How SORTBY Works

When the SORTBY field has a matching index and the optimizer selects it, the index itself provides the sort order.

- `ASC` reads the index forward.
- `DESC` reverses the FoundationDB scan direction.
- The cursor checkpoint tracks the position in the index, so each `BUCKET.ADVANCE` call picks up where the previous
  batch ended.
- **Global ordering is guaranteed.** Documents across all batches form a single, consistently sorted sequence.

Example: `age` is indexed.

```kronotop
127.0.0.1:5484> BUCKET.QUERY users '{}' SORTBY age ASC LIMIT 3
1# "cursor_id" => (integer) 2
2# "entries" =>
   1) {"_id": "69ce80c76597b10d87d134ff", "age": 20}
   2) {"_id": "69ce80c76597b10d87d13500", "age": 25}
   3) {"_id": "69ce80c76597b10d87d13501", "age": 30}
```

Pagination via `BUCKET.ADVANCE`:

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE QUERY 2
1# "cursor_id" => (integer) 2
2# "entries" =>
   1) {"_id": "69ce80c76597b10d87d13502", "age": 35}
   2) {"_id": "69ce80c76597b10d87d13503", "age": 40}
   3) {"_id": "69ce80c76597b10d87d13504", "age": 45}
```

Every batch continues in strict ascending order.

## Collation

`SORTBY` ordering is determined by the collation used when the index was built. The index stores string values as ICU4J
collation keys in FoundationDB, so the physical key order already encodes the locale-aware sort order.

The query-level `COLLATION` parameter does **not** affect `SORTBY` ordering. It applies to filter predicate evaluation,
but it cannot change the order in which the index returns documents. That order is fixed at index creation time.

If the query specifies a collation that differs from the index's collation, the query is rejected at planning time:

```kronotop
127.0.0.1:5484> BUCKET.QUERY users '{}' SORTBY name ASC COLLATION '{
  "locale": "fr"
}'
(error) ERR query cannot be executed: SORTBY 'name' cannot use the existing index because its collation does not match the query collation. Hint: use a collation that matches the index on 'name'
```

To sort strings under a specific locale, create the index with that collation and issue queries without a conflicting
`COLLATION` override:

```kronotop
> BUCKET.INDEX CREATE users '{
  "name": {
    "bson_type": "string",
    "collation": {"locale": "tr", "strength": 1}
  }
}'
> BUCKET.QUERY users '{}' SORTBY name ASC
```

If you need a different collation for sorting than what the index provides, use `RESULTSORT` instead. It performs an
in-memory sort on each batch and respects the full collation resolution order.

## Compound Index Support

`SORTBY` works with compound indexes. Range queries on the sort field itself are fine. The index naturally provides
ordering within the scanned range. The only restriction is on **prefix fields**: all compound index fields **before**
the sort field must use equality (`EQ`) filters. A range filter on a prefix field breaks the trailing-field ordering.

Example: compound index on `(status, age)`.

```kronotop
-- EQ on 'status', SORTBY on 'age' → works
BUCKET.QUERY users '{"status": "active"}' SORTBY age ASC LIMIT 10

-- Range on 'age', SORTBY on 'age' → works (sort field = range field)
BUCKET.QUERY users '{"age": {"$gt": 5, "$lt": 100}}' SORTBY age ASC LIMIT 10

-- Range on 'status', SORTBY on 'age' → rejected
BUCKET.QUERY users '{"status": {"$gt": "a"}}' SORTBY age ASC LIMIT 10
```

The last query is rejected because the range filter on `status` (a prefix field) breaks the ordering of `age` within the
compound index.

## Filter and Sort on Separate Single-Field Indexes

When the filter field and the sort field have separate single-field indexes, the query is rejected. The execution plan
uses the filter-field index, which does not provide ordering on the sort field.

```kronotop
BUCKET.QUERY users '{"status": "active"}' SORTBY age ASC LIMIT 10
```

If `status` and `age` each have their own single-field index, this query fails. Create a compound index on
`(status, age)` instead. The compound index provides both filtering on the prefix field and natural ordering on the sort
field.

## Validation Errors

When `SORTBY` cannot be satisfied, the planner rejects the query with an actionable error message:

### No index on the sort field

The sort field has no index, so the engine cannot provide ordered results.

```kronotop
127.0.0.1:5484> BUCKET.QUERY users '{"status": "active"}' SORTBY score ASC
(error) ERR query cannot be executed: SORTBY 'score' requires an index that provides natural ordering. Hint: create an index on 'score'
```

### Compound index range prefix conflict

A compound index contains the sort field, but a range filter on a preceding field breaks the trailing-field ordering.

```kronotop
BUCKET.QUERY users '{"status": {"$gt": "a"}}' SORTBY age ASC
```

Compound index on `(status, age)`. The range on `status` breaks `age` ordering. The planner returns:

```kronotop
127.0.0.1:5484> BUCKET.QUERY users '{"status": {"$gt": "a"}}' SORTBY age ASC
(error) ERR query cannot be executed: SORTBY 'age' cannot be executed because a range filter on 'status' breaks compound index ordering. Hint: use an equality filter on 'status' or create an index on 'age'
```

Two ways to fix: change the prefix filter to equality or create a dedicated single-field index on the sort field.

## Negation and Set Operators

`$ne`, `$nin`, `$in`, and `$nor` interact with `SORTBY` differently depending on whether the operator targets the same
field as the sort field.

### $ne and $nin

`$ne` produces a single index scan that skips the excluded value. `$nin` becomes an AND of not-equal scans, which
collapses to a single index scan with residual NE predicates. In both cases, the index scan iterates in order and
filters out excluded values. Ordering is preserved.

```kronotop
-- 'age' is indexed
BUCKET.QUERY users '{"age": {"$ne": 25}}' SORTBY age ASC LIMIT 10
BUCKET.QUERY users '{"age": {"$nin": [20, 40]}}' SORTBY age ASC LIMIT 10
```

Both queries scan the `age` index in ascending order and skip excluded values. Global sort ordering is guaranteed.

`$ne` and `$nin` on a **different** field than the sort field are rejected:

```kronotop
127.0.0.1:5484> BUCKET.QUERY users '{
  "role": {"$nin": ["admin", "editor"]}
}' SORTBY age ASC
(error) ERR query cannot be executed: SORTBY 'age' cannot be used because the query's execution plan does not provide natural ordering on this field. Hint: use RESULTSORT 'age' for in-memory sorting
```

### $in

`$in` on the **same** indexed field as the sort field produces globally sorted results. The engine executes individual
EQ scans sequentially in value order. Each scan returns entries for a single value, and scans are ordered by value. This
avoids scanning the entire index.

```kronotop
-- 'age' is indexed
BUCKET.QUERY users '{"age": {"$in": [30, 10, 50]}}' SORTBY age ASC LIMIT 10
```

The engine executes EQ scans in order: first `age = 10`, then `age = 30`, then `age = 50`. Each scan is exhausted before
the next begins. Pagination across value boundaries is handled correctly. A `BUCKET.ADVANCE` call may resume mid-value
or cross into the next value.

`$in` on a **different** field than the sort field is rejected:

```kronotop
127.0.0.1:5484> BUCKET.QUERY users '{
  "name": {"$in": ["Alice", "Bob"]}
}' SORTBY age ASC
(error) ERR query cannot be executed: SORTBY 'age' cannot be used because the query's execution plan does not provide natural ordering on this field. Hint: use RESULTSORT 'age' for in-memory sorting
```

### $nor

`$nor` desugars to `$not($or(...))`. Negation always produces a full scan with a residual predicate. The full scan
iterates by `_id`, not by the sort field. `SORTBY` is rejected:

```kronotop
127.0.0.1:5484> BUCKET.QUERY users '{
  "$nor": [{"age": 10}, {"age": 30}]
}' SORTBY age ASC
(error) ERR query cannot be executed: SORTBY 'age' cannot be used because the query's execution plan does not provide natural ordering on this field. Hint: use RESULTSORT 'age' for in-memory sorting
```

Use `RESULTSORT` for in-memory per-batch sorting when `SORTBY` is not available.

## Pagination

The cursor checkpoint stores the position in the index. Each advance resumes from the exact key where the previous batch
stopped. The combined result of all batches is identical to sorting the full result set.

```kronotop
-- 'created_at' is indexed
127.0.0.1:5484> BUCKET.QUERY events '{}' SORTBY created_at DESC LIMIT 5
1# "cursor_id" => (integer) 3
2# "entries" =>
   1) {"_id": "69ce82626597b10d87d1350f", "created_at": {"$date": "2025-12-05T00:00:00Z"}}
   2) {"_id": "69ce82626597b10d87d1350e", "created_at": {"$date": "2025-12-04T00:00:00Z"}}
   3) {"_id": "69ce82626597b10d87d1350d", "created_at": {"$date": "2025-12-03T00:00:00Z"}}
   4) {"_id": "69ce82626597b10d87d1350c", "created_at": {"$date": "2025-12-02T00:00:00Z"}}
   5) {"_id": "69ce82626597b10d87d1350b", "created_at": {"$date": "2025-12-01T00:00:00Z"}}
```

```kronotop
127.0.0.1:5484> BUCKET.ADVANCE QUERY 3
1# "cursor_id" => (integer) 3
2# "entries" =>
   1) {"_id": "69ce82626597b10d87d1350a", "created_at": {"$date": "2025-11-30T00:00:00Z"}}
   2) {"_id": "69ce82626597b10d87d13509", "created_at": {"$date": "2025-11-29T00:00:00Z"}}
   3) {"_id": "69ce82626597b10d87d13508", "created_at": {"$date": "2025-11-28T00:00:00Z"}}
   4) {"_id": "69ce82626597b10d87d13507", "created_at": {"$date": "2025-11-27T00:00:00Z"}}
   5) {"_id": "69ce82626597b10d87d13506", "created_at": {"$date": "2025-11-26T00:00:00Z"}}
```

## RESULTSORT

`RESULTSORT` is a separate parameter that provides in-memory per-batch sorting on any field, indexed or not. Unlike
`SORTBY`, it does not require an index and does not guarantee global ordering across batches.

```kronotop
BUCKET.QUERY users '{}' RESULTSORT score ASC LIMIT 10
```

- Works on any field, no index required.
- Each batch is sorted independently in memory after documents are fetched.
- **Global ordering across `BUCKET.ADVANCE` calls is NOT guaranteed.** Batch N+1 may contain values that would sort
  before values in batch N.
- `RESULTSORT` is available for `BUCKET.QUERY` only, not for `BUCKET.UPDATE` or `BUCKET.DELETE`.

`SORTBY` and `RESULTSORT` serve different purposes: `SORTBY` provides globally sorted results through index ordering,
while `RESULTSORT` provides per-batch ordering when an index is unavailable or unnecessary.

### Collation

When string values are compared during `RESULTSORT`, the effective collation is resolved in this order:

1. **Query-level collation**: set via the `COLLATION` parameter on the query.
2. **Index-level collation (single-field)**: the collation defined on a single-field index whose selector matches the
   `RESULTSORT` field.
3. **Index-level collation (compound)**: if all READY compound indexes containing the `RESULTSORT` field as a `string`
   field agree on the same collation, that collation is used. If any two disagree, this step is skipped.
4. **Bucket-level collation**: the default collation configured on the bucket.
5. **Binary comparison**: used when none of the above apply.

This matches the collation precedence used for filter predicate evaluation, so the same field behaves consistently
whether it appears in a filter or in `RESULTSORT`.

## Sorting by `_id`

The primary index on `_id` is always `READY`, so `SORTBY _id ASC|DESC` works without creating any additional indexes.
Since ObjectId values encode a timestamp in their leading bytes, sorting by `_id` produces approximate insertion-order
results.

```kronotop
BUCKET.QUERY users '{}' SORTBY _id ASC LIMIT 10
```

This is useful for scanning documents in the order they were inserted, or for fetching the most recently inserted
documents with `DESC`.

## Null and Missing Field Handling

When a document is missing the SORTBY field, or the field is explicitly `null`, the sort key is treated as `BsonNull`.

- `BsonNull` has type order 0 (the lowest rank in the type bracket ordering).
- In `ASC` order, nulls sort **first**.
- In `DESC` order, nulls sort **last**.

See [type-bracketing.md](type-bracketing.md) for the full type order and comparison rules.

## Plan Cache Interaction

The SORTBY field is part of the plan cache key. Two queries with the same filter but different SORTBY fields produce
different cache entries because different sort fields lead to different index selection during physical planning.

```kronotop
-- These produce separate cache entries:
BUCKET.QUERY users '{}' SORTBY age ASC
BUCKET.QUERY users '{}' SORTBY name ASC
```

See [plan-cache.md](plan-cache.md) for details on cache keys, TTL, and eviction.

## Best Practices

- **Create indexes on fields you sort by.** `SORTBY` requires an index. Without one, the query is rejected.
- **Use `BUCKET.EXPLAIN`** to verify which index the optimizer selected.
- **Use `RESULTSORT`** when you need within-batch ordering on a field that doesn't have an index and global ordering is
  not required.
- **Keep compound index prefix fields as EQ filters** when you need to sort on a trailing field in the compound index.
