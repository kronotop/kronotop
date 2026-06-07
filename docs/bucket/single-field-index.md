---
title: "Single-Field Indexes"
description: "A single-field index covers one field in a document."
---

## Introduction

A single-field index covers one field in a document. It accelerates queries that filter on that field by allowing the
query engine to scan the index instead of reading every document in the bucket. Single-field indexes are the most common
index type and the right default choice when your queries filter on individual fields.

## Creation

Single-field indexes are defined as top-level keys in the index schema. Each key is a field selector, and the value
specifies the index properties:

```json
{
  "username": {
    "bson_type": "string"
  }
}
```

With the command:

```kronotop
BUCKET.INDEX CREATE users '{"username": {"bson_type": "string"}}'
```

With a custom name:

```kronotop
BUCKET.INDEX CREATE users '{"username": {"bson_type": "string", "name": "idx_username"}}'
```

If `name` is omitted, a name is auto-generated from the selector and type. For example, `username` with type `string`
produces `selector:username.bsonType:STRING`.

Multiple single-field indexes can be created in one command:

```kronotop
BUCKET.INDEX CREATE users '{"age": {"bson_type": "int32"}, "email": {"bson_type": "string"}}'
```

### Nested fields

Use dot notation to index fields inside nested objects:

```kronotop
BUCKET.INDEX CREATE users '{"address.city": {"bson_type": "string"}}'
```

Given a document `{"address": {"city": "Istanbul", "zip": "34000"}}`, the selector `address.city` reaches `"Istanbul"`.
See [Dot Notation](dot-notation.md) for the full path syntax.

### Collation

String-typed indexes can specify a collation to control comparison rules:

```kronotop
BUCKET.INDEX CREATE users '{"username": {"bson_type": "string", "collation": {"locale": "tr"}}}'
```

When a query uses a collation, the query engine only selects an index if its collation is compatible.
See [Collation](collation.md) for details.

See [BUCKET.INDEX CREATE](commands/bucket-index.md#bucketindex-create) for the full command reference.

## Supported operators

The following operators can use a single-field index:

| Operator     | Behavior                                                                                                                                             |
|--------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `$eq`        | Equality lookup. Point scan on the index.                                                                                                            |
| `$ne`        | Not-equal scan. Not supported on multi-key indexes (falls back to full scan).                                                                        |
| `$gt`        | Range scan for values strictly greater than the operand.                                                                                             |
| `$gte`       | Range scan for values greater than or equal to the operand.                                                                                          |
| `$lt`        | Range scan for values strictly less than the operand.                                                                                                |
| `$lte`       | Range scan for values less than or equal to the operand.                                                                                             |
| `$in`        | Transformed into multiple equality scans combined with OR.                                                                                           |
| `$nin`       | Transformed into multiple not-equal scans combined with AND. Not supported on multi-key.                                                             |
| `$elemMatch` | Matches documents where at least one array element satisfies the sub-filter. Uses the index when combined with a multi-key index on the array field. |

Range operators can be combined. For example, `{age: {$gte: 18, $lt: 65}}` produces a bounded range scan on a
single-field index.

The following operators are **not** index-accelerated and always trigger a full scan for that predicate:

| Operator  | Why not indexed                                              |
|-----------|--------------------------------------------------------------|
| `$all`    | Requires checking that all elements are present in an array. |
| `$size`   | Requires counting array elements.                            |
| `$exists` | Requires checking field presence, not field value.           |

## Multi-key indexes

When a field contains an array, set `multi_key: true` to index each array element separately:

```kronotop
BUCKET.INDEX CREATE products '{"tags": {"bson_type": "string", "multi_key": true}}'
```

For an array of objects, use dot notation with `multi_key`:

```kronotop
BUCKET.INDEX CREATE users '{"orders.total": {"bson_type": "int32", "multi_key": true}}'
```

Given a document `{"orders": [{"total": 120}, {"total": 45}]}`, the selector `orders.total` with `multi_key: true`
indexes both `120` and `45` as separate entries. A query `{orders.total: {$gt: 100}}` matches this document because at
least one element satisfies the condition.

### Limitations

- **Undefined ordering.** Documents with multi-key indexes can have multiple index entries. The order in which documents
  are returned cannot be guaranteed.
- **Larger index size.** Each array element creates a separate index entry, so multi-key indexes grow proportionally to
  array sizes.
- **Strict type matching.** Only array elements matching the declared BSON type are indexed. Elements of other types are
  skipped.
- **`$ne` and `$nin` not supported.** On multi-key indexes, these operators fall back to a full scan. The index finds "
  any element not equal to the value," but the correct semantics requires "no element equal to the value." To avoid
  incorrect results, the query engine skips the index entirely.

## Index maintenance on updates

When `BUCKET.UPDATE` modifies a document, every index whose selector overlaps a modified field path is brought in sync
with the new document content. Overlap covers three cases:

- **The field itself.** Setting or unsetting `age` refreshes an index on `age`.
- **A parent path.** Replacing `tags` as a whole refreshes an index on `tags.name`, because the update rewrites
  everything under `tags`.
- **A nested path.** Setting `tags.0` refreshes an index on `tags`, because an element of the indexed array changed.

For example, with an index on `tags.name` and the document `{"tags": [{"name": "java"}, {"name": "kotlin"}]}`, the
update `{"$set": {"tags": [{"name": "go"}]}}` removes the `java` and `kotlin` entries and indexes `go`.

Indexes on unrelated or sibling fields are left untouched. Setting `meta.name` does not modify an index on `meta.color`.

After an update, an index always reflects the current document content. The result is the same as if the updated
document had been freshly inserted.

## The primary index

Every bucket has a built-in primary index on the `_id` field:

- **Name:** `primary-index`
- **Selector:** `_id`
- **Type:** `objectid`
- **Status:** Always `READY`

The primary index is created automatically when the bucket is created. It cannot be dropped.

The primary index is a regular single-field index and supports the same operators as any other single-field index: `$eq`
for point lookups, `$gt`, `$gte`, `$lt`, `$lte` for range scans, and `$in` for multi-value lookups. It can also be used
with `SORTBY _id ASC|DESC` to iterate documents in insertion order.

Like other single-field indexes, the primary index can be analyzed with `BUCKET.INDEX ANALYZE` to collect histogram
statistics for selectivity estimation.

## Query engine behavior

When a query has a filter on a single field, the query engine checks for a matching single-field index. If one exists
and is in `READY` status, the engine uses it.

When both a single-field index and a compound index cover the same field, the engine generally prefers the single-field
index. The exception is when the query uses `SORTBY` and the filter operator is `$eq`. In that case, a compound index on
the filter field and the sort field provides both filtering and sorted output, so the engine prefers it.

For queries with multiple filters on different fields, the query engine can use multiple single-field indexes
independently. Each index produces a set of candidate documents, and the engine intersects the results.

## Constraints

- **Supported BSON types.** `string`, `int32`, `int64`, `double`, `boolean`, `datetime`, `timestamp`, `binary`,
  `objectid`. The `decimal128` type is not yet fully supported for indexing.
- **Unique names.** Index names must be unique across all indexes (single-field, compound, and vector) in the bucket.
- **Strict type matching.** Each index has a declared BSON type. Values that don't match the type are rejected (with
  `strict_types = true`) or silently skipped (with `strict_types = false`). See [Strict Types](strict-types.md).
- **One type per index.** A single-field index indexes values of exactly one BSON type. There is no mixed-type index.

## Practical example

The examples below use RESP3 protocol output. Switch to RESP3 with `HELLO 3` before running the commands.

Create a bucket with a single-field index on `age`:

```kronotop
127.0.0.1:5484> BUCKET.CREATE users INDEXES '{
  "age": {"bson_type": "int32", "name": "idx_age"}
}'
OK
```

Insert some documents:

```kronotop
BUCKET.INSERT users DOCS '{"name": "Alice", "age": 30}'
BUCKET.INSERT users DOCS '{"name": "Bob", "age": 25}'
BUCKET.INSERT users DOCS '{"name": "Charlie", "age": 40}'
BUCKET.INSERT users DOCS '{"name": "Diana", "age": 22}'
```

**Query: equality**, uses the index for a point lookup:

```kronotop
127.0.0.1:5484> BUCKET.QUERY users '{"age": {"$eq": 30}}'
1# "cursor_id" => (integer) 16
2# "entries" => 1) {"_id": "69ce9b496597b10d87d13515", "name": "Alice", "age": 30}
```

**Query: range**, uses the index for a range scan:

```kronotop
127.0.0.1:5484> BUCKET.QUERY users '{"age": {"$gte": 25, "$lt": 40}}'
1# "cursor_id" => (integer) 17
2# "entries" =>
   1) {"_id": "69ce9b4a6597b10d87d13516", "name": "Bob", "age": 25}
   2) {"_id": "69ce9b496597b10d87d13515", "name": "Alice", "age": 30}
```

Returns Bob (25) and Alice (30). Charlie (40) is excluded by `$lt: 40`, Diana (22) by `$gte: 25`.

**Explain output**, confirms the index scan:

```kronotop
127.0.0.1:5484> BUCKET.EXPLAIN users '{"age": {"$gte": 25, "$lt": 40}}'
1# "is_cached" => (true)
2# "plan" =>
    1# "planner_version" => (integer) 1
    2# "nodeType" => "RangeScan"
    3# "id" => (integer) 7
    4# "scanType" => "RANGE_SCAN"
    5# "index" => "selector:age.bsonType:INT32"
    6# "selector" => "age"
    7# "lowerBound" => "Param[ref=ParamRef[index=0]]"
    8# "upperBound" => "Param[ref=ParamRef[index=1]]"
    9# "includeLower" => (true)
   10# "includeUpper" => (false)
```
