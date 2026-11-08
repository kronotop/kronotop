---
title: "Bucket Tutorial"
description: "A hands-on walkthrough of the Bucket API, from creating your first bucket to removing it."
---

A hands-on walkthrough of the Bucket API, from creating your first bucket to removing it.

## Before you start

You need a running Kronotop cluster with at least one initialized shard. Connect with any RESP-compatible
client on port 5484 (the default client port).

All examples in this tutorial use JSON input and output because JSON is easy to read. In production, use BSON. It
avoids conversion overhead and supports more data types.

## Session setup

Before working with documents, configure the session to use JSON:

```kronotop
SESSION.ATTRIBUTE SET input_type json
SESSION.ATTRIBUTE SET reply_type json
SESSION.ATTRIBUTE SET object_id_format hex
```

| Attribute          | Effect                                                                          |
|--------------------|---------------------------------------------------------------------------------|
| `input_type`       | Controls whether documents you send are parsed as `json` or `bson`.             |
| `reply_type`       | Controls whether documents returned by queries are encoded as `json` or `bson`. |
| `object_id_format` | Controls whether ObjectIds are returned as `hex` strings or raw `bytes`.        |

See [Session Attributes](../sessions/index.md) for the full list of session settings.

## Creating a bucket

Create a bucket named `users`:

```kronotop
> BUCKET.CREATE users
OK
```

Verify it exists:

```kronotop
> BUCKET.LIST
1) "users"
```

For idempotent scripts, use `IF-NOT-EXISTS` to avoid errors when the bucket already exists:

```kronotop
> BUCKET.CREATE users IF-NOT-EXISTS
OK
```

Every bucket gets a primary index on the `_id` field when it is created. This index is always in `READY` status,
and you cannot drop it.

See [BUCKET.CREATE](commands/bucket-create.md) for shard assignment and index creation at bucket creation time.

## Data modelling and inserting documents

### Single insert

```kronotop
> BUCKET.INSERT users DOCS '{"name": "Alice", "age": 30, "status": "active"}'
1) "6a23da8a87f4f93001bd8df6"
```

Kronotop generates an `_id` (ObjectId) for each document and returns it.

### Batch insert

Pass multiple documents after the `DOCS` keyword:

```kronotop
> BUCKET.INSERT users DOCS '{"name": "Bob", "age": 25, "status": "active"}' '{"name": "Carol", "age": 35, "status": "inactive"}'
1) "6a23da9887f4f93001bd8df7"
2) "6a23da9887f4f93001bd8df8"
```

### User-provided `_id`

To set the ObjectId yourself, use extended JSON notation:

```kronotop
> BUCKET.INSERT users DOCS '{"_id": {"$oid": "507f1f77bcf86cd799439011"}, "name": "Dave", "age": 28}'
1) "507f1f77bcf86cd799439011"
```

If the `_id` already exists, the command returns a `DUPLICATEKEY` error. You cannot change the `_id` field later.

### Schemaless documents

Documents in the same bucket can have different fields:

```kronotop
> BUCKET.INSERT users DOCS '{"name": "Eve", "email": "eve@example.com"}'
1) "6a23dab187f4f93001bd8df9"
```

### Nested documents

```kronotop
> BUCKET.INSERT users DOCS '{
  "name": "Frank",
  "age": 40,
  "address": {"city": "Istanbul", "zip": "34000"}
}'
1) "6a23dabd87f4f93001bd8dfa"
```

### Arrays

```kronotop
> BUCKET.INSERT users DOCS '{"name": "Grace", "age": 29, "tags": ["admin", "verified"]}'
1) "6a23dac687f4f93001bd8dfb"
```

### Supported data types

| Type      | JSON example                                           | Notes                 |
|-----------|--------------------------------------------------------|-----------------------|
| String    | `"Alice"`                                              | UTF-8                 |
| Int32     | `25`                                                   | 32-bit integer        |
| Int64     | `9223372036854775807`                                  | 64-bit integer        |
| Double    | `19.99`                                                | 64-bit floating point |
| Boolean   | `true`, `false`                                        |                       |
| Null      | `null`                                                 |                       |
| DateTime  | `{"$date": "2026-06-01T10:00:00Z"}`                    | Extended JSON         |
| Timestamp | `{"$timestamp": {"t": 1749031200, "i": 1}}`            | Extended JSON         |
| Binary    | `{"$binary": {"base64": "SGVsbG8=", "subType": "00"}}` | Extended JSON         |
| ObjectId  | `{"$oid": "..."}`                                      | Extended JSON         |
| Array     | `[1, 2, 3]`                                            |                       |
| Document  | `{"key": "value"}`                                     | Nested                |

See [BUCKET.INSERT](commands/bucket-insert.md) for the full reference.

## Querying documents

### Match all

An empty filter returns every document in the bucket:

```kronotop
> BUCKET.QUERY users '{}'
1# "cursor_id" => (integer) 1
2# "entries" =>
   1) {"_id": "507f1f77bcf86cd799439011", "name": "Dave", "age": 28}
   2) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   3) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
   4) {"_id": "6a23da9887f4f93001bd8df8", "name": "Carol", "age": 35, "status": "inactive"}
   5) {"_id": "6a23dab187f4f93001bd8df9", "name": "Eve", "email": "eve@example.com"}
   6) {"_id": "6a23dabd87f4f93001bd8dfa", "name": "Frank", "age": 40, "address": {"city": "Istanbul", "zip": "34000"}}
   7) {"_id": "6a23dac687f4f93001bd8dfb", "name": "Grace", "age": 29, "tags": ["admin", "verified"]}
```

Every response contains a `cursor_id` and an `entries` array. Each returned document includes the `_id` field
that the server adds.

### Exact match

A plain field value works as `$eq`:

```kronotop
> BUCKET.QUERY users '{"name": "Alice"}'
1# "cursor_id" => (integer) 2
2# "entries" => 1) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
```

### Query by `_id`

```kronotop
> BUCKET.QUERY users '{"_id": {"$oid": "6a23da8a87f4f93001bd8df6"}}'
1# "cursor_id" => (integer) 4
2# "entries" => 1) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
```

See [BUCKET.QUERY](commands/bucket-query.md) for the full reference.

## Filtering with BQL

`BUCKET.QUERY`, `BUCKET.DELETE`, and `BUCKET.UPDATE` use BQL (Bucket Query Language). All examples below query
the `users` bucket.

### Comparison operators

| Operator | Description              |
|----------|--------------------------|
| `$eq`    | Equal to                 |
| `$ne`    | Not equal to             |
| `$gt`    | Greater than             |
| `$gte`   | Greater than or equal to |
| `$lt`    | Less than                |
| `$lte`   | Less than or equal to    |

Explicit comparison:

```kronotop
> BUCKET.QUERY users '{"age": {"$gt": 25}}'
1# "cursor_id" => (integer) 5
2# "entries" =>
   1) {"_id": "507f1f77bcf86cd799439011", "name": "Dave", "age": 28}
   2) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   3) {"_id": "6a23da9887f4f93001bd8df8", "name": "Carol", "age": 35, "status": "inactive"}
   4) {"_id": "6a23dabd87f4f93001bd8dfa", "name": "Frank", "age": 40, "address": {"city": "Istanbul", "zip": "34000"}}
   5) {"_id": "6a23dac687f4f93001bd8dfb", "name": "Grace", "age": 29, "tags": ["admin", "verified"]}
```

For a range, put several operators on the same field. They are combined with AND:

```kronotop
> BUCKET.QUERY users '{"age": {"$gte": 18, "$lt": 65}}'
1# "cursor_id" => (integer) 6
2# "entries" =>
   1) {"_id": "507f1f77bcf86cd799439011", "name": "Dave", "age": 28}
   2) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   3) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
   4) {"_id": "6a23da9887f4f93001bd8df8", "name": "Carol", "age": 35, "status": "inactive"}
   5) {"_id": "6a23dabd87f4f93001bd8dfa", "name": "Frank", "age": 40, "address": {"city": "Istanbul", "zip": "34000"}}
   6) {"_id": "6a23dac687f4f93001bd8dfb", "name": "Grace", "age": 29, "tags": ["admin", "verified"]}
```

### Logical operators

Several fields in the same object are combined with AND:

```kronotop
> BUCKET.QUERY users '{"status": "active", "age": {"$gte": 18}}'
1# "cursor_id" => (integer) 7
2# "entries" =>
   1) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   2) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
```

You can also write the AND explicitly with `$and`:

```kronotop
> BUCKET.QUERY users '{"$and": [{"status": "active"}, {"age": {"$gte": 18}}]}'
1# "cursor_id" => (integer) 8
2# "entries" =>
   1) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   2) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
```

`$or` matches documents that meet at least one condition:

```kronotop
> BUCKET.QUERY users '{"$or": [{"status": "active"}, {"status": "pending"}]}'
1# "cursor_id" => (integer) 9
2# "entries" =>
   1) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   2) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
```

`$nor` matches documents that meet none of the conditions:

```kronotop
> BUCKET.QUERY users '{"$nor": [{"status": "inactive"}, {"status": "deleted"}]}'
1# "cursor_id" => (integer) 10
2# "entries" =>
   1) {"_id": "507f1f77bcf86cd799439011", "name": "Dave", "age": 28}
   2) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   3) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
   4) {"_id": "6a23dab187f4f93001bd8df9", "name": "Eve", "email": "eve@example.com"}
   5) {"_id": "6a23dabd87f4f93001bd8dfa", "name": "Frank", "age": 40, "address": {"city": "Istanbul", "zip": "34000"}}
   6) {"_id": "6a23dac687f4f93001bd8dfb", "name": "Grace", "age": 29, "tags": ["admin", "verified"]}
```

`$not` inverts an operator expression:

```kronotop
> BUCKET.QUERY users '{"age": {"$not": {"$gt": 100}}}'
1# "cursor_id" => (integer) 11
2# "entries" =>
   1) {"_id": "507f1f77bcf86cd799439011", "name": "Dave", "age": 28}
   2) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   3) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
   4) {"_id": "6a23da9887f4f93001bd8df8", "name": "Carol", "age": 35, "status": "inactive"}
   5) {"_id": "6a23dab187f4f93001bd8df9", "name": "Eve", "email": "eve@example.com"}
   6) {"_id": "6a23dabd87f4f93001bd8dfa", "name": "Frank", "age": 40, "address": {"city": "Istanbul", "zip": "34000"}}
   7) {"_id": "6a23dac687f4f93001bd8dfb", "name": "Grace", "age": 29, "tags": ["admin", "verified"]}
```

### Array operators

`$in` matches any value in a set:

```kronotop
> BUCKET.QUERY users '{"status": {"$in": ["active", "pending"]}}'
1# "cursor_id" => (integer) 12
2# "entries" =>
   1) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   2) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
```

`$nin` matches none of the values:

```kronotop
> BUCKET.QUERY users '{"status": {"$nin": ["deleted", "archived"]}}'
1# "cursor_id" => (integer) 13
2# "entries" =>
   1) {"_id": "507f1f77bcf86cd799439011", "name": "Dave", "age": 28}
   2) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   3) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
   4) {"_id": "6a23da9887f4f93001bd8df8", "name": "Carol", "age": 35, "status": "inactive"}
   5) {"_id": "6a23dab187f4f93001bd8df9", "name": "Eve", "email": "eve@example.com"}
   6) {"_id": "6a23dabd87f4f93001bd8dfa", "name": "Frank", "age": 40, "address": {"city": "Istanbul", "zip": "34000"}}
   7) {"_id": "6a23dac687f4f93001bd8dfb", "name": "Grace", "age": 29, "tags": ["admin", "verified"]}
```

`$all` matches when the array field contains all the given values:

```kronotop
> BUCKET.QUERY users '{"tags": {"$all": ["admin", "verified"]}}'
1# "cursor_id" => (integer) 14
2# "entries" => 1) {"_id": "6a23dac687f4f93001bd8dfb", "name": "Grace", "age": 29, "tags": ["admin", "verified"]}
```

`$size` matches when the array has the given length:

```kronotop
> BUCKET.QUERY users '{"tags": {"$size": 2}}'
1# "cursor_id" => (integer) 15
2# "entries" => 1) {"_id": "6a23dac687f4f93001bd8dfb", "name": "Grace", "age": 29, "tags": ["admin", "verified"]}
```

`$elemMatch` matches when a single array element meets all the conditions. First, insert a document with an
array:

```kronotop
> BUCKET.INSERT users DOCS '{"name": "Henry", "age": 31, "scores": [75, 85, 95]}'
1) "6a23e0ac87f4f93001bd8dfd"
```

Query it back with `$elemMatch`:

```kronotop
> BUCKET.QUERY users '{"scores": {"$elemMatch": {"$gte": 80, "$lt": 90}}}'
1# "cursor_id" => (integer) 32
2# "entries" => 1) {"_id": "6a23e0ac87f4f93001bd8dfd", "name": "Henry", "age": 31, "scores": [75, 85, 95]}
```

### Field operators

`$exists` checks whether a field is present or absent:

```kronotop
> BUCKET.QUERY users '{"email": {"$exists": true}}'
1# "cursor_id" => (integer) 17
2# "entries" => 1) {"_id": "6a23dab187f4f93001bd8df9", "name": "Eve", "email": "eve@example.com"}
```

```kronotop
> BUCKET.QUERY users '{"deletedAt": {"$exists": false}}'
1# "cursor_id" => (integer) 18
2# "entries" =>
   1) {"_id": "507f1f77bcf86cd799439011", "name": "Dave", "age": 28}
   2) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   3) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
   4) {"_id": "6a23da9887f4f93001bd8df8", "name": "Carol", "age": 35, "status": "inactive"}
   5) {"_id": "6a23dab187f4f93001bd8df9", "name": "Eve", "email": "eve@example.com"}
   6) {"_id": "6a23dabd87f4f93001bd8dfa", "name": "Frank", "age": 40, "address": {"city": "Istanbul", "zip": "34000"}}
   7) {"_id": "6a23dac687f4f93001bd8dfb", "name": "Grace", "age": 29, "tags": ["admin", "verified"]}
```

### Nested field access

Use dot notation:

```kronotop
> BUCKET.QUERY users '{"address.city": "Istanbul"}'
1# "cursor_id" => (integer) 19
2# "entries" => 1) {"_id": "6a23dabd87f4f93001bd8dfa", "name": "Frank", "age": 40, "address": {"city": "Istanbul", "zip": "34000"}}
```

### Null values

```kronotop
> BUCKET.QUERY users '{"middleName": null}'
1# "cursor_id" => (integer) 20
2# "entries" =>
   1) {"_id": "507f1f77bcf86cd799439011", "name": "Dave", "age": 28}
   2) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   3) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
   4) {"_id": "6a23da9887f4f93001bd8df8", "name": "Carol", "age": 35, "status": "inactive"}
   5) {"_id": "6a23dab187f4f93001bd8df9", "name": "Eve", "email": "eve@example.com"}
   6) {"_id": "6a23dabd87f4f93001bd8dfa", "name": "Frank", "age": 40, "address": {"city": "Istanbul", "zip": "34000"}}
   7) {"_id": "6a23dac687f4f93001bd8dfb", "name": "Grace", "age": 29, "tags": ["admin", "verified"]}
```

See [BQL Language Reference](bql-reference.md) for the complete operator reference.

## Sorting and pagination

### Cursor model

Every `BUCKET.QUERY`, `BUCKET.DELETE`, and `BUCKET.UPDATE` response includes a `cursor_id`. The cursor keeps
its position in the result set, so you can page through the results with `BUCKET.ADVANCE`.

### Basic pagination

```kronotop
> BUCKET.QUERY users '{}' BATCH 2
1# "cursor_id" => (integer) 21
2# "entries" =>
   1) {"_id": "507f1f77bcf86cd799439011", "name": "Dave", "age": 28}
   2) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
```

```kronotop
> BUCKET.ADVANCE QUERY 21
1# "cursor_id" => (integer) 21
2# "entries" =>
   1) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
   2) {"_id": "6a23da9887f4f93001bd8df8", "name": "Carol", "age": 35, "status": "inactive"}
```

An empty `entries` array means that all matching documents have been returned.

### Default batch size

If you omit `BATCH`, the session's `batch` attribute sets the batch size (default: 100). To change it:

```kronotop
> SESSION.ATTRIBUTE SET batch 50
OK
```

### Closing cursors

Cursors live in the session and use resources while they are open. Close a cursor with `BUCKET.CLOSE` when you
are done:

```kronotop
> BUCKET.CLOSE QUERY 0
OK
```

### Listing active cursors

```kronotop
> BUCKET.CURSORS
1) QUERY -> 0 -> "{}"
2) UPDATE -> (empty map)
3) DELETE -> (empty map)
```

Filter by operation type:

```kronotop
> BUCKET.CURSORS QUERY
1) QUERY -> 0 -> "{}"
```

### Sorting with `SORTBY`

```kronotop
BUCKET.QUERY users '{"age": {"$gt": 25}}' SORTBY age ASC BATCH 10
```

`SORTBY` needs an index on the sort field, and the query plan must return results that are already in order by
that field. Here the query filters and sorts on the same indexed field, so the index scan gives the order. The
sort order holds across all `BUCKET.ADVANCE` calls. If the plan cannot give that order, the planner rejects the
query. To sort by a field other than the filter field, create a compound index on both fields, or use
`RESULTSORT` to sort each batch in memory.

This example needs the `age` index from the [Indexes](#indexes) section below. Kronotop builds indexes in the
background and rejects `SORTBY` until the index reaches `READY` status. Check the status with
`BUCKET.INDEX DESCRIBE`.

See [SORTBY](sortby.md) for sorting details, [BUCKET.ADVANCE](commands/bucket-advance.md),
[BUCKET.CLOSE](commands/bucket-close.md), and [BUCKET.CURSORS](commands/bucket-cursors.md) for the full reference.

## Updating documents

### `$set`: set field values

```kronotop
> BUCKET.UPDATE users '{"name": "Alice"}' '{"$set": {"status": "inactive"}}'
1# "cursor_id" => (integer) 22
2# "object_ids" => 1) "6a23da8a87f4f93001bd8df6"
```

The response contains the ObjectIds of all updated documents.

### `$unset`: remove fields

```kronotop
> BUCKET.UPDATE users '{"age": {"$gt": 30}}' '{"$unset": ["temporary_field", "deprecated_field"]}'
1# "cursor_id" => (integer) 23
2# "object_ids" =>
   1) "6a23da9887f4f93001bd8df8"
   2) "6a23dabd87f4f93001bd8dfa"
```

### Combining `$set` and `$unset`

```kronotop
> BUCKET.UPDATE users '{}' '{"$set": {"version": 2}, "$unset": ["old_field"]}'
1# "cursor_id" => (integer) 24
2# "object_ids" =>
   1) "507f1f77bcf86cd799439011"
   2) "6a23da8a87f4f93001bd8df6"
   3) "6a23da9887f4f93001bd8df7"
   4) "6a23da9887f4f93001bd8df8"
   5) "6a23dab187f4f93001bd8df9"
   6) "6a23dabd87f4f93001bd8dfa"
   7) "6a23dac687f4f93001bd8dfb"
```

### Upsert

When `upsert` is `true` and no document matches the filter, the command inserts a new document:

```kronotop
> BUCKET.UPDATE users '{"name": "Helen"}' '{"$set": {"name": "Helen", "status": "active"}, "upsert": true}'
1# "cursor_id" => (integer) 25
2# "object_ids" => 1) "6a23dd0287f4f93001bd8dfc"
```

### Array filters

To update only the array elements that match a condition, use the positional `$[identifier]` syntax with
`array_filters`. The filter runs against each element. Henry's `scores` array is `[75, 85, 95]`. This command sets
every element that is greater than or equal to 80 to 100:

```kronotop
> BUCKET.UPDATE users '{"name": "Henry"}' '{"$set": {"scores.$[elem]": 100}, "array_filters": [{"elem": {"$gte": 80}}]}'
1# "cursor_id" => (integer) 26
2# "object_ids" => 1) "6a23e0ac87f4f93001bd8dfd"
```

Henry's `scores` array is now `[75, 100, 100]`.

### Ordered batch updates

Use `SORTBY` with `BATCH` to update documents in a set order. `SORTBY` needs the query to return results that are
already in order by the sort field. When the filter field and the sort field are different, create a compound
index on both, with the filter field first:

```kronotop
> BUCKET.INDEX CREATE users '{
  "$compound": [{
    "name": "idx_status_created_at",
    "fields": [
      {"selector": "status", "bson_type": "string"},
      {"selector": "created_at", "bson_type": "datetime"}
    ]
  }]
}'
OK
```

Kronotop builds indexes in the background. Wait until the index reaches `READY` status before you run queries
that sort with it. Check the status with `BUCKET.INDEX DESCRIBE`.

Insert a few pending documents with `created_at` timestamps:

```kronotop
> BUCKET.INSERT users DOCS '{
  "name": "Ivy",
  "status": "pending",
  "created_at": {"$date": "2026-06-01T10:00:00Z"}
}' '{
  "name": "Jack",
  "status": "pending",
  "created_at": {"$date": "2026-06-02T10:00:00Z"}
}'
1) "6a23e10187f4f93001bd8dfd"
2) "6a23e10187f4f93001bd8dfe"
```

```kronotop
> BUCKET.INSERT users DOCS '{
  "name": "Kate",
  "status": "pending",
  "created_at": {"$date": "2026-06-03T10:00:00Z"}
}' '{
  "name": "Liam",
  "status": "pending",
  "created_at": {"$date": "2026-06-04T10:00:00Z"}
}'
1) "6a23e10987f4f93001bd8dff"
2) "6a23e10987f4f93001bd8e00"
```

With an equality filter on `status`, the compound index returns the documents in `created_at` order:

```kronotop
> BUCKET.UPDATE users '{"status": "pending"}' '{"$set": {"status": "active"}}' SORTBY created_at ASC BATCH 2
1# "cursor_id" => (integer) 29
2# "object_ids" =>
   1) "6a23e10187f4f93001bd8dfd"
   2) "6a23e10187f4f93001bd8dfe"
```

The first batch updates the two oldest pending documents. Advance the cursor to update the next batch:

```kronotop
> BUCKET.ADVANCE UPDATE 29
1# "cursor_id" => (integer) 29
2# "object_ids" =>
   1) "6a23e10987f4f93001bd8dff"
   2) "6a23e10987f4f93001bd8e00"
```

See [BUCKET.UPDATE](commands/bucket-update.md) for the full reference.

## Deleting documents

### Delete by filter

```kronotop
> BUCKET.DELETE users '{"status": "inactive"}'
1# "cursor_id" => (integer) 27
2# "object_ids" =>
   1) "6a23da8a87f4f93001bd8df6"
   2) "6a23da9887f4f93001bd8df8"
```

### Batched deletes

Use `BATCH` and `BUCKET.ADVANCE` to delete in batches:

```kronotop
> BUCKET.DELETE users '{"status": "inactive"}' BATCH 100
1# "cursor_id" => (integer) 28
2# "object_ids" =>
   1) "6a23da8a87f4f93001bd8df6"
   2) "6a23da9887f4f93001bd8df8"
```

Advance the cursor:

```kronotop
> BUCKET.ADVANCE DELETE 0
cursor_id -> (integer) 0
object_ids -> [...] (next 100 deleted)
```

### Delete all documents

```kronotop
> BUCKET.DELETE users '{}'
1# "cursor_id" => (integer) 28
2# "object_ids" =>
   1) "6a23da8a87f4f93001bd8df6"
   2) "6a23da9887f4f93001bd8df8"
```

`BUCKET.DELETE` does not support `SORTBY`.

See [BUCKET.DELETE](commands/bucket-delete.md) for the full reference.

## Indexes

An index only makes queries faster. Indexed and non-indexed fields behave the same way, and a query returns the
same results whatever indexes exist.

### Primary index

Every bucket has a primary index on `_id`. Kronotop creates it automatically, and you cannot drop it.

### Creating secondary indexes

```kronotop
> BUCKET.INDEX CREATE users '{"age": {"bson_type": "int32"}}'
OK
```

Create multiple indexes at once:

```kronotop
> BUCKET.INDEX CREATE users '{"age": {"bson_type": "int32"}, "email": {"bson_type": "string"}}'
OK
```

### Indexes at bucket creation time

You can define indexes when creating a bucket:

```kronotop
> BUCKET.CREATE events INDEXES '{"timestamp": {"bson_type": "datetime"}}'
OK
```

### Multi-key indexes

For array fields, set `multi_key` to `true` so each array element gets its own index entry:

```kronotop
> BUCKET.INDEX CREATE users '{"tags": {"bson_type": "string", "multi_key": true}}'
OK
```

### Compound indexes

A compound index covers several fields in a fixed order. Use one when your queries often filter on the same set
of fields:

```kronotop
> BUCKET.INDEX CREATE products '{
  "$compound": [{
    "name": "idx_cat_price",
    "fields": [
      {"selector": "category", "bson_type": "string"},
      {"selector": "price", "bson_type": "double"}
    ]
  }]
}'
OK
```

Query on both fields. The compound index covers the whole filter in one scan:

```kronotop
BUCKET.QUERY products '{"category": {"$eq": "electronics"}, "price": {"$gt": 100.0}}'
```

The query engine matches filters against the compound index fields from left to right. Every field before the
last one must use equality, and only the last matched field may use a range operator. At least two fields must
match before the engine uses the compound index. `SORTBY` is the exception. One equality match is enough when the
sort field is the next field in the index, as in the ordered batch updates example above.

See [Compound Indexes](compound-index.md) for the prefix rule, trade-offs, and constraints.

### Index lifecycle

Kronotop builds indexes in the background. After you create an index, it moves through these states:

| Status     | Description                            |
|------------|----------------------------------------|
| `WAITING`  | Queued for building.                   |
| `BUILDING` | Background task is populating entries. |
| `READY`    | Available for query planning.          |
| `DROPPED`  | Marked for deletion and cleanup.       |
| `FAILED`   | Build failed.                          |

Monitor progress with `BUCKET.INDEX TASKS`:

```kronotop
BUCKET.INDEX TASKS users "selector:age.bsonType:INT32"
```

Check index details with `BUCKET.INDEX DESCRIBE`:

```kronotop
> BUCKET.INDEX DESCRIBE users "selector:age.bsonType:INT32"
1# "index_type" => "single_field"
2# "id" => (integer) 3048755950204840837
3# "selector" => "age"
4# "bson_type" => "INT32"
5# "status" => "READY"
6# "collation" =>
   1# "locale" => (nil)
   2# "strength" => (nil)
   3# "case_level" => (nil)
   4# "case_first" => (nil)
   5# "numeric_ordering" => (nil)
   6# "alternate" => (nil)
   7# "backwards" => (nil)
   8# "normalization" => (nil)
   9# "max_variable" => (nil)
7# "statistics" =>
   1# "cardinality" => (integer) 6
```

### Listing and dropping indexes

```kronotop
> BUCKET.INDEX LIST users
1) "primary-index"
2) "selector:age.bsonType:INT32"
3) "selector:email.bsonType:STRING"
4) "selector:tags.bsonType:STRING"
```

```kronotop
> BUCKET.INDEX DROP users "selector:email.bsonType:STRING"
OK
```

You cannot drop the primary index (`primary-index`).

### Analyzing statistics

Run a statistics analysis so the query optimizer can choose better plans:

```kronotop
> BUCKET.INDEX ANALYZE users "selector:age.bsonType:INT32"
OK
```

### Strict typing

The index type must be compatible with the type in the query predicate. For an index on `age` with type `int32`:

```
{"age": {"$eq": 25}}       -- uses the index (int32 matches int32)
{"age": {"$eq": "25"}}     -- does NOT use the index (string does not match int32)
```

Kronotop does not convert values between unrelated types. Numeric types (`INT32`, `INT64`, `DOUBLE`,
`DECIMAL128`) support lossless widening. See [strict-types.md](strict-types.md#numeric-widening) for details.

See [BUCKET.INDEX](commands/bucket-index.md) for the full reference.

## Inspecting query plans

Use `BUCKET.EXPLAIN` to see the plan for a query without running it:

```kronotop
> BUCKET.EXPLAIN users '{"age": {"$gt": 25}}'
1# "is_cached" => (false)
2# "plan" =>
   1# "planner_version" => (integer) 1
   2# "nodeType" => "IndexScan"
   3# "id" => (integer) 2
   4# "scanType" => "INDEX_SCAN"
   5# "index" => "selector:age.bsonType:INT32"
   6# "selector" => "age"
   7# "operator" => "GT"
   8# "operand" => "Param[ref=ParamRef[index=0]]"
```

### Common node types

| Node type                        | Meaning                                                                     |
|----------------------------------|-----------------------------------------------------------------------------|
| `FullScan`                       | Scans all documents (no usable index).                                      |
| `IndexScan`                      | Point lookup on an index.                                                   |
| `RangeScan`                      | Bounded range scan on an index.                                             |
| `TransformWithResidualPredicate` | Post-scan filter for conditions that could not be pushed into an index.     |
| `Union`                          | Combines results from multiple scans (logical OR).                          |
| `CompoundIndexScan`              | Single scan on a compound index using equality prefixes and optional range. |
| `Intersection`                   | Combines results from multiple scans (logical AND).                         |

### Mixed indexed and non-indexed filters

When `age` is indexed but `name` is not:

```kronotop
> BUCKET.EXPLAIN users '{"$and": [{"age": {"$eq": 25}}, {"name": {"$eq": "Alice"}}]}'
1# "is_cached" => (false)
2# "plan" =>
   1# "planner_version" => (integer) 1
   2# "nodeType" => "IndexScan"
   3# "id" => (integer) 23
   4# "scanType" => "INDEX_SCAN"
   5# "index" => "selector:age.bsonType:INT32"
   6# "selector" => "age"
   7# "operator" => "EQ"
   8# "operand" => "Param[ref=ParamRef[index=1]]"
   9# "next" =>
      1# "planner_version" => (integer) 1
      2# "nodeType" => "TransformWithResidualPredicate"
      3# "id" => (integer) 26
      4# "operation" => "FILTER"
      5# "predicate" =>
         1# "type" => "AND"
         2# "children" =>
            1) 1# "type" => "PREDICATE"
               2# "selector" => "name"
               3# "operator" => "EQ"
               4# "operand" => "Param[ref=ParamRef[index=0]]"
```

The plan uses an `IndexScan` on `age` followed by a `TransformWithResidualPredicate` that filters on `name`.

### Plan caching

Kronotop caches plans by query shape, which is the structure of the query without its literal values. Two queries
with the same operators and field paths but different values use the same cached plan. Creating or dropping an
index clears the cache.

```kronotop
> BUCKET.QUERY users '{"status": "active"}'
...

> BUCKET.EXPLAIN users '{"status": "active"}'
is_cached -> (boolean) true
plan -> ...
```

### Detecting full scans

If `BUCKET.EXPLAIN` shows a `FullScan` node for a query you run frequently, create an index on the
filtered field.

See [BUCKET.EXPLAIN](commands/bucket-explain.md) for the full reference.

## Transactions

By default, every command runs in auto-commit mode, so each operation is its own transaction.

To run several steps atomically, use an explicit transaction:

```kronotop
> BEGIN
OK

> BUCKET.INSERT users DOCS '{"name": "Ivy", "age": 22}'
1) "6835a1c0e4b0f72a3c000007"

> BUCKET.UPDATE users '{"name": "Alice"}' '{"$set": {"status": "inactive"}}'
1# "cursor_id" => (integer) 30
2# "object_ids" => 1) "6835a1c0e4b0f72a3c000001"

> COMMIT
OK
```

To cancel, use `ROLLBACK` instead of `COMMIT`.

Inside an explicit transaction, reads see the earlier writes of the same transaction (read-your-writes).

FoundationDB sets two limits on transactions:

| Constraint           | Limit     |
|----------------------|-----------|
| Transaction size     | 10 MB     |
| Transaction duration | 5 seconds |

See [Transactions](../transactions/index.md) for snapshot reads, size inspection, and advanced usage.

## Namespaces

Buckets live inside namespaces. The default namespace is `global`. All bucket commands work in the current
session namespace.

Switch to a different namespace:

```kronotop
> NAMESPACE USE production
OK
```

Check the current namespace:

```kronotop
> NAMESPACE CURRENT
"production"
```

Any bucket created or queried after `NAMESPACE USE` belongs to that namespace:

```kronotop
> BUCKET.CREATE orders
OK

> BUCKET.LIST
1) "orders"
```

See [Namespaces](../namespaces/index.md) for hierarchical namespaces, creation, and removal.

## Bucket lifecycle

Removing a bucket takes two phases.

### Phase 1: soft delete

`BUCKET.REMOVE` marks the bucket as logically removed. You can no longer access the bucket, and Kronotop tells the
background workers (index maintenance, replication) to stop.

```kronotop
> BUCKET.REMOVE users
OK
```

### Phase 2: hard delete

`BUCKET.PURGE` permanently deletes all bucket data, indexes, and metadata. Before it deletes anything, it checks a
distributed sync barrier to make sure that every live cluster member has seen the removal.

```kronotop
> BUCKET.PURGE users
OK
```

If the barrier is not satisfied yet, the command returns `BARRIERNOTSATISFIED`. Run the command again. One
retry is usually enough.

```kronotop
> BUCKET.PURGE users
(error) BARRIERNOTSATISFIED Barrier not satisfied: not all shards observed version ...

> BUCKET.PURGE users
OK
```

After the purge, you can reuse the bucket name.

See [BUCKET.REMOVE](commands/bucket-remove.md) and [BUCKET.PURGE](commands/bucket-purge.md) for the full reference.
