---
title: "Bucket Tutorial"
description: "A hands-on walkthrough of the Bucket API, from creating your first bucket to removing it."
---

A hands-on walkthrough of the Bucket API, from creating your first bucket to removing it.

## Before You Start

You need a running Kronotop cluster with at least one initialized shard. Connect with any RESP-compatible
client on port **5484** (the default client port).

All examples in this tutorial use JSON input and output for readability. In production, BSON is recommended
because it avoids conversion overhead and supports richer data types.

## Session Setup

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

## Creating a Bucket

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

Every bucket is created with a **primary index** on the `_id` field. This index is always in `READY` status
and cannot be dropped.

See [BUCKET.CREATE](commands/bucket-create.md) for shard assignment and index creation at bucket creation time.

## Data Modelling and Inserting Documents

### Single Insert

```kronotop
> BUCKET.INSERT users DOCS '{"name": "Alice", "age": 30, "status": "active"}'
1) "6a23da8a87f4f93001bd8df6"
```

Kronotop auto-generates an `_id` (ObjectId) for each document and returns it.

### Batch Insert

Pass multiple documents after the `DOCS` keyword:

```kronotop
> BUCKET.INSERT users DOCS '{"name": "Bob", "age": 25, "status": "active"}' '{"name": "Carol", "age": 35, "status": "inactive"}'
1) "6a23da9887f4f93001bd8df7"
2) "6a23da9887f4f93001bd8df8"
```

### User-Provided `_id`

Supply an ObjectId using extended JSON notation:

```kronotop
> BUCKET.INSERT users DOCS '{"_id": {"$oid": "507f1f77bcf86cd799439011"}, "name": "Dave", "age": 28}'
1) "507f1f77bcf86cd799439011"
```

If the `_id` already exists, the command returns a `DUPLICATEKEY` error. The `_id` field is immutable.

### Schemaless Documents

Documents in the same bucket can have different fields:

```kronotop
> BUCKET.INSERT users DOCS '{"name": "Eve", "email": "eve@example.com"}'
1) "6a23dab187f4f93001bd8df9"
```

### Nested Documents

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

### Supported Data Types

| Type      | JSON Example                                           | Notes                 |
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

## Querying Documents

### Match All

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

Every response contains a `cursor_id` and an `entries` array. Each returned document includes the
server-injected `_id` field.

### Exact Match

A plain field value is an implicit `$eq`:

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

BQL (Bucket Query Language) is used by `BUCKET.QUERY`, `BUCKET.DELETE`, and `BUCKET.UPDATE`. All examples
below query the `users` bucket.

### Comparison Operators

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

Range shorthand: multiple operators on the same field are implicitly ANDed.

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

### Logical Operators

**Implicit AND**, multiple fields in the same object:

```kronotop
> BUCKET.QUERY users '{"status": "active", "age": {"$gte": 18}}'
1# "cursor_id" => (integer) 7
2# "entries" =>
   1) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   2) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
```

**Explicit `$and`:**

```kronotop
> BUCKET.QUERY users '{"$and": [{"status": "active"}, {"age": {"$gte": 18}}]}'
1# "cursor_id" => (integer) 8
2# "entries" =>
   1) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   2) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
```

**`$or`:**

```kronotop
> BUCKET.QUERY users '{"$or": [{"status": "active"}, {"status": "pending"}]}'
1# "cursor_id" => (integer) 9
2# "entries" =>
   1) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   2) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
```

**`$nor`:**

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

**`$not`:**

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

### Array Operators

**`$in`**, match any value in a set:

```kronotop
> BUCKET.QUERY users '{"status": {"$in": ["active", "pending"]}}'
1# "cursor_id" => (integer) 12
2# "entries" =>
   1) {"_id": "6a23da8a87f4f93001bd8df6", "name": "Alice", "age": 30, "status": "active"}
   2) {"_id": "6a23da9887f4f93001bd8df7", "name": "Bob", "age": 25, "status": "active"}
```

**`$nin`**, match none of the values:

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

**`$all`**, array field contains all specified values:

```kronotop
> BUCKET.QUERY users '{"tags": {"$all": ["admin", "verified"]}}'
1# "cursor_id" => (integer) 14
2# "entries" => 1) {"_id": "6a23dac687f4f93001bd8dfb", "name": "Grace", "age": 29, "tags": ["admin", "verified"]}
```

**`$size`**, array has the specified length:

```kronotop
> BUCKET.QUERY users '{"tags": {"$size": 2}}'
1# "cursor_id" => (integer) 15
2# "entries" => 1) {"_id": "6a23dac687f4f93001bd8dfb", "name": "Grace", "age": 29, "tags": ["admin", "verified"]}
```

**`$elemMatch`**, an array element matches a compound condition:

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

### Field Operators

**`$exists`**, field is present or absent:

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

### Nested Field Access

Use dot notation:

```kronotop
> BUCKET.QUERY users '{"address.city": "Istanbul"}'
1# "cursor_id" => (integer) 19
2# "entries" => 1) {"_id": "6a23dabd87f4f93001bd8dfa", "name": "Frank", "age": 40, "address": {"city": "Istanbul", "zip": "34000"}}
```

### Null Values

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

## Sorting and Pagination

### Cursor Model

Every `BUCKET.QUERY`, `BUCKET.DELETE`, and `BUCKET.UPDATE` response includes a `cursor_id`. The cursor
tracks position in the result set so you can page through results with `BUCKET.ADVANCE`.

### Basic Pagination

```kronotop
> BUCKET.QUERY users '{}' LIMIT 2
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

An empty `entries` array signals that all matching documents have been returned.

### Default Batch Size

When `LIMIT` is omitted, the session's `limit` attribute controls the batch size (default: 100). You can
change it with:

```kronotop
> SESSION.ATTRIBUTE SET limit 50
OK
```

### Closing Cursors

Cursors are stored in the session and consume resources while open. Close a cursor with `BUCKET.CLOSE`
when you are done:

```kronotop
> BUCKET.CLOSE QUERY 0
OK
```

### Listing Active Cursors

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

### Sorting with SORTBY

```kronotop
BUCKET.QUERY users '{"age": {"$gt": 25}}' SORTBY age ASC LIMIT 10
```

`SORTBY` requires an index on the sort field, and the query plan must produce results already ordered by that
field. Here the query filters and sorts on the same indexed field, so the index scan provides the ordering.
Results are globally sorted across all `BUCKET.ADVANCE` calls. If the plan cannot provide the ordering, the
query is rejected at planning time. To sort by a different field than the filter, create a compound index
covering both fields, or use `RESULTSORT` for in-memory per-batch sorting.

This example needs the `age` index created in the [Indexes](#indexes) section below. Indexes are built
asynchronously; `SORTBY` is rejected until the index reaches `READY` status. Check the status with
`BUCKET.INDEX DESCRIBE`.

See [SORTBY](sortby.md) for sorting details, [BUCKET.ADVANCE](commands/bucket-advance.md),
[BUCKET.CLOSE](commands/bucket-close.md), and [BUCKET.CURSORS](commands/bucket-cursors.md) for the full reference.

## Updating Documents

### `$set`: Set Field Values

```kronotop
> BUCKET.UPDATE users '{"name": "Alice"}' '{"$set": {"status": "inactive"}}'
1# "cursor_id" => (integer) 22
2# "object_ids" => 1) "6a23da8a87f4f93001bd8df6"
```

The response contains the ObjectIds of all updated documents.

### `$unset`: Remove Fields

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

When `upsert` is `true`, a new document is inserted if no documents match the filter:

```kronotop
> BUCKET.UPDATE users '{"name": "Helen"}' '{"$set": {"name": "Helen", "status": "active"}, "upsert": true}'
1# "cursor_id" => (integer) 25
2# "object_ids" => 1) "6a23dd0287f4f93001bd8dfc"
```

### Array Filters

Use the positional `$[identifier]` syntax with `array_filters` to update only the array elements that match
a condition. The filter is evaluated against each element. Henry's `scores` array is `[75, 85, 95]`; set the
elements greater than or equal to 80 to 100:

```kronotop
> BUCKET.UPDATE users '{"name": "Henry"}' '{"$set": {"scores.$[elem]": 100}, "array_filters": [{"elem": {"$gte": 80}}]}'
1# "cursor_id" => (integer) 26
2# "object_ids" => 1) "6a23e0ac87f4f93001bd8dfd"
```

Henry's `scores` array is now `[75, 100, 100]`.

### Ordered Batch Updates

Combine `SORTBY` and `LIMIT` to update documents in a specific order. `SORTBY` requires the query to produce
results already ordered by the sort field. When the filter field and the sort field are different, create a
compound index that covers both, with the filter field first:

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

Indexes are built asynchronously. Wait until the index reaches `READY` status before running queries that
sort with it; check with `BUCKET.INDEX DESCRIBE`.

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

With an equality filter on `status`, the compound index provides natural ordering on `created_at`:

```kronotop
> BUCKET.UPDATE users '{"status": "pending"}' '{"$set": {"status": "active"}}' SORTBY created_at ASC LIMIT 2
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

## Deleting Documents

### Delete by Filter

```kronotop
> BUCKET.DELETE users '{"status": "inactive"}'
1# "cursor_id" => (integer) 27
2# "object_ids" =>
   1) "6a23da8a87f4f93001bd8df6"
   2) "6a23da9887f4f93001bd8df8"
```

### Batched Deletes

Use `LIMIT` and `BUCKET.ADVANCE` to delete in batches:

```kronotop
> BUCKET.DELETE users '{"status": "inactive"}' LIMIT 100
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

### Delete All Documents

```kronotop
> BUCKET.DELETE users '{}'
1# "cursor_id" => (integer) 28
2# "object_ids" =>
   1) "6a23da8a87f4f93001bd8df6"
   2) "6a23da9887f4f93001bd8df8"
```

Note: `SORTBY` is not supported on `BUCKET.DELETE`.

See [BUCKET.DELETE](commands/bucket-delete.md) for the full reference.

## Indexes

An index is an accelerator. There is no semantic difference between indexed and non-indexed fields. A query
returns the same results regardless of which indexes exist; indexes only affect performance.

### Primary Index

Every bucket has a primary index on `_id`. It is created automatically and cannot be dropped.

### Creating Secondary Indexes

```kronotop
> BUCKET.INDEX CREATE users '{"age": {"bson_type": "int32"}}'
OK
```

Create multiple indexes at once:

```kronotop
> BUCKET.INDEX CREATE users '{"age": {"bson_type": "int32"}, "email": {"bson_type": "string"}}'
OK
```

### Indexes at Bucket Creation Time

You can define indexes when creating a bucket:

```kronotop
> BUCKET.CREATE events INDEXES '{"timestamp": {"bson_type": "datetime"}}'
OK
```

### Multi-Key Indexes

For array fields, set `multi_key` to `true` so each array element gets its own index entry:

```kronotop
> BUCKET.INDEX CREATE users '{"tags": {"bson_type": "string", "multi_key": true}}'
OK
```

### Compound Indexes

A compound index covers multiple fields in a defined order. Use it when queries consistently filter on the same
combination of fields:

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

Query using both fields. The compound index handles the entire filter in a single scan:

```kronotop
BUCKET.QUERY products '{"category": {"$eq": "electronics"}, "price": {"$gt": 100.0}}'
```

The query engine matches filters left to right against the compound index fields. All fields before the last
must use equality; only the last matched field may use a range operator. At least two fields must match for
the compound index to activate. The exception is `SORTBY`: a single equality match is enough when the sort
field is the next field in the index, as in the ordered batch updates example above.

See [Compound Indexes](compound-index.md) for the prefix rule, trade-offs, and constraints.

### Index Lifecycle

Indexes are built asynchronously in the background. After creation, an index progresses through these states:

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

### Listing and Dropping Indexes

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

The primary index (`primary-index`) cannot be dropped.

### Analyzing Statistics

Trigger statistics analysis to help the query optimizer choose better plans:

```kronotop
> BUCKET.INDEX ANALYZE users "selector:age.bsonType:INT32"
OK
```

### Strict Typing

Index types must be compatible with query predicate types. Given an index on `age` with type `int32`:

```
{"age": {"$eq": 25}}       -- uses the index (int32 matches int32)
{"age": {"$eq": "25"}}     -- does NOT use the index (string does not match int32)
```

No implicit type coercion is performed between unrelated types. Numeric types (`INT32`, `INT64`, `DOUBLE`, `DECIMAL128`)
support lossless widening. See [strict-types.md](strict-types.md#numeric-widening) for details.

See [BUCKET.INDEX](commands/bucket-index.md) for the full reference.

## Inspecting Query Plans

Use `BUCKET.EXPLAIN` to see how a query will be executed without running it:

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

### Key Node Types

| Node Type                        | Meaning                                                                     |
|----------------------------------|-----------------------------------------------------------------------------|
| `FullScan`                       | Scans all documents (no usable index).                                      |
| `IndexScan`                      | Point lookup on an index.                                                   |
| `RangeScan`                      | Bounded range scan on an index.                                             |
| `TransformWithResidualPredicate` | Post-scan filter for conditions that could not be pushed into an index.     |
| `Union`                          | Combines results from multiple scans (logical OR).                          |
| `CompoundIndexScan`              | Single scan on a compound index using equality prefixes and optional range. |
| `Intersection`                   | Combines results from multiple scans (logical AND).                         |

### Mixed Indexed and Non-Indexed Filters

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

### Plan Caching

Plans are cached by query shape, the structural fingerprint without literal values. Two queries with the
same operators and field paths but different values share the same cached plan. The cache is invalidated when
indexes are created or dropped.

```kronotop
> BUCKET.QUERY users '{"status": "active"}'
...

> BUCKET.EXPLAIN users '{"status": "active"}'
is_cached -> (boolean) true
plan -> ...
```

### Detecting Full Scans

If `BUCKET.EXPLAIN` shows a `FullScan` node for a query you run frequently, create an index on the
filtered field.

See [BUCKET.EXPLAIN](commands/bucket-explain.md) for the full reference.

## Transactions

By default, every command runs in **auto-commit mode**. Each operation is its own transaction.

For multi-step atomic operations, use explicit transactions:

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

Within an explicit transaction, reads reflect prior writes from the same transaction (read-your-writes).

FoundationDB imposes two constraints on transactions:

| Constraint           | Limit     |
|----------------------|-----------|
| Transaction size     | 10 MB     |
| Transaction duration | 5 seconds |

See [Transactions](../transactions/index.md) for snapshot reads, size inspection, and advanced usage.

## Namespaces

Buckets live inside namespaces. The default namespace is `global`. All bucket commands operate within the
current session namespace.

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

## Bucket Lifecycle

Removing a bucket is a two-phase process.

### Phase 1: Soft Delete

`BUCKET.REMOVE` marks the bucket as logically removed. The bucket becomes inaccessible, and background workers
(index maintenance, replication) are signaled to stop.

```kronotop
> BUCKET.REMOVE users
OK
```

### Phase 2: Hard Delete

`BUCKET.PURGE` permanently deletes all bucket data, indexes, and metadata. Before proceeding, it enforces a
distributed sync barrier that verifies every alive cluster member has observed the removal event.

```kronotop
> BUCKET.PURGE users
OK
```

If the barrier is not yet satisfied, the command returns `BARRIERNOTSATISFIED`. Retry the command; one
retry is usually enough.

```kronotop
> BUCKET.PURGE users
(error) BARRIERNOTSATISFIED Barrier not satisfied: not all shards observed version ...

> BUCKET.PURGE users
OK
```

After purging, the bucket name can be reused.

See [BUCKET.REMOVE](commands/bucket-remove.md) and [BUCKET.PURGE](commands/bucket-purge.md) for the full reference.
