---
title: "Bucket"
sidebar:
  label: "Overview"
description: "A bucket is a named collection of BSON documents that lives inside a namespace."
---

## Overview

A bucket is a named collection of BSON documents that lives inside a [namespace](../namespaces/index.md). Buckets
are the primary unit of the document model in Kronotop. You insert documents into a bucket, query them with BQL
(Bucket Query Language), and manage indexes on their fields.

Buckets are distributed across one or more shards. Each shard is owned by a primary member and optionally replicated to
standby members. The number of shards is configured at the cluster level and assigned at bucket creation time.

## Documents

Documents are stored internally as BSON. Clients can send documents as JSON. Kronotop converts them to BSON on
ingestion. The session's document format setting controls whether query results are returned as JSON or BSON.

Every document has an `_id` field of type ObjectId. If the client omits `_id` on insert, Kronotop generates one
automatically. If the client provides an `_id`, it must be a valid ObjectId. The `_id` field is immutable. It cannot
be changed after insertion.

```kronotop
> BUCKET.INSERT users DOCS '{"name": "Alice", "age": 30}'
1) "6835a1c0e4b0f72a3c000001"

> BUCKET.INSERT users DOCS '{"name": "Bob", "age": 25}' '{"name": "Carol", "age": 42}'
1) "6835a1c0e4b0f72a3c000002"
2) "6835a1c0e4b0f72a3c000003"
```

The returned value is a list of ObjectIds assigned to the inserted documents.

## Query Language (BQL)

BQL is the filter language used by `BUCKET.QUERY`, `BUCKET.DELETE`, and `BUCKET.UPDATE`. Filters are JSON objects
containing field names and operator expressions.

### Comparison Operators

| Operator | Description              |
|----------|--------------------------|
| `$eq`    | Equal to                 |
| `$ne`    | Not equal to             |
| `$gt`    | Greater than             |
| `$gte`   | Greater than or equal to |
| `$lt`    | Less than                |
| `$lte`   | Less than or equal to    |

### Logical Operators

| Operator | Description                       |
|----------|-----------------------------------|
| `$and`   | All conditions must match         |
| `$or`    | At least one condition must match |
| `$nor`   | None of the conditions must match |
| `$not`   | Negates a condition               |

### Array Operators

| Operator     | Description                               |
|--------------|-------------------------------------------|
| `$in`        | Field matches any value in the array      |
| `$nin`       | Field matches none of the values          |
| `$all`       | Array field contains all specified values |
| `$elemMatch` | Array element matches a sub-query         |
| `$size`      | Array has the specified length            |

### Other Operators

| Operator  | Description                                  |
|-----------|----------------------------------------------|
| `$exists` | Field exists or is absent                    |
| `$regex`  | Match a string field against a regex pattern |

### Sorting and Pagination

`BUCKET.QUERY` and `BUCKET.UPDATE` accept `SORTBY <field> <ASC|DESC>` to control result ordering and `LIMIT <n>` to
cap the number of documents processed per batch. Results are paginated through cursors. Call `BUCKET.ADVANCE` to
fetch the next batch, and `BUCKET.CLOSE` to release the cursor when done.

```kronotop
> BUCKET.QUERY users '{"age": {"$gte": 18}}' SORTBY name ASC LIMIT 10
1# "cursor_id" => (integer) 1
2# "entries" =>
   1) {"_id": "6835a1c0e4b0f72a3c000001", "name": "Alice", "age": 30}
   2) {"_id": "6835a1c0e4b0f72a3c000002", "name": "Bob", "age": 25}

> BUCKET.ADVANCE QUERY 1
1# "cursor_id" => (integer) 1
2# "entries" => 1) {"_id": "6835a1c0e4b0f72a3c000003", "name": "Carol", "age": 42}

> BUCKET.CLOSE QUERY 1
OK
```

## Query Processing

Queries pass through a multi-stage pipeline before execution:

1. **Parser** - Parses the BQL filter string into an abstract syntax tree (`BqlParser`).
2. **Logical Planner** - Converts the AST into a logical plan and applies optimization transforms: flattening
   nested AND/OR nodes, eliminating double negation, detecting contradictions, removing tautologies, folding constants,
   and pruning redundant conditions.
3. **Physical Planner** - Converts the logical plan into a physical plan by selecting scan strategies (index scan, range
   scan, or full scan) based on available indexes.
4. **Optimizer** - Applies rule-based optimization passes: eliminating redundant scans, consolidating adjacent range
   scans, introducing index intersections, and ordering scans by selectivity.
5. **Executor** - Executes the physical plan against indexes and the volume layer, applying any residual predicates that
   could not be pushed into index scans.

### Plan Caching

Query plans are cached by query shape, the structural fingerprint of a query without its parameter values. Two queries
with the same operators, field paths, and value types but different literal values share the same cached plan. The cache
holds up to 200 plans per bucket and is invalidated when indexes are created or dropped.

Use `BUCKET.EXPLAIN` to inspect the plan for a query and see whether it was served from the cache.

## Indexes

### Primary Index

Every bucket has a primary index on the `_id` field, created automatically at bucket creation time. The primary index
maps ObjectId values to document locations. It is always in `READY` status and cannot be dropped.

### Secondary Indexes

Secondary indexes are user-defined indexes on document fields. Create them with `BUCKET.INDEX CREATE`, providing a JSON
schema that maps field selectors to their BSON type and optional flags.

```kronotop
> BUCKET.INDEX CREATE users '{"age": {"bson_type": "int32"}}'
OK
```

A query returns the same results regardless of which indexes exist. Indexes only affect performance.

### Multi-Key Indexes

When a field contains an array, a multi-key index creates a separate entry for each element of the array, making
queries on array contents efficient. Create a multi-key index by setting `multi_key` to `true` in the field schema:

```kronotop
> BUCKET.INDEX CREATE users '{"tags": {"bson_type": "string", "multi_key": true}}'
OK
```

### Compound Indexes

A compound index covers multiple fields in a defined order. Instead of maintaining separate single-field indexes, a
compound index lets the query engine satisfy multi-field predicates with a single index scan. Fields are declared
inside the `$compound` key of the index schema:

```kronotop
> BUCKET.INDEX CREATE <bucket> '{
  "$compound": [{
    "name": "<index-name>",
    "fields": [
      {"selector": "<field1>", "bson_type": "<type>"},
      {"selector": "<field2>", "bson_type": "<type>"}
    ]
  }]
}'
OK
```

See [Compound Indexes](compound-index.md) for the prefix rule, range scan behavior, and full constraints.

### Vector Indexes

A vector index supports similarity search on fields that contain fixed-dimension numeric vectors. It uses
[JVector](https://github.com/datastax/jvector) and supports three distance functions: `cosine`, `euclidean`, and
`dot_product`. Create a vector index with the `$vector` schema:

```kronotop
> BUCKET.INDEX CREATE products '{
  "$vector": {"field": "embedding", "dimensions": 3, "distance": "cosine"}
}'
OK
```

| Parameter    | Required | Description                                                   |
|--------------|----------|---------------------------------------------------------------|
| `field`      | Yes      | Document field containing the vector. Supports dot notation.  |
| `dimensions` | Yes      | Number of dimensions (must be >= 1).                          |
| `distance`   | Yes      | Similarity function: `cosine`, `euclidean`, or `dot_product`. |
| `name`       | No       | Index name. Auto-generated if omitted.                        |

Vector indexes require single-shard buckets. Use `BUCKET.VECTOR` to search. See
[BUCKET.VECTOR](commands/bucket-vector.md) for query syntax and filtering options.

### Index Lifecycle

Indexes are built asynchronously. After creation, an index progresses through these states:

| Status     | Description                           |
|------------|---------------------------------------|
| `WAITING`  | Queued for building                   |
| `BUILDING` | Background task is populating entries |
| `READY`    | Available for query planning          |
| `DROPPED`  | Marked for deletion and cleanup       |
| `FAILED`   | Build failed                          |

Use `BUCKET.INDEX TASKS` to monitor the progress of index builds.

## Sharding

Buckets span one or more shards. At creation time, shards can be assigned explicitly or selected automatically via
round-robin:

```kronotop
> BUCKET.CREATE users
OK

> BUCKET.CREATE orders SHARDS 0 1
OK
```

When no `SHARDS` clause is given, Kronotop picks a shard using round-robin selection. Use `BUCKET.LOCATE` to see
which shards a bucket spans and the addresses of their primary and standby members:

```kronotop
> BUCKET.LOCATE users
1) (integer) 0                  # shard id
2) "10.0.0.1:5484"             # primary address
3) 1) "10.0.0.2:5484"          # standby addresses
```

## Two-Phase Removal

Deleting a bucket follows the same two-phase pattern as [namespace removal](../namespaces/index.md#two-phase-removal):

1. **`BUCKET.REMOVE`** : Marks the bucket as logically removed. The bucket becomes inaccessible and background workers
   (index maintenance, replication) are signaled to stop.

2. **`BUCKET.PURGE`** : Permanently deletes the bucket data. Before proceeding, the command enforces a distributed sync
   barrier that verifies every alive cluster member has observed the removal event. If the barrier is not satisfied, the
   command returns `BARRIERNOTSATISFIED` and should be retried.

## Commands

| Command                                      | Description                                      |
|----------------------------------------------|--------------------------------------------------|
| [BUCKET.CREATE](commands/bucket-create.md)   | Create a new bucket                              |
| [BUCKET.INSERT](commands/bucket-insert.md)   | Insert documents into a bucket                   |
| [BUCKET.QUERY](commands/bucket-query.md)     | Query documents using a BQL filter               |
| [BUCKET.DELETE](commands/bucket-delete.md)   | Delete documents matching a filter               |
| [BUCKET.UPDATE](commands/bucket-update.md)   | Update documents matching a filter               |
| [BUCKET.EXPLAIN](commands/bucket-explain.md) | Show the execution plan for a query              |
| [BUCKET.INDEX](commands/bucket-index.md)     | Create, list, describe, drop, or analyze indexes |
| [BUCKET.ADVANCE](commands/bucket-advance.md) | Fetch the next batch from a cursor               |
| [BUCKET.CLOSE](commands/bucket-close.md)     | Close a cursor and release its resources         |
| [BUCKET.CURSORS](commands/bucket-cursors.md) | List active cursors for the session              |
| [BUCKET.VECTOR](commands/bucket-vector.md)   | Perform vector similarity search on a bucket     |
| [BUCKET.LOCATE](commands/bucket-locate.md)   | Show shard routing information for a bucket      |
| [BUCKET.LIST](commands/bucket-list.md)       | List all buckets in the current namespace        |
| [BUCKET.REMOVE](commands/bucket-remove.md)   | Mark a bucket for removal (phase 1)              |
| [BUCKET.PURGE](commands/bucket-purge.md)     | Permanently delete a removed bucket (phase 2)    |
