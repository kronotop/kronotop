---
title: "Compound Indexes"
description: "A compound index covers multiple fields in a defined order."
---

## Introduction

A compound index covers multiple fields in a defined order. Instead of creating separate indexes on `category` and
`price`, a compound index on `(category, price)` lets the query engine satisfy multi-field predicates with a single
index scan. Use compound indexes when your queries consistently filter on the same combination of fields.

## Creation

Compound indexes are defined with the `$compound` key in the index schema. Each entry specifies an ordered list of
fields:

```json
{
  "$compound": [
    {
      "name": "idx_cat_price",
      "fields": [
        {
          "selector": "category",
          "bson_type": "string"
        },
        {
          "selector": "price",
          "bson_type": "double"
        }
      ]
    }
  ]
}
```

With the command:

```kronotop
BUCKET.INDEX CREATE products '{
  "$compound": [{
    "name": "idx_cat_price",
    "fields": [
      {"selector": "category", "bson_type": "string"},
      {"selector": "price", "bson_type": "double"}
    ]
  }]
}'
```

A three-field compound index:

```kronotop
BUCKET.INDEX CREATE orders '{
  "$compound": [{
    "fields": [
      {"selector": "status", "bson_type": "string"},
      {"selector": "region", "bson_type": "string"},
      {"selector": "created_at", "bson_type": "datetime"}
    ]
  }]
}'
```

If `name` is omitted, a name is auto-generated from the selectors and types.

A compound index with a `multi_key` field for array element indexing:

```kronotop
BUCKET.INDEX CREATE products '{
  "$compound": [{
    "fields": [
      {"selector": "category", "bson_type": "string"},
      {"selector": "tags", "bson_type": "string", "multi_key": true}
    ]
  }]
}'
```

Each element in the `tags` array is indexed as a separate entry. At most one field in a compound index may have
`multi_key` enabled.

Compound indexes support an optional `collation` at the index level for locale-aware string ordering:

```kronotop
BUCKET.INDEX CREATE products '{
  "$compound": [{
    "fields": [
      {"selector": "category", "bson_type": "string"},
      {"selector": "price", "bson_type": "double"}
    ],
    "collation": {"locale": "en"}
  }]
}'
```

Collation requires at least one `string` field in the compound index. If omitted, the bucket-level collation is
inherited for string fields. See [Collation](collation.md) for full details.

Single-field and compound indexes can be defined in the same schema:

```kronotop
BUCKET.INDEX CREATE products '{
  "email": {"bson_type": "string"},
  "$compound": [{
    "name": "idx_cat_price",
    "fields": [
      {"selector": "category", "bson_type": "string"},
      {"selector": "price", "bson_type": "double"}
    ]
  }]
}'
```

See [BUCKET.INDEX CREATE](commands/bucket-index.md#bucketindex-create) for the full command reference.

## The prefix rule

Field order in a compound index matters. The query engine matches filters against index fields strictly left to right.
This is the prefix rule. It is the most important concept for compound indexes.

The rules are:

1. **Left-to-right matching.** The engine walks the index fields in order. If a field has no matching filter, the walk
   stops. Later fields are not considered even if they have filters.
2. **Equality before range.** All fields before the last matched field must use equality (`$eq`). Only the last matched
   field may use a range operator (`$gt`, `$gte`, `$lt`, `$lte`).
3. **Range stops the walk.** Once a range operator is encountered on a field, no further fields are matched. The range
   marks the end of the prefix.
4. **Leading field is sufficient.** A single equality or range filter on the leading (first) field is enough to activate
   the compound index. When a single-field index exists for that field, the query engine generally prefers it, unless
   SORTBY makes the compound index a better choice (see [Natural sort order](#natural-sort-order)).

### Example: compound index on `(a, b, c)`

| Query filters           | Fields matched | Compound index used? | Why                                                             |
|-------------------------|----------------|----------------------|-----------------------------------------------------------------|
| `a = 1, b = 2, c = 3`   | `a, b, c`      | Yes                  | Full prefix, all equality                                       |
| `a = 1, b = 2`          | `a, b`         | Yes                  | Two-field equality prefix                                       |
| `a = 1, b > 5`          | `a, b`         | Yes                  | Equality on `a`, range on `b` (last matched)                    |
| `a = 1, b > 5, b < 10`  | `a, b`         | Yes                  | Equality on `a`, range bounds on `b`                            |
| `a = 1, b = 2, c > 100` | `a, b, c`      | Yes                  | Equality prefix on `a, b`, range on `c` (last matched)          |
| `a = 1, b > 5, c = 3`   | `a, b`         | Yes                  | Range on `b` stops the walk; `c` becomes a residual filter      |
| `a = 1`                 | `a` only       | Yes                  | Single equality on leading field; compound index as fallback    |
| `a = 1, c = 3`          | `a` only       | Yes                  | Compound scan on `a`; `c` becomes a residual filter             |
| `a > 5`                 | `a` only       | Yes                  | Leading prefix range scan on `a`                                |
| `a > 5, b = 2`          | `a` only       | Yes                  | Leading prefix range scan on `a`; `b` becomes a residual filter |
| `b = 2, c = 3`          | none           | No                   | `a` has no filter; walk stops immediately                       |

When the compound index is not used, the query engine falls back to single-field indexes (if available) or a full scan.

## Supported operators

The following operators participate in compound index matching:

| Operator | Role in compound index                    |
|----------|-------------------------------------------|
| `$eq`    | Equality, can appear on any matched field |
| `$gt`    | Range, only on the last matched field     |
| `$gte`   | Range, only on the last matched field     |
| `$lt`    | Range, only on the last matched field     |
| `$lte`   | Range, only on the last matched field     |

Multiple range operators can apply to the same last field. For example, `a = 1, b > 5, b < 10` uses the compound index
with both range bounds on `b`.

Operators like `$ne`, `$in`, and `$nin` do not participate in compound index matching. If a filter uses one of these
operators on a compound index field, that field is not matched and the prefix walk stops.

## Trade-offs vs. single-field indexes

**When compound indexes win:**

- Multi-field equality lookups. A query on `category = "electronics", price = 29.99` is a single scan on a
  `(category, price)` compound index, instead of two separate index scans followed by an intersection.
- Equality + range patterns. A query on `status = "active", created_at > "2025-01-01"` is a single range scan within the
  `status = "active"` partition.

**When single-field indexes are better:**

- Queries that filter on a single field where a single-field index exists. A single-field index has smaller keys and is
  more efficient than scanning a compound index for the same field. When both exist, the query engine generally prefers
  the single-field index, unless a compound index can also satisfy SORTBY (
  see [Natural sort order](#natural-sort-order)).
- Queries where the fields don't match the prefix order. If your queries sometimes filter on `a` alone and sometimes on
  `b` alone, two single-field indexes serve both patterns. A compound index on `(a, b)` only helps queries that start
  with `a`.

**Costs:**

- Larger index keys. Each entry stores values for all fields in the compound index.
- More storage. The combined key size grows with the number of fields.
- Longer build times. The background index build task processes all fields per document.

**Guidance:** Create compound indexes for query patterns you actually have. If you always query `status` and `region`
together, a compound index on `(status, region)` makes sense. Don't create compound indexes speculatively.

## Residual predicates

When a compound index matches only a prefix of the query's filters, the remaining filters become residual predicates.
These are evaluated as post-filters after the index scan produces candidate documents.

Example: with a compound index on `(a, b)` and a query `a = 1, b = 2, c = 3`:

- `a = 1, b = 2` is handled by the compound index scan.
- `c = 3` is a residual predicate evaluated against each candidate document.

Results are always correct. Residual predicates don't change what the query returns. They just aren't index-accelerated
for those fields. If `c` has its own single-field index, the query engine may use it separately.

## Leading prefix scans

A single equality or range filter on the leading (first) field of a compound index is sufficient to activate a compound
index scan, even when no other fields are matched. When a single-field index exists for that field, the query engine
generally prefers it, unless a compound index can also satisfy SORTBY.

### Example: compound index on `(a, b)`

| Query filters   | Compound index used? | Why                                                    |
|-----------------|----------------------|--------------------------------------------------------|
| `a = 1`         | Yes                  | Equality on leading field scans the prefix             |
| `a = 1, c = 3`  | Yes                  | Equality scan on `a`; `c = 3` applied as residual      |
| `a >= 20`       | Yes                  | Range on leading field scans the index directly        |
| `a > 5, a < 30` | Yes                  | Bounded range on leading field                         |
| `a > 5, b = 2`  | Yes                  | Range scan on `a`; `b = 2` applied as residual filter  |
| `a > 3, b > 10` | Yes                  | Range scan on `a`; `b > 10` applied as residual filter |

When the leading field has a filter and subsequent fields also have filters, those subsequent filters become residual
predicates, evaluated against each candidate document after the index scan.

## Natural sort order

A compound index can provide natural sort order for SORTBY, eliminating in-memory sorting. The query engine determines
this as follows:

1. Fields with equality (`$eq`) filters form a prefix of constant values. These are trivially sorted.
2. The first field after the equality prefix is naturally sorted by the underlying tuple ordering.
3. SORTBY is satisfied if the sort field is either in the equality prefix or is the first field after it.

### Example: compound index on `(a, b, c)`

| Query filters   | SORTBY field | Index provides sort? | Why                                        |
|-----------------|--------------|----------------------|--------------------------------------------|
| `a = 1, b = 2`  | `c`          | Yes                  | EQ prefix on a, b. First non-EQ field is c |
| `a = 1, b >= 5` | `b`          | Yes                  | EQ prefix on a. First non-EQ field is b    |
| `a = 1, b >= 5` | `c`          | No                   | First non-EQ field is b, not c             |
| `a = 1`         | `b`          | Yes                  | EQ prefix on a. First non-EQ field is b    |
| `a = 1`         | `c`          | No                   | First non-EQ field is b, not c             |

When a compound index can satisfy SORTBY, the query engine may prefer it over a single-field index to avoid in-memory
sorting. Both ASC and DESC sort directions are supported.

## Constraints

- **Minimum two fields.** A compound index must have at least two fields. A single-field compound index is just a
  single-field index. Use the regular index syntax instead.
- **Maximum 32 fields.** A compound index supports at most 32 fields.
- **At most one multi-key field.** A compound index allows at most one field with `multi_key` enabled. Multiple
  multi-key fields in the same compound index are rejected at creation time.
- **No duplicate selectors.** Each field selector must appear exactly once within a compound index. Duplicate selectors
  are rejected.
- **Strict type matching.** Each field in a compound index has a declared BSON type. The same strict type matching rules
  apply as with single-field indexes. See [Strict Types](strict-types.md).
- **Unique names.** Index names must be unique across all indexes (single-field and compound) in the bucket.

When `BUCKET.UPDATE` modifies a document, a compound index is refreshed as a whole if any of its fields overlaps a
modified field path. The path overlap rules are the same as for single-field indexes.
See [Index maintenance on updates](single-field-index.md#index-maintenance-on-updates).

## Practical example

Create a bucket and a compound index on `(category, price)`:

```kronotop
BUCKET.CREATE products
BUCKET.INDEX CREATE products '{
  "$compound": [{
    "name": "idx_cat_price",
    "fields": [
      {"selector": "category", "bson_type": "string"},
      {"selector": "price", "bson_type": "double"}
    ]
  }]
}'
```

Insert some documents:

```kronotop
BUCKET.INSERT products DOCS '{"category": "electronics", "price": 299.99, "name": "Headphones"}'
BUCKET.INSERT products DOCS '{"category": "electronics", "price": 49.99, "name": "USB Cable"}'
BUCKET.INSERT products DOCS '{"category": "books", "price": 19.99, "name": "Design Patterns"}'
BUCKET.INSERT products DOCS '{"category": "electronics", "price": 999.99, "name": "Laptop"}'
```

**Query: equality on both fields**, uses the full compound index:

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{
  "category": {"$eq": "electronics"},
  "price": {"$eq": 299.99}
}'
1# "cursor_id" => (integer) 12
2# "entries" => 1) {"_id": "69ce887e6597b10d87d13511", "category": "electronics", "price": 299.99, "name": "Headphones"}
```

**Query: equality + range**, uses the compound index with equality on `category` and range on `price`:

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{
  "category": {"$eq": "electronics"},
  "price": {"$gt": 100.0}
}'
1# "cursor_id" => (integer) 13
2# "entries" =>
   1) {"_id": "69ce887e6597b10d87d13511", "category": "electronics", "price": 299.99, "name": "Headphones"}
   2) {"_id": "69ce88886597b10d87d13514", "category": "electronics", "price": 999.99, "name": "Laptop"}
```

Returns the Headphones and Laptop documents.

**Query: filter on non-leading field only**, compound index not used:

```kronotop
127.0.0.1:5484> BUCKET.QUERY products '{"price": {"$gt": 20.0}}'
1# "cursor_id" => (integer) 14
2# "entries" =>
   1) {"_id": "69ce887e6597b10d87d13511", "category": "electronics", "price": 299.99, "name": "Headphones"}
   2) {"_id": "69ce88816597b10d87d13512", "category": "electronics", "price": 49.99, "name": "USB Cable"}
   3) {"_id": "69ce88886597b10d87d13514", "category": "electronics", "price": 999.99, "name": "Laptop"}
```

`price` is the second field in the compound index `(category, price)`. The prefix rule requires a predicate on
`category` first. Without it, the prefix walk stops immediately and the compound index cannot be used. This falls back
to a single-field index on `price` (if one exists) or a full scan.

```kronotop
127.0.0.1:5484> BUCKET.EXPLAIN products '{"price": {"$gt": 20.0}}'
1# "is_cached" => (true)
2# "plan" =>
   1# "planner_version" => (integer) 1
   2# "nodeType" => "FullScan"
   3# "id" => (integer) 1
   4# "scanType" => "FULL_SCAN"
   5# "index" => "primary-index"
   6# "predicate" =>
      1# "type" => "PREDICATE"
      2# "selector" => "price"
      3# "operator" => "GT"
      4# "operand" => "Param[ref=ParamRef[index=0]]"
```
