---
title: "Plan Cache"
description: "The plan cache stores compiled execution plans so that structurally identical queries skip the planning pipeline (parse → logical plan → physical plan → optimize) and reuse a previously compiled plan."
---

The plan cache stores compiled execution plans so that structurally identical queries skip the planning pipeline
(parse → logical plan → physical plan → optimize) and reuse a previously compiled plan. Queries that differ only
in literal values skip planning entirely.

## How It Works

### Query Shapes

A **query shape** is the structural fingerprint of a query: the operators, field paths, value types, and their
nesting, but **not** the literal values. Two queries have the same shape when they use the same operators on the
same fields with the same value types in the same structure.

**Same shape.** These two queries share one cached plan:

```
{"age": {"$gt": 25}}
{"age": {"$gt": 40}}
```

Both use `$gt` on the field `age` with an integer value.

**Different shapes.** These produce different cache entries:

```
{"age": {"$gt": 25}}
{"age": {"$eq": 25}}
```

The operator changed from `$gt` to `$eq`, so the shape is different.

**Different shape.** Value type matters:

```
{"age": {"$gt": 25}}
{"age": {"$gt": 25.0}}
```

The first uses an integer, the second uses a double. Different value types produce different shapes.

**Same shape with compound filters:**

```
{"$and": [{"age": {"$gt": 25}}, {"status": {"$eq": "active"}}]}
{"$and": [{"age": {"$gt": 40}}, {"status": {"$eq": "inactive"}}]}
```

Same operators, same fields, same value types, same shape.

**Different shape.** Array operator element count and element types matter:

```
{"status": {"$in": ["a", "b"]}}
{"status": {"$in": ["a", "b", "c"]}}
```

Both use `$in` on the field `status`, but the first has two elements and the second has three. The element count
is part of the shape, so these produce different cache entries. The same rule applies to `$nin` and `$all`.

**Order independence.** Field ordering within `$and`/`$or` does not affect the shape:

```
{"$and": [{"age": {"$gt": 25}}, {"status": {"$eq": "active"}}]}
{"$and": [{"status": {"$eq": "active"}}, {"age": {"$gt": 25}}]}
```

These two queries produce the same shape hash because children are sorted before hashing.

### Parameterized Execution

Cached plans are templates with parameter slots, conceptually similar to SQL prepared statements. When a query
is planned for the first time, its literal values are extracted into a parameter list and the compiled plan is
stored in the cache. When a subsequent query with the same shape arrives:

1. The parameter values are extracted from the new query in the same deterministic order.
2. The cached plan is retrieved.
3. Each parameter slot in the plan is bound to the corresponding value from the new query.

This means the full planning pipeline runs only once per shape. Subsequent executions skip straight to parameter
binding and plan execution.

### Parameter Ordering

Parameters are extracted in **canonical order**. AND/OR children are sorted by their shape hash, with
insertion order preserved for siblings that have identical shapes. Physical plan nodes are walked in the same
canonical order. Each node's operand is mapped to a parameter index so that the binding is deterministic
regardless of how the optimizer rearranges the plan internally.

For range scans (e.g., `$gt` + `$lt` on the same field), the lower and upper bounds are tracked as separate
occurrences within the same node binding.

## Cache Key

Each cached plan is keyed by:

| Component  | Description                           |
|------------|---------------------------------------|
| Namespace  | The active namespace                  |
| Bucket ID  | UUID of the bucket                    |
| Shape hash | FNV-1a 64-bit hash of the query shape |

The shape hash incorporates the `SORTBY` field and the collation setting when present. Two queries that differ
only in their `SORTBY` field or collation produce different cache entries because these affect index selection
and comparison behavior.

## Eviction and TTL

| Setting                     | Default | Description                               |
|-----------------------------|---------|-------------------------------------------|
| Max entries per bucket      | 200     | FIFO eviction, oldest entry removed first |
| `bucket.plan_cache.max_ttl` | 300000  | TTL in milliseconds (5 minutes)           |

- Each bucket independently holds up to 200 cached plans. When the limit is exceeded, the oldest entry is
  evicted.
- TTL is checked lazily on each cache lookup. Expired plans are not returned and will be replaced on the next
  cache write for that shape.

## Configuration

The plan cache is controlled by two settings in `reference.conf`:

```hocon
bucket {
  plan_cache {
    enabled: true
    max_ttl: 300000 // milliseconds
  }
}
```

| Key                         | Type    | Default  | Description                        |
|-----------------------------|---------|----------|------------------------------------|
| `bucket.plan_cache.enabled` | boolean | `true`   | Enable or disable the plan cache   |
| `bucket.plan_cache.max_ttl` | int     | `300000` | Time-to-live for cached plans (ms) |

## Automatic Invalidation

The cache is automatically invalidated in response to metadata changes:

| Event                    | Scope                                              |
|--------------------------|----------------------------------------------------|
| Index created or dropped | All plans for the affected bucket                  |
| Index statistics updated | All plans for the affected bucket                  |
| Bucket removed           | All plans for the removed bucket                   |
| Namespace removed        | All plans under the removed namespace              |
| Namespace moved          | All plans under the old namespace path (by prefix) |

## Observability

Use `BUCKET.EXPLAIN` to inspect the execution plan for a query. The response includes an `is_cached` boolean
that indicates whether the plan was served from the cache.

First execution. Plan is compiled and cached:

```kronotop
> BUCKET.EXPLAIN users '{"status": "active"}'
is_cached -> (boolean) false
plan -> planner_version -> (integer) 1
         nodeType -> "IndexScan"
         ...
```

Subsequent execution with the same shape. Plan is served from cache:

```kronotop
> BUCKET.EXPLAIN users '{"status": "inactive"}'
is_cached -> (boolean) true
plan -> planner_version -> (integer) 1
         nodeType -> "IndexScan"
         ...
```

The plan structure is identical in both cases; only the bound parameter values differ.
