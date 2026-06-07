---
title: "Selectivity Estimation"
description: "When a query has multiple indexes available, the query engine must decide which one to use."
---

When a query has multiple indexes available, the query engine must decide which one to use. The engine predicts
how many documents each candidate index would return and picks the one with the smallest estimate, the most
selective index.

Without statistics, all candidate indexes look equally good. The engine picks one, but it may not be the best.
After you run `BUCKET.INDEX ANALYZE`, the engine uses collected statistics to make an informed choice.

## How It Works

Selectivity estimation is a three-stage process: sampling, analysis, and estimation.

### Sampling

During normal index operations (inserts, updates, deletes), the engine automatically samples a small fraction
of indexed values. Roughly 1 in every 16,000 values is selected and recorded as a sampling hint. No user
action is required.

The sampled hints serve as representative pivot points across the index's value space and guide the
analysis stage.

### Analysis

When you run `BUCKET.INDEX ANALYZE`, a background task reads the collected sampling hints and uses them as
pivot points for distributed sampling across the index. Around each pivot, the engine reads a small
neighborhood of index entries. It also samples from the edges (the smallest and largest values in the index).

The collected samples are then partitioned into a histogram -- an approximate summary of the value distribution.
The histogram divides the value space into at most 10 ranges (buckets) of approximately equal size. Each range
tracks the minimum value, maximum value, and approximate entry count within that range.

The analysis task also records the index's cardinality: the total number of indexed entries.

### Estimation

At query time, when multiple indexes can satisfy a filter, the engine uses the histogram to estimate how many
documents each index would return:

- **Equality filters** (`$eq`): The estimate is based on uniform distribution within the matching histogram
  range.
- **One-sided range filters** (`$gt`, `$gte`, `$lt`, `$lte`): The engine locates the filter value in the
  histogram and estimates the fraction of entries that fall within the range.
- **Bounded range filters** (`$gt` + `$lt` on the same field): The engine estimates the fraction of entries
  between the two bounds.

The index with the lowest estimated result count is selected as the primary scan. Remaining filters become
residual predicates applied after retrieval.

When no statistics are available for an index, the engine assigns it the worst possible estimate, effectively
deprioritizing it in favor of indexes that have been analyzed.

## Running Analysis

Trigger analysis with `BUCKET.INDEX ANALYZE`:

```kronotop
127.0.0.1:5484> BUCKET.INDEX ANALYZE users "selector:age.bsonType:INT32"
OK
```

The index must be in `READY` status. Analysis runs as a background task. Check its progress with
`BUCKET.INDEX TASKS`:

```kronotop
127.0.0.1:5484> BUCKET.INDEX TASKS users "selector:age.bsonType:INT32"
1) 1# "kind" => "ANALYZE"
   2# "status" => "COMPLETED"
```

After analysis completes, `BUCKET.INDEX DESCRIBE` shows the collected cardinality:

```kronotop
127.0.0.1:5484> BUCKET.INDEX DESCRIBE users "selector:age.bsonType:INT32"
1# "index_type" => "single_field"
2# "id" => (integer) 2
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
   1# "cardinality" => (integer) 50000
```

Only one analysis task can run per index at a time. If an analysis task already exists, the command returns an
error.

## What the Optimizer Does with Statistics

The optimizer uses selectivity information in two ways: choosing which index to scan and deciding the order in
which conditions are evaluated.

### Index Selection

When a query's filter matches multiple indexes, the engine estimates the result count for each candidate using
the histogram and picks the most selective one. For example, given indexes on both `status` and `region`:

```kronotop
BUCKET.QUERY orders '{"status": {"$eq": "shipped"}, "region": {"$eq": "eu-west"}}'
```

If the `status` index estimates 5,000 matches and the `region` index estimates 800 matches, the engine uses
the `region` index as the primary scan and applies `status = "shipped"` as a residual filter.

For compound indexes, the engine packs the equality prefix and any trailing range bound into a composite key
and looks it up in the compound index's histogram. This allows compound indexes to compete fairly against
single-field indexes during selection.

### Condition Ordering

Independent of histogram statistics, the optimizer reorders conditions within `$and` and `$or` expressions
to improve short-circuit evaluation:

- **`$and`**: Conditions are ordered from most selective to least selective. If the first condition eliminates
  most candidates, later conditions run against a smaller set.
- **`$or`**: Conditions are ordered from least selective to most selective. A broad condition that matches
  early avoids evaluating narrower conditions unnecessarily.

This ordering uses lightweight heuristics based on operator type and index availability:

| Factor             | More selective                         | Less selective                 |
|--------------------|----------------------------------------|--------------------------------|
| Operator           | `$eq` (point lookup)                   | `$ne`, `$exists` (broad match) |
| Index availability | Indexed field (scan is bounded)        | Unindexed field (full scan)    |
| Scan type          | Compound index scan (narrow key space) | Full scan (entire dataset)     |

## When to Analyze

Analysis is most valuable when:

- **Multiple indexes cover the same query.** Without statistics, the engine cannot distinguish between them.
  Analysis lets it pick the most selective one.
- **Data distribution is skewed.** If 90% of documents have `status = "active"` and 10% have
  `status = "archived"`, the histogram captures this skew and avoids choosing `status` as the primary scan
  for `$eq: "active"` queries.
- **After significant data changes.** A large batch insert or delete can shift the value distribution.
  Re-running analysis updates the histogram to reflect the current state.

Analysis is unnecessary when:

- **Only one index matches the query.** The engine uses it regardless of statistics.
- **The dataset is small.** With few documents, the cost difference between indexes is negligible.

## Observability

Use `BUCKET.EXPLAIN` to see which index the engine selected and whether the plan was influenced by statistics:

```kronotop
127.0.0.1:5484> BUCKET.EXPLAIN orders '{
  "status": {"$eq": "shipped"},
  "region": {"$eq": "eu-west"}
}'
1# "is_cached" => (false)
2# "plan" =>
   1# "planner_version" => (integer) 1
   2# "nodeType" => "TransformWithResidualPredicate"
   3# "id" => (integer) 5
   4# "child" =>
      1# "nodeType" => "IndexScan"
      2# "index" => "selector:region.bsonType:STRING"
      3# "selector" => "region"
      ...
   5# "predicate" =>
      1# "selector" => "status"
      2# "operator" => "EQ"
      ...
```

The plan shows `region` was chosen as the primary index scan and `status` was pushed to a residual predicate.
This indicates the optimizer estimated `region` to be more selective than `status` for this query.

Compare with `BUCKET.INDEX DESCRIBE` to verify that both indexes have been analyzed and have cardinality data.

## Plan Cache Interaction

When analysis completes and statistics are updated, the plan cache for the affected bucket is automatically
invalidated. This ensures that subsequent queries are re-planned using the new statistics rather than reusing
a cached plan that was compiled without them.

See [Plan Cache](plan-cache.md) for details on cache invalidation triggers.

## Limitations

- **Approximate, not exact.** The histogram summarizes the value distribution with at most 10 ranges. Estimates
  are good enough for index selection but are not precise row counts.
- **No automatic refresh.** Analysis does not run automatically. After significant data changes, you should
  re-run `BUCKET.INDEX ANALYZE` to update the statistics.
- **Compound indexes supported.** Compound indexes are analyzed using composite keys that preserve field
  ordering. The histogram reflects the combined key distribution, not individual fields.
