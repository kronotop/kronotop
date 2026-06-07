---
title: "BUCKET.EXPLAIN"
sidebar:
  order: 12
description: "Returns the query execution plan for a given query without executing it."
---

Returns the query execution plan for a given query without executing it.

## Syntax

```kronotop
BUCKET.EXPLAIN <bucket> <query> [SORTBY <field> <ASC|DESC>] [LIMIT <n>] [COLLATION <json-spec>]
```

## Parameters

| Parameter   | Type               | Required | Description                                                                                         |
|-------------|--------------------|----------|-----------------------------------------------------------------------------------------------------|
| `bucket`    | string             | Yes      | Name of the bucket to explain the query against.                                                    |
| `query`     | JSON or BSON       | Yes      | Filter expression to analyze. Use `{}` to match all documents.                                      |
| `SORTBY`    | string + direction | No       | Sort specification. Requires field name followed by `ASC` or `DESC`.                                |
| `LIMIT`     | integer            | No       | Maximum number of documents per batch.                                                              |
| `COLLATION` | JSON               | No       | Query-level collation spec. When provided, the plan reflects how collation affects index selection. |

The parameters are identical to `BUCKET.QUERY`. The query is parsed and planned but never executed.

## Return Value

The command returns a map containing the plan cache status and the execution plan.

**RESP3 (map format):**

```kronotop
1# "is_cached" => (false)
2# "plan" =>
   1# "planner_version" => (integer) 1
   2# "nodeType" => "CompoundIndexScan"
   3# "id" => (integer) 4
   4# "scanType" => "COMPOUND_INDEX_SCAN"
   5# "index" => "idx_cat_price"
   6# "filters" =>
      1) 1# "selector" => "category"
         2# "operator" => "EQ"
         3# "operand" => "Param[ref=ParamRef[index=0]]"
      2) 1# "selector" => "price"
         2# "operator" => "GT"
         3# "operand" => "Param[ref=ParamRef[index=1]]"
```

**RESP2 (array format):**

```kronotop
1) "is_cached"
2) (false)
3) "plan"
4)  1) "planner_version"
    2) (integer) 1
    3) "nodeType"
    4) "CompoundIndexScan"
    5) "id"
    6) (integer) 4
    7) "scanType"
    8) "COMPOUND_INDEX_SCAN"
    9) "index"
   10) "idx_cat_price"
   11) "filters"
   12) 1) 1# "selector" => "category"
          2# "operator" => "EQ"
          3# "operand" => "Param[ref=ParamRef[index=0]]"
       2) 1# "selector" => "price"
          2# "operator" => "GT"
          3# "operand" => "Param[ref=ParamRef[index=1]]"
```

The `query_collation` field reflects the effective collation used during planning. It is omitted when no `COLLATION`
parameter is provided.

## Plan Cache

The `is_cached` field indicates whether the returned plan was retrieved from the plan cache.

- `true`: The plan was previously built and cached by a command that runs the same query shape. Any query-executing
  command populates the cache: `BUCKET.QUERY`, `BUCKET.DELETE`, `BUCKET.UPDATE`, and filtered `BUCKET.VECTOR`.
  `BUCKET.EXPLAIN` itself never writes to the cache.
- `false`: The plan was freshly generated for this `BUCKET.EXPLAIN` call.

A query shape is determined by the structure and operators in the query, not the literal operand values. Two queries
with the same structure but different values share the same shape and therefore the same cached plan.

## Plan Node Types

Every plan node contains the following common fields:

| Field             | Type    | Description                                      |
|-------------------|---------|--------------------------------------------------|
| `planner_version` | integer | Plan format version (currently `1`).             |
| `nodeType`        | string  | The type of this plan node.                      |
| `id`              | integer | Unique identifier for this node within the plan. |

When a node has a downstream processing step, it includes a `next` field containing the next node in the pipeline chain.

### FullScan

Scans all entries in an index. Used when no selective index is available for the query.

| Field       | Type   | Description                                                                                  |
|-------------|--------|----------------------------------------------------------------------------------------------|
| `scanType`  | string | `FULL_SCAN`                                                                                  |
| `index`     | string | Name of the index being scanned (typically `primary-index`).                                 |
| `predicate` | map    | Residual predicate applied during the scan. See [Residual Predicates](#residual-predicates). |

### IndexScan

Scans an index using a single comparison predicate. Used for equality lookups and single-bound inequalities. The
operator determines the matched bound and the scan direction.

| Field      | Type   | Description                                                 |
|------------|--------|-------------------------------------------------------------|
| `scanType` | string | `INDEX_SCAN`                                                |
| `index`    | string | Name of the index being scanned.                            |
| `selector` | string | Field path used for the scan.                               |
| `operator` | string | Comparison operator (e.g., `EQ`, `LT`, `GT`, `LTE`, `GTE`). |
| `operand`  | varies | The value being compared against.                           |

### RangeScan

Scans an index over a bounded range.

| Field          | Type    | Description                              |
|----------------|---------|------------------------------------------|
| `scanType`     | string  | `RANGE_SCAN`                             |
| `index`        | string  | Name of the index being scanned.         |
| `selector`     | string  | Field path used for the range.           |
| `lowerBound`   | varies  | Lower bound value, or null if unbounded. |
| `upperBound`   | varies  | Upper bound value, or null if unbounded. |
| `includeLower` | boolean | Whether the lower bound is inclusive.    |
| `includeUpper` | boolean | Whether the upper bound is inclusive.    |

### Union

Combines results from multiple child scan nodes using set union (logical OR).

| Field       | Type   | Description               |
|-------------|--------|---------------------------|
| `operation` | string | `UNION`                   |
| `children`  | array  | List of child plan nodes. |

### OrderedConcat

Runs child scan nodes one after another in a fixed order, fully consuming one child before moving to the next. Used when
an `$in` condition is combined with `SORTBY` on the same indexed field: each child is an equality scan ordered by value,
so concatenating them yields globally sorted results without scanning the whole index.

| Field       | Type   | Description               |
|-------------|--------|---------------------------|
| `operation` | string | `ORDERED_CONCAT`          |
| `children`  | array  | List of child plan nodes. |

### CompoundIndexScan

Scans a compound index using a combination of equality prefixes and an optional range on the last matched field.

| Field      | Type   | Description                                                                                                                |
|------------|--------|----------------------------------------------------------------------------------------------------------------------------|
| `scanType` | string | `COMPOUND_INDEX_SCAN`                                                                                                      |
| `index`    | string | Name of the compound index being scanned.                                                                                  |
| `filters`  | array  | Ordered list of per-field filters applied during the scan. Each entry is a map with `selector`, `operator`, and `operand`. |

Each entry in `filters` contains:

| Field      | Type   | Description                                           |
|------------|--------|-------------------------------------------------------|
| `selector` | string | Field path for this filter.                           |
| `operator` | string | Comparison operator (`EQ`, `GT`, `GTE`, `LT`, `LTE`). |
| `operand`  | varies | The value being compared against.                     |

### TransformWithResidualPredicate

Applies a post-scan filter to results that could not be fully resolved by index scans. This is the `nodeType` value; the
`operation` field carries `FILTER`.

| Field       | Type   | Description                                                                          |
|-------------|--------|--------------------------------------------------------------------------------------|
| `operation` | string | `FILTER`                                                                             |
| `predicate` | map    | The residual predicate to evaluate. See [Residual Predicates](#residual-predicates). |

## Residual Predicates

Residual predicates represent filter conditions that are evaluated after index scanning. They appear in `FullScan` and
`TransformWithResidualPredicate` nodes.

| Type          | Fields                            | Description                                             |
|---------------|-----------------------------------|---------------------------------------------------------|
| `PREDICATE`   | `selector`, `operator`, `operand` | A single field comparison.                              |
| `AND`         | `children`                        | Logical AND of multiple predicates.                     |
| `OR`          | `children`                        | Logical OR of multiple predicates.                      |
| `ALWAYS_TRUE` | (none)                            | Matches all documents (used for unfiltered full scans). |

## Errors

| Error Code              | Description                     |
|-------------------------|---------------------------------|
| `NOSUCHBUCKET`          | The bucket does not exist.      |
| `BUCKETBEINGREMOVED`    | The bucket is being removed.    |
| `NOSUCHNAMESPACE`       | The namespace does not exist.   |
| `NAMESPACEBEINGREMOVED` | The namespace is being removed. |

## Examples

**Explain a full scan (no filter):**

```kronotop
BUCKET.EXPLAIN users '{}'
```

Response (RESP3):

```kronotop
1# "is_cached" => (false)
2# "plan" =>
   1# "planner_version" => (integer) 1
   2# "nodeType" => "FullScan"
   3# "id" => (integer) 1
   4# "scanType" => "FULL_SCAN"
   5# "index" => "primary-index"
   6# "predicate" =>
      1# "type" => "ALWAYS_TRUE"
```

**Explain an index scan on the primary key:**

```kronotop
BUCKET.EXPLAIN users '{"_id": {"$eq": {"$oid": "6835a1c0e4b0f72a3c000001"}}}'
```

Response (RESP3):

```kronotop
1# "is_cached" => (false)
2# "plan" =>
   1# "planner_version" => (integer) 1
   2# "nodeType" => "IndexScan"
   3# "id" => (integer) 2
   4# "scanType" => "INDEX_SCAN"
   5# "index" => "primary-index"
   6# "selector" => "_id"
   7# "operator" => "EQ"
   8# "operand" => "Param[ref=ParamRef[index=0]]"
```

**Explain a range scan:**

```kronotop
BUCKET.EXPLAIN users '{_id: {$gte: "aaa", $lte: "zzz"}}'
```

Response (RESP3):

```kronotop
1# "is_cached" => (false)
2# "plan" =>
    1# "planner_version" => (integer) 1
    2# "nodeType" => "RangeScan"
    3# "id" => (integer) 7
    4# "scanType" => "RANGE_SCAN"
    5# "index" => "primary-index"
    6# "selector" => "_id"
    7# "lowerBound" => "Param[ref=ParamRef[index=0]]"
    8# "upperBound" => "Param[ref=ParamRef[index=1]]"
    9# "includeLower" => (true)
   10# "includeUpper" => (true)
```

**Explain a query with an indexed field and a non-indexed field:**

```kronotop
BUCKET.EXPLAIN users '{$and: [{age: {$eq: 25}}, {name: {$eq: "Alice"}}]}'
```

When `age` is indexed but `name` is not, the plan uses an index scan on `age` with a residual predicate filter for
`name`:

Response (RESP3):

```kronotop
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

**Explain a compound index scan:**

Given a compound index `idx_cat_price` on `(category: STRING, price: DOUBLE)`:

```kronotop
BUCKET.INDEX CREATE products '{"$compound": [{"name": "idx_cat_price", "fields": [{"selector": "category", "bson_type": "string"}, {"selector": "price", "bson_type": "double"}]}]}'
```

Get the execution plan:

```kronotop
BUCKET.EXPLAIN products '{"category": {"$eq": "electronics"}, "price": {"$gt": 100.0}}'
```

Response (RESP3):

```kronotop
1# "is_cached" => (false)
2# "plan" =>
   1# "planner_version" => (integer) 1
   2# "nodeType" => "CompoundIndexScan"
   3# "id" => (integer) 4
   4# "scanType" => "COMPOUND_INDEX_SCAN"
   5# "index" => "idx_cat_price"
   6# "filters" =>
      1) 1# "selector" => "category"
         2# "operator" => "EQ"
         3# "operand" => "Param[ref=ParamRef[index=0]]"
      2) 1# "selector" => "price"
         2# "operator" => "GT"
         3# "operand" => "Param[ref=ParamRef[index=1]]"
```

**Explain a cached plan:**

After running a query, explaining the same query shape returns the cached plan:

```kronotop
> BUCKET.QUERY users '{status: {$eq: "active"}}'
1# "cursor_id" => (integer) 1
2# "entries" => 1) {"_id": "6a240c7b5da17d872dc0e102", "name": "Bob", "age": 25, "status": "active"}

> BUCKET.EXPLAIN users '{status: {$eq: "active"}}'
1# "is_cached" => (true)
2# "plan" =>
   1# "planner_version" => (integer) 1
   2# "nodeType" => "FullScan"
   3# "id" => (integer) 1
   4# "scanType" => "FULL_SCAN"
   5# "index" => "primary-index"
   6# "predicate" =>
      1# "type" => "PREDICATE"
      2# "selector" => "status"
      3# "operator" => "EQ"
      4# "operand" => "Param[ref=ParamRef[index=0]]"
```
