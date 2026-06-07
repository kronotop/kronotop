---
title: "Type Bracketing"
description: "Type bracketing defines a deterministic total ordering across all BSON types."
---

Type bracketing defines a deterministic total ordering across all BSON types. When a `RESULTSORT` operation encounters
mixed types in the sort field, this ordering ensures the result set has a well-defined, consistent sort order.

## Type Order

Values of different types are ordered by their type bracket, from lowest to highest:

| Rank | Type                             |
|------|----------------------------------|
| 0    | Null                             |
| 1    | Int32, Int64, Double, Decimal128 |
| 2    | String                           |
| 3    | Document                         |
| 4    | Array                            |
| 5    | Binary                           |
| 6    | ObjectId                         |
| 7    | Boolean                          |
| 8    | DateTime                         |
| 9    | Timestamp                        |

All four numeric types share a single bracket (rank 1). Within this bracket, values of different numeric types are
compared
numerically via lossless widening (see below).

A field with a `Null` value always sorts before any numeric value, a numeric value always sorts before a `String`, and
so on regardless of the actual values.

## Same-Bracket Comparison

When two values share the same type bracket, they are compared by their actual values:

- **Numeric bracket (Int32, Int64, Double, Decimal128)**: All four types share a single bracket. When two values have
  different numeric types, both are promoted to a common lossless type and compared numerically. For example,
  `Int32(100)` and `Int64(50)` are compared as `Int64` values, so `100 > 50`. The common type is determined by lossless
  widening rules (see [strict-types.md](strict-types.md#numeric-widening)).
- **String**: Lexicographic comparison.
- **Document**: Field-by-field in iteration order. Keys are compared first, then values. A shorter document wins if all
  compared fields are equal.
- **Array**: Element-by-element ordering. A shorter array wins if all compared elements are equal.
- **Binary / ObjectId**: Byte-level comparison.
- **Boolean**: `false < true`.
- **DateTime / Timestamp**: Numeric comparison of the underlying value.
- **Null**: All nulls are equal.

## Where Type Bracketing Applies

Type bracketing is used in in-memory `RESULTSORT` operations. When a query includes `RESULTSORT`, the matched documents
are sorted using type bracketing before applying `LIMIT`. It is also used at plan time to order `$in` scan predicates
for `SORTBY` optimization.

Type bracketing is **not** used in:

- Index scans: indexes store entries of a declared type, and lossless numeric widening handles cross-type matching at
  scan time.
- Predicate evaluation: the query engine uses lossless numeric widening for numeric types and strict matching for
  non-numeric types.

## Relationship with strict_types

`strict_types` controls whether type mismatches during indexing produce errors or are silently skipped (
see [strict-types.md](strict-types.md)). Type bracketing is orthogonal to this setting:

- **strict_types = true** (default): A type mismatch at index time causes an error. However, fields that are not indexed
  can still contain mixed types across documents. A full scan with `RESULTSORT` on such a field will encounter mixed
  types, and type bracketing handles the ordering.
- **strict_types = false**: Mismatched fields are silently skipped during indexing. Documents are still stored with
  their original field types, so `RESULTSORT` on those fields will encounter mixed types.

In both cases, type bracketing produces a consistent, deterministic sort order.

## Predicate Evaluation and Type Matching

The query engine enforces type matching during predicate evaluation, regardless of `strict_types` configuration. For
numeric
types, lossless widening is applied: a predicate like `{age: {$gt: 30}}` (where `30` is an Int32) matches documents
where
`age` is Int32, Int64, Double, or Decimal128, and both values are promoted to a common lossless type before comparison.
Documents where `age` is a String or any other non-numeric type are excluded.

Type bracketing applies after filtering when sorting the matched results.
