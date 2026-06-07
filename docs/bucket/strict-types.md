---
title: "Strict Types"
description: "Kronotop treats every BSON type as distinct."
---

## Introduction

Kronotop treats every BSON type as distinct. `INT32` is not `STRING`. `BOOLEAN` is not `INT32`. There is no implicit
type
coercion between unrelated types, not during indexing, not during queries, not during background index builds.

For numeric types (`INT32`, `INT64`, `DOUBLE`, `DECIMAL128`), Kronotop supports **lossless numeric widening**. An
`INT32`
value can be widened to `INT64`, `DOUBLE`, or `DECIMAL128` without loss of precision. This allows queries and indexes to
work across compatible numeric types while preserving exactness.

## Why strict types?

### Clean data, clean results

When every value in an index has a known numeric or non-numeric type, queries are predictable. A predicate like
`{age: {$gt: 25}}`
with an `INT32` value matches `INT32`, `INT64`, `DOUBLE`, and `DECIMAL128` values via lossless widening, but never a
`STRING`.
No surprises from unrelated types sneaking in.

### Simpler index internals

Without strict typing, range scans on FoundationDB would need "type bracketing", encoding type prefixes into index keys
so a scan for integers doesn't accidentally cross into string territory. FoundationDB's tuple layer encodes different
types
differently, so a range scan for numeric values between 10 and 50 could pick up `STRING` values that happen to fall in
the same
byte range. Strict typing sidesteps this for non-numeric types. For numeric types, all four types (`INT32`, `INT64`,
`DOUBLE`,
`DECIMAL128`) share a single bracket, and lossless widening handles cross-type comparisons within that bracket.

### Less complexity, fewer bugs

Numeric widening is limited to lossless paths. Every conversion preserves the original value exactly. Non-numeric types
remain strictly separated with no coercion of any kind.

## Configuration

```hocon
bucket {
  index {
    strict_types = true   # default
  }
}
```

`strict_types` is a global setting, not per-index. The default is `true`.

## Numeric widening

Kronotop supports lossless numeric widening across the four numeric BSON types. Widening is applied during index
selection,
query predicate evaluation, index scan bounds calculation, and type bracket comparison.

### Allowed widening paths

| From   | To                        |
|--------|---------------------------|
| INT32  | INT64, DOUBLE, DECIMAL128 |
| INT64  | DECIMAL128                |
| DOUBLE | DECIMAL128                |

### Forbidden path

`INT64` to `DOUBLE` is explicitly forbidden. 64-bit integers exceed the 53-bit mantissa of IEEE 754 doubles, which would
cause silent precision loss.

### Common type resolution and cost

When two numeric values of different types need to be compared, Kronotop resolves them to the cheapest lossless common
type.
It does NOT always promote to `DECIMAL128`.

| Pair                    | Common type | Cost                                                           |
|-------------------------|-------------|----------------------------------------------------------------|
| INT32 + INT32           | INT32       | Identity, no conversion                                        |
| INT32 + INT64           | INT64       | Cheap cast                                                     |
| INT32 + DOUBLE          | DOUBLE      | Cheap cast                                                     |
| INT32 + DECIMAL128      | DECIMAL128  | BigDecimal allocation                                          |
| INT64 + INT64           | INT64       | Identity, no conversion                                        |
| INT64 + DOUBLE          | DECIMAL128  | BigDecimal allocation, unavoidable since INT64→DOUBLE is lossy |
| DOUBLE + DOUBLE         | DOUBLE      | Identity, no conversion                                        |
| DOUBLE + DECIMAL128     | DECIMAL128  | BigDecimal allocation                                          |
| DECIMAL128 + DECIMAL128 | DECIMAL128  | Identity, no conversion                                        |

`DECIMAL128` (backed by `BigDecimal`) is only used when one side is already `DECIMAL128`, or for `INT64` vs `DOUBLE`
where
it is the only lossless common representation.

Same-type pairs are compared with primitive operations directly. Widening is only invoked for actual cross-type
comparisons.

### Where widening applies

- **Index selection**: The physical planner can select an `INT64` index for an `INT32` query predicate.
- **Predicate evaluation**: A predicate `{age: {$gt: 25}}` (INT32) matches documents where `age` is `INT64(30)`.
- **Index scan bounds**: Query bounds are widened to the index's declared type for correct FoundationDB tuple encoding.
- **Type bracket comparison**: All numeric types share a single bracket in the sort ordering.

### What is NOT widened

Non-numeric types are never widened. `STRING`, `BOOLEAN`, `DATETIME`, `OBJECT_ID`, and all other non-numeric types
remain
strictly typed. A type mismatch between any of these types always evaluates to `false`.

## Behavior when `strict_types = true` (default)

When strict types are enabled, a type mismatch between a document field and the index's declared type causes the
operation
to fail, unless the mismatch is a lossless numeric widening. For example, inserting an `INT32` value into an `INT64`
index
succeeds because `INT32` can be losslessly widened to `INT64`.

- **INSERT**: If a document field's type doesn't match the index's declared type and cannot be losslessly widened, the
  operation fails with an `INDEXTYPE_MISMATCH` error.
- **UPDATE**: A non-widenable type mismatch on an indexed field fails the update.
- **Background index build**: A non-widenable type mismatch marks the index build task as `FAILED`.

The error message format is:

```kronotop
Index type mismatch: index 'idx_age' expects 'INT32', but selector 'age' matched a value of type 'STRING'
```

## Behavior when `strict_types = false`

When strict types are disabled, type mismatches are handled silently. Documents are still written, but mismatched fields
are skipped during indexing.

- **INSERT**: Type-mismatched fields are silently skipped during indexing. The document is still inserted, but the
  mismatched field is not added to the index.
- **UPDATE**: Mismatched fields are skipped, and the document is updated normally.
- **Background index build**: Mismatched documents are skipped, and the build continues to completion.
- **Query impact**: Documents with unindexed fields won't appear in index-assisted queries for that field. They can
  still be found via a full scan, but that's slower.

## Query engine behavior

The query engine enforces type matching during predicate evaluation, regardless of the `strict_types` setting.
For non-numeric types, this is strict: a `STRING` predicate never matches an `INT32` value. For numeric types, lossless
widening is applied.

A predicate `{age: {$gt: 25}}` where `25` is `INT32` matches documents where `age` is `INT32`, `INT64`, `DOUBLE`, or
`DECIMAL128`. Both values are promoted to a common lossless type before comparison. The same predicate will never match
a document where `age` is a `STRING`.

**Non-numeric types** (strict matching, no cross-type comparison):

- `StringVal` matches only `STRING`
- `BooleanVal` matches only `BOOLEAN`
- `ObjectIdVal` matches only `OBJECT_ID`
- `DateTimeVal` matches only `DATE_TIME`

**Numeric types** (lossless widening across the numeric bracket):

- `Int32Val` matches `INT32`, `INT64`, `DOUBLE`, `DECIMAL128`
- `Int64Val` matches `INT32`, `INT64`, `DOUBLE`, `DECIMAL128`
- `DoubleVal` matches `INT32`, `INT64`, `DOUBLE`, `DECIMAL128`
- `Decimal128Val` matches `INT32`, `INT64`, `DOUBLE`, `DECIMAL128`

Every numeric predicate can match every numeric document type. Both values are promoted to the cheapest lossless common
type before comparison (e.g., `INT32` + `INT64` → `INT64`, `INT64` + `DOUBLE` → `DECIMAL128`).

## Practical example

Create a bucket and an `INT32` index on `age`:

```kronotop
BUCKET.CREATE users
BUCKET.INDEX CREATE users '{"age": {"bson_type": "int32", "name": "idx_age"}}'
```

Insert a document with a matching type:

```kronotop
BUCKET.INSERT users DOCS '{"name": "Alice", "age": 30}'
```

This succeeds. The `age` field is `INT32`, matching the index definition.

Now insert a document with a mismatched type:

```kronotop
BUCKET.INSERT users DOCS '{"name": "Bob", "age": "thirty"}'
```

With `strict_types = true` (default), this fails:

```kronotop
INDEXTYPE_MISMATCH Index type mismatch: index 'idx_age' expects 'INT32', but selector 'age' matched a value of type 'STRING'
```

With `strict_types = false`, the insert succeeds but the `age` field is not indexed. A query like `{age: {$gt: 25}}`
using the index won't find Bob's document.

### Numeric widening in action

Create a bucket and a `DOUBLE` index on `price`:

```kronotop
BUCKET.CREATE products
BUCKET.INDEX CREATE products '{"price": {"bson_type": "double", "name": "idx_price"}}'
```

Insert a document with an `INT32` value:

```kronotop
BUCKET.INSERT products DOCS '{"name": "Widget", "price": 50}'
```

This succeeds. The `price` field is `INT32`, which can be losslessly widened to `DOUBLE` for the index.

Query with an `INT32` predicate:

```kronotop
BUCKET.QUERY products '{"price": {"$gt": 25}}'
```

This also succeeds. The `INT32` predicate value `25` is widened to `DOUBLE` to match the index's declared type, and the
index scan finds the document.

Now insert a document with a `STRING` value:

```kronotop
BUCKET.INSERT products DOCS '{"name": "Gadget", "price": "fifty"}'
```

With `strict_types = true` (default), this fails with `INDEXTYPE_MISMATCH`. `STRING` cannot be widened to `DOUBLE`.

## Recommendation

Keep `strict_types = true` (the default). It catches data quality issues at write time rather than producing confusing
query results later. Only disable it if you have a specific need for schemaless flexibility and understand that
type-mismatched
fields won't be indexed.
