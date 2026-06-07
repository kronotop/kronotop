---
title: "Collation"
description: "Collation controls how strings are compared and ordered."
---

Collation controls how strings are compared and ordered. By default, Kronotop compares strings using binary (byte-order)
comparison, which works well for ASCII text but produces unexpected results for accented characters, mixed-case text,
and non-Latin scripts. Collation uses [ICU4J](https://unicode-org.github.io/icu/userguide/icu4j/) (International
Components for Unicode for Java) to provide locale-aware
string comparison, enabling case-insensitive searches, accent-insensitive matching, and natural language ordering.
ICU4J handles all collation behavior: sort key generation, strength levels, numeric ordering, and locale-specific rules.

Kronotop supports collation at three levels: bucket, index, and query.

## Collation Specification

A collation is specified as a JSON object. Only `locale` is required; all other fields have sensible defaults:

```json
{"locale": "tr"}
```

The full set of fields:

| Field              | Type    | Required | Default           | Description                                                                                                                                 |
|--------------------|---------|----------|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `locale`           | string  | Yes      | --                | ICU locale identifier (e.g., `"en"`, `"tr"`, `"fr"`, `"de"`, `"es"`).                                                                       |
| `strength`         | integer | No       | `3`               | Comparison strength level (1--5). See [Strength Levels](#strength-levels).                                                                  |
| `case_level`       | boolean | No       | `false`           | When `true`, adds a case distinction level between primary and secondary comparisons.                                                       |
| `case_first`       | string  | No       | `"off"`           | Controls whether uppercase or lowercase letters sort first: `"upper"`, `"lower"`, or `"off"`.                                               |
| `numeric_ordering` | boolean | No       | `false`           | When `true`, digit substrings are compared as numbers. See [Numeric Ordering](#numeric-ordering).                                           |
| `alternate`        | string  | No       | `"non-ignorable"` | Controls handling of spaces and punctuation: `"non-ignorable"` or `"shifted"`. See [Alternate and MaxVariable](#alternate-and-maxvariable). |
| `backwards`        | boolean | No       | `false`           | When `true`, reverses the secondary (accent) comparison pass. Useful for some French sorting rules.                                         |
| `normalization`    | boolean | No       | `false`           | When `true`, performs Unicode normalization before comparison.                                                                              |
| `max_variable`     | string  | No       | `"punct"`         | When `alternate` is `"shifted"`, controls which characters become ignorable: `"punct"` or `"space"`.                                        |

A full specification with all fields:

```json
{
  "locale": "en",
  "strength": 2,
  "case_level": false,
  "case_first": "off",
  "numeric_ordering": false,
  "alternate": "non-ignorable",
  "backwards": false,
  "normalization": false,
  "max_variable": "punct"
}
```

## Strength Levels

The `strength` field controls how fine-grained the comparison is. Lower strengths ignore more differences.

| Strength | Name       | Behavior                                                       | Example                                                 |
|----------|------------|----------------------------------------------------------------|---------------------------------------------------------|
| 1        | PRIMARY    | Base letter only. Case and accents are ignored.                | `"cafe"` = `"CAFE"` = `"café"`                          |
| 2        | SECONDARY  | Base letter + accents. Case is ignored.                        | `"cafe"` = `"Cafe"`, but `"cafe"` ≠ `"café"`            |
| 3        | TERTIARY   | Base letter + accents + case. Default.                         | `"cafe"` ≠ `"Cafe"` ≠ `"café"`                          |
| 4        | QUATERNARY | Adds punctuation distinctions when `alternate` is `"shifted"`. | `"black-bird"` ≠ `"blackbird"` (with shifted alternate) |
| 5        | IDENTICAL  | All differences are significant, including code point.         | Distinguishes canonically equivalent sequences.         |

Strengths 1 and 2 are the most commonly used. Strength 1 provides the broadest matching (case-insensitive and
accent-insensitive), while strength 2 provides case-insensitive but accent-sensitive matching.

## Numeric Ordering

When `numeric_ordering` is `true`, contiguous digit substrings within strings are compared as numbers instead of
character by character. This produces natural sorting for strings with embedded numbers.

Without numeric ordering (default), strings with embedded numbers are compared character by character, the same ordering
as binary comparison without any collation:

```
"item1" < "item10" < "item2" < "item20"
```

With `numeric_ordering: true`:

```
"item1" < "item2" < "item10" < "item20"
```

### Limitations

- Works for positive integers embedded in strings.
- Negative numbers are not supported. The minus sign is treated as a separator, so `"-2"` and `"-10"` do not sort
  numerically.
- Decimal numbers are not supported. The decimal point is treated as a separator, so `"2.1"` and `"2.10"` do not sort as
  expected.
- The `+` prefix is not supported as a positive sign. It is treated as a separator, so `"+2"` and `"+10"` do not sort as
  signed numbers.
- Exponent notation is not supported. In strings like `"1e5"` or `"2E3"`, the letter acts as a separator and digit
  groups on each side are compared independently as integers.

## Alternate and MaxVariable

The `alternate` and `max_variable` fields work together to control whether spaces and punctuation are significant during
comparison.

| `alternate`       | `max_variable` | Effect                                                                                         |
|-------------------|----------------|------------------------------------------------------------------------------------------------|
| `"non-ignorable"` | (ignored)      | Spaces and punctuation are significant at all strength levels. This is the default.            |
| `"shifted"`       | `"punct"`      | Spaces and punctuation are ignorable. `"black-bird"` = `"blackbird"` = `"black bird"`.         |
| `"shifted"`       | `"space"`      | Only spaces are ignorable. `"black bird"` = `"blackbird"`, but `"black-bird"` ≠ `"blackbird"`. |

`max_variable` has no effect when `alternate` is `"non-ignorable"`.

## Collation Levels

### Bucket-level collation

Set a default collation for the entire bucket at creation time. All string indexes in the bucket inherit this collation
unless overridden at the index level.

```kronotop
> BUCKET.CREATE users COLLATION '{"locale": "en", "strength": 2}'
OK
```

Every string index created in this bucket uses case-insensitive English collation by default.

### Index-level collation

Set collation on individual indexes in the index schema. This overrides the bucket-level collation for that specific
index. Collation is only valid for `string` type fields.

Single-field index:

```kronotop
> BUCKET.INDEX CREATE users '{
  "username": {
    "bson_type": "string",
    "collation": {"locale": "tr", "strength": 2}
  }
}'
OK
```

Compound index:

```kronotop
> BUCKET.INDEX CREATE products '{
  "$compound": [{
    "fields": [
      {"selector": "category", "bson_type": "string"},
      {"selector": "price", "bson_type": "double"}
    ],
    "collation": {"locale": "en"}
  }]
}'
OK
```

Compound indexes require at least one `string` field for collation to apply.

### Query-level collation

Override collation for a single operation using the `COLLATION` parameter. This takes the highest precedence and
overrides both index-level and bucket-level collation.

Query:

```kronotop
> BUCKET.QUERY users '{"name": {"$eq": "alice"}}' COLLATION '{
  "locale": "en",
  "strength": 1
}'
```

Update:

```kronotop
> BUCKET.UPDATE users '{"name": {"$eq": "alice"}}' '{
  "$set": {"verified": true}
}' COLLATION '{"locale": "en", "strength": 1}'
```

Delete:

```kronotop
> BUCKET.DELETE users '{"name": {"$eq": "alice"}}' COLLATION '{
  "locale": "en",
  "strength": 1
}'
```

## Resolution Precedence

Collation resolution follows different rules depending on whether the operation is reading (evaluating predicates) or
writing (building index keys).

### Read path (predicate evaluation)

When evaluating filter conditions in `BUCKET.QUERY`, `BUCKET.UPDATE`, and `BUCKET.DELETE`, the most specific collation
wins:

1. **Query-level** -- collation specified in the command (`COLLATION` parameter)
2. **Index-level (single-field)** -- collation defined on a single-field index for this selector
3. **Index-level (compound)** -- if all READY compound indexes containing the selector as a `string` field agree on the
   same collation, that collation is used; if any two indexes disagree, the entire compound step is skipped and
   resolution falls through to bucket-level. Compound indexes with no collation defined are excluded from the agreement
   check. They neither contribute a collation nor trigger a conflict. For example, if one compound index has collation
   `"de"` and another has no collation, the result is `"de"`.
4. **Bucket-level** -- default collation set at bucket creation
5. **Binary comparison** -- no collation; strings are compared byte by byte

When no collation is specified at any level, strings are compared using binary comparison.

### Write path (index key generation)

When writing index entries during `BUCKET.INSERT` and the index-update phase of `BUCKET.UPDATE`, the query-level
`COLLATION` parameter has no effect. Index keys are always built with the collation that was fixed at index creation
time:

1. **Index-level** -- collation defined on the index
2. **Bucket-level** -- default collation set at bucket creation
3. **Binary comparison** -- no collation; strings are stored as raw bytes

This is intentional: an index is built with a single, stable collation so that its sort keys remain consistent across
all writes. Accepting a different collation at write time would corrupt the sort order of existing entries.

**Practical consequence for `BUCKET.UPDATE`**: the `COLLATION` parameter controls which documents the filter matches,
but the index keys written for the updated document always use the index's own collation, regardless of what `COLLATION`
was specified in the command.

## Index Compatibility

Collation affects which indexes the query engine can use. When a query's effective collation does not match an index's
collation, the index is skipped and the query falls back to a full scan.

The rules:

- A `string` index built with a specific collation is only used when the query's effective collation matches.
- If a query specifies a different collation than the index, the index is skipped.
- A `string` index built without collation (binary) is skipped when the query specifies a collation.
- Non-string indexes (`int32`, `double`, `boolean`, etc.) are unaffected by collation. They are used regardless of the
  query's collation setting.
- Compound indexes with at least one `string` field are skipped entirely when the collation does not match.

Use `BUCKET.EXPLAIN` to verify that the expected index is being used when collation is involved:

```kronotop
> BUCKET.EXPLAIN users '{"name": {"$eq": "alice"}}' COLLATION '{
  "locale": "en",
  "strength": 1
}'
```

If the `BUCKET.EXPLAIN` output shows a `FullScan` instead of an `IndexScan`, the collation may not match the index.

## Practical Example

The examples below use RESP3 protocol output. Switch to RESP3 with `HELLO 3` before running the commands.

Create a bucket with Turkish collation at strength 1 (case-insensitive and accent-insensitive), and a string index on
`city`:

```kronotop
127.0.0.1:5484> BUCKET.CREATE cities COLLATION '{
  "locale": "tr",
  "strength": 1
}' INDEXES '{"city": {"bson_type": "string"}}'
OK
```

The `city` index inherits the bucket's Turkish collation.

Insert documents with different case variations:

```kronotop
BUCKET.INSERT cities DOCS '{"city": "istanbul", "population": 16000000}'
BUCKET.INSERT cities DOCS '{"city": "Istanbul", "population": 16000000}'
BUCKET.INSERT cities DOCS '{"city": "ISTANBUL", "population": 16000000}'
BUCKET.INSERT cities DOCS '{"city": "ankara", "population": 5700000}'
```

Query for `"istanbul"` -- with strength 1, all case variations match:

```kronotop
127.0.0.1:5484> BUCKET.QUERY cities '{"city": {"$eq": "istanbul"}}'
1# "cursor_id" => (integer) 1
2# "entries" =>
   1) {"_id": "682c5a006597b10d87d13500", "city": "istanbul", "population": 16000000}
   2) {"_id": "682c5a006597b10d87d13501", "city": "Istanbul", "population": 16000000}
   3) {"_id": "682c5a006597b10d87d13502", "city": "ISTANBUL", "population": 16000000}
```

All three documents match because at primary strength, `"istanbul"`, `"Istanbul"`, and `"ISTANBUL"` are considered equal
under Turkish locale rules.

Override with query-level collation at strength 3 (case-sensitive) -- only exact matches are returned:

```kronotop
127.0.0.1:5484> BUCKET.QUERY cities '{"city": {"$eq": "istanbul"}}' COLLATION '{
  "locale": "tr",
  "strength": 3
}'
1# "cursor_id" => (integer) 2
2# "entries" =>
   1) {"_id": "682c5a006597b10d87d13500", "city": "istanbul", "population": 16000000}
```

Only the exact match is returned. The query-level collation overrides the bucket's strength 1 setting for this single
query.

Delete with case-insensitive collation -- all case variations are deleted:

```kronotop
127.0.0.1:5484> BUCKET.DELETE cities '{"city": {"$eq": "istanbul"}}' COLLATION '{
  "locale": "tr",
  "strength": 1
}'
1# "cursor_id" => (integer) 3
2# "object_ids" =>
   1) "682c5a006597b10d87d13500"
   2) "682c5a006597b10d87d13501"
   3) "682c5a006597b10d87d13502"
```
