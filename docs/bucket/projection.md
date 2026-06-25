---
title: "Projection"
description: "Projection controls which fields appear in documents returned by BUCKET.QUERY and BUCKET.VECTOR."
---

Projection controls which fields appear in documents returned by `BUCKET.QUERY` and `BUCKET.VECTOR`. The `PROJECTION`
parameter accepts a JSON specification that selects fields to include or exclude.

Projection is applied after query execution. It does not affect which documents match a filter or how vector similarity
is ranked. It only shapes what each returned document contains.

```kronotop
BUCKET.QUERY users '{"status": "active"}' PROJECTION '{"name": 1, "email": 1}'
```

## Projection Spec

A projection spec is a JSON object where keys are field names and values are `1` (include) or `0` (exclude).

### Inclusion Mode

When the spec contains fields set to `1`, only those fields are returned. The `_id` field is included by default.

```kronotop
BUCKET.QUERY users '{}' PROJECTION '{"name": 1, "age": 1}'
```

Input document:

```json
{"_id": {"$oid": "6835a1c0e4b0f72a3c000001"}, "name": "Alice", "age": 30, "email": "alice@example.com"}
```

Returned:

```json
{"_id": {"$oid": "6835a1c0e4b0f72a3c000001"}, "name": "Alice", "age": 30}
```

### Exclusion Mode

When the spec contains fields set to `0`, all fields are returned except those specified.

```kronotop
BUCKET.QUERY users '{}' PROJECTION '{"email": 0}'
```

Input document:

```json
{"_id": {"$oid": "6835a1c0e4b0f72a3c000001"}, "name": "Alice", "age": 30, "email": "alice@example.com"}
```

Returned:

```json
{"_id": {"$oid": "6835a1c0e4b0f72a3c000001"}, "name": "Alice", "age": 30}
```

### Mixing Rules

Inclusion and exclusion cannot be mixed in the same spec. The only exception is `_id: 0`, which can be combined with
inclusion fields.

| Spec                      | Valid | Mode      |
|---------------------------|-------|-----------|
| `{"name": 1, "age": 1}`   | Yes   | Inclusion |
| `{"email": 0}`            | Yes   | Exclusion |
| `{"_id": 0, "name": 1}`   | Yes   | Inclusion |
| `{"_id": 0}`              | Yes   | Exclusion |
| `{"name": 1, "email": 0}` | No    | Error     |
| `{}`                      | Yes   | No-op     |

### Empty Spec

An empty spec `{}` returns all fields unchanged, equivalent to no projection.

## The `_id` Field

The `_id` field is included by default in inclusion mode. To exclude it, set `"_id": 0` explicitly.

**Inclusion without `_id` exclusion**, `_id` is included:

```kronotop
BUCKET.QUERY users '{}' PROJECTION '{"name": 1}'
```

```json
{"_id": {"$oid": "6835a1c0e4b0f72a3c000001"}, "name": "Alice"}
```

**Inclusion with `_id: 0`**, `_id` is excluded:

```kronotop
BUCKET.QUERY users '{}' PROJECTION '{"_id": 0, "name": 1}'
```

```json
{"name": "Alice"}
```

**`{"_id": 0}` alone**, exclusion mode, returns all fields except `_id`:

```kronotop
BUCKET.QUERY users '{}' PROJECTION '{"_id": 0}'
```

```json
{"name": "Alice", "age": 30, "email": "alice@example.com"}
```

## Nested Fields

Projection specs support [dot notation](dot-notation.md) for nested fields. The parent document structure is preserved.

### Inclusion

```kronotop
BUCKET.QUERY users '{}' PROJECTION '{"address.city": 1}'
```

Input document:

```json
{"_id": {"$oid": "..."}, "name": "Alice", "address": {"city": "Istanbul", "zip": "34000"}}
```

Returned:

```json
{"_id": {"$oid": "..."}, "address": {"city": "Istanbul"}}
```

The `address` object is preserved but only contains the `city` field. Sibling fields (`name`, `address.zip`) are
omitted.

### Exclusion

```kronotop
BUCKET.QUERY users '{}' PROJECTION '{"address.zip": 0}'
```

Returned:

```json
{"_id": {"$oid": "..."}, "name": "Alice", "address": {"city": "Istanbul"}}
```

Only the `zip` field is removed. All other fields are preserved.

### Arrays

When a dot-notation path crosses an array of documents, the projection applies to each element in the array.

```kronotop
BUCKET.QUERY shop '{}' PROJECTION '{"orders.total": 1}'
```

Input document:

```json
{"_id": {"$oid": "..."}, "orders": [{"total": 120, "status": "shipped"}, {"total": 45, "status": "pending"}]}
```

Returned:

```json
{"_id": {"$oid": "..."}, "orders": [{"total": 120}, {"total": 45}]}
```

This works through multiple levels of nesting: `"orders.items.name": 1` extracts `name` from each item in each order.

## Positional Operator (`$`)

The `$` operator returns the first array element that matched the query condition. It is used with inclusion mode.

```kronotop
BUCKET.QUERY students '{"grades": {"$gte": 85}}' PROJECTION '{"grades.$": 1}'
```

Input document:

```json
{"_id": {"$oid": "..."}, "name": "Alice", "grades": [70, 87, 90]}
```

Returned:

```json
{"_id": {"$oid": "..."}, "grades": [87]}
```

The query matched elements `87` and `90` (both `>= 85`), but `$` returns only the first match.

### Nested Paths

The `$` operator works with nested paths:

```kronotop
BUCKET.QUERY students '{"user.grades": {"$gte": 85}}' PROJECTION '{"user.grades.$": 1}'
```

### Rules

- `$` must appear at the **end** of the field path (e.g., `"grades.$": 1`, not `"grades.$.value": 1`).
- Only **one** `$` operator is allowed per projection spec.
- The query must reference the array field for `$` to identify the matched element.
- `$` can be combined with other inclusion fields in the same spec.

### Fallback Behavior

When the query does not reference the array field used with `$`, the operator defaults to the first element (index 0).

```kronotop
BUCKET.QUERY students '{"name": "Alice"}' PROJECTION '{"grades.$": 1}'
```

The query filters on `name`, not on `grades`. The `$` operator returns `grades[0]`.

## Slice Operator (`$slice`)

The `$slice` operator returns a subset of an array field instead of the whole array. The value is either a single number 
or a `[skip, limit]` pair.

```kronotop
BUCKET.QUERY users '{}' PROJECTION '{"comments": {"$slice": 2}}'
```

Input document:

```json
{"_id": {"$oid": "..."}, "name": "Alice", "comments": ["a", "b", "c", "d"]}
```

Returned:

```json
{"_id": {"$oid": "..."}, "name": "Alice", "comments": ["a", "b"]}
```

### Single Number

- A positive `n` returns the first `n` elements.
- A negative `n` returns the last `|n|` elements.
- When `|n|` is larger than the array length, the whole array is returned.

```kronotop
BUCKET.QUERY users '{}' PROJECTION '{"comments": {"$slice": -2}}'
```

Returns the last two comments.

### Skip and Limit

`[skip, limit]` skips elements first, then caps how many are returned. The `limit` must be positive.

- A non-negative `skip` skips from the start. A `skip` past the array length returns an empty array.
- A negative `skip` counts back from the end. When its magnitude exceeds the length, it clamps to the start of the array.

```kronotop
BUCKET.QUERY users '{}' PROJECTION '{"comments": {"$slice": [1, 2]}}'
```

Skips the first comment and returns the next two.

### Nested Paths

The `$slice` operator works with nested paths:

```kronotop
BUCKET.QUERY products '{}' PROJECTION '{"details.colors": {"$slice": 1}}'
```

### Inclusion and Exclusion

`$slice` used by itself behaves like exclusion: every other field is returned, with the named array sliced. Sibling fields under a nested path are kept.

When the spec also lists inclusion fields, `$slice` behaves within that inclusion: only the included fields and the sliced array are returned, and sibling fields under the nested path are dropped. `_id` is returned by default unless `_id: 0` is set.

### Rules

- The slice value must be an integer, or an `[skip, limit]` pair of integers. Fractional or out-of-range values are rejected.
- The `limit` in `[skip, limit]` must be positive.
- A non-array value passes through unchanged.
- `$slice` cannot be combined with the positional `$` operator in the same spec.
- A `$slice` path and any projected field cannot overlap. Neither may be a prefix of the other, so you cannot project a field embedded in a sliced array, slice inside a fully projected parent, or use two overlapping `$slice` paths.

## Usage

| Command         | Syntax                                                             |
|-----------------|--------------------------------------------------------------------|
| `BUCKET.QUERY`  | `BUCKET.QUERY <bucket> <query> PROJECTION <spec>`                  |
| `BUCKET.VECTOR` | `BUCKET.VECTOR <bucket> <selector> <vector> ... PROJECTION <spec>` |

### BUCKET.QUERY

```kronotop
BUCKET.QUERY users '{"status": "active"}' PROJECTION '{"name": 1, "email": 1}' LIMIT 10
```

### BUCKET.VECTOR

Projection is useful for excluding large embedding arrays from vector search results:

```kronotop
BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' PROJECTION '{"embedding": 0}'
```

Or returning only specific fields:

```kronotop
BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' PROJECTION '{"label": 1}'
```

When a `FILTER` is provided, the positional `$` operator uses the filter expression to identify matched elements.

```kronotop
BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' FILTER '{"tags": "ml"}' PROJECTION '{"tags.$": 1}'
```
