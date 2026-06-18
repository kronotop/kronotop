---
title: "$regex"
description: "$regex matches a string field against a regular expression pattern."
---

## Introduction

`$regex` matches a string field against a regular expression pattern. This is pattern matching, not full-text search.

`$regex` is a query operator only. It is used in BQL filters for `BUCKET.QUERY`, `BUCKET.DELETE`, `BUCKET.UPDATE`, and
as a `FILTER` in `BUCKET.VECTOR`. It cannot be stored as a document value.

## Syntax

A pattern as a string:

```json
{ "name": { "$regex": "^foo" } }
```

A pattern with options:

```json
{ "name": { "$regex": "^foo", "$options": "i" } }
```

A regular expression literal (when the client sends the query as BSON):

```json
{ "name": /^foo/i }
```

Negated with `$not`:

```json
{ "name": { "$not": { "$regex": "^foo" } } }
```

When `$regex` appears in the same field document as another operator, the conditions are combined with AND. The
following matches documents where `name` starts with `a` and the `name` field exists:

```json
{ "name": { "$regex": "^a", "$exists": true } }
```

If both a regular expression literal and an explicit `$options` are given, the explicit `$options` takes precedence.

## Options

| Option | Meaning                                                     |
|--------|-------------------------------------------------------------|
| `i`    | Case-insensitive matching                                   |
| `m`    | Multiline: `^` and `$` match at line boundaries             |
| `s`    | Dotall: `.` matches newline characters                      |
| `u`    | Accepted but has no effect; matching is already UTF-8 aware |

Any other option character is rejected when the query is parsed.

## Pattern syntax

Kronotop uses RE2 regular expression syntax. For the full grammar, character classes, and flags, see the
[RE2 syntax reference](https://github.com/google/re2/wiki/Syntax).

Matching runs in linear time and never backtracks. Backreferences and lookaround are not supported. A pattern that uses
them is rejected when the query is parsed, as is any malformed pattern.

## Matching semantics

`$regex` matches by substring search, not full-string match. The pattern `foo` matches any string that contains `foo`
anywhere. Anchor the pattern with `^` and `$` to require a full-string match.

Type matching is strict. `$regex` only matches string values. A field holding a number, boolean, or any other non-string
type never matches.

For an array field, `$regex` matches the document if any string element of the array matches the pattern. Non-string
elements are ignored.

## Use with $in, $nin, and $all

`$in`, `$nin`, and `$all` accept regular expression literals as array elements. Each regex element is a matcher applied
with the same string-only semantics described above, and a list may mix regex literals with plain values.

- `$in` matches when the field matches any element, literal or regex.
- `$nin` excludes the document when the field matches any element.
- `$all` requires every element, including each regex, to match. For an array field, each pattern must match some element.

Regex literals in arrays can be expressed only through BSON input, since JSON arrays have no regular expression literal
syntax. The filter below matches documents whose `name` begins with `Al` or `Bo`:

```json
{ "name": { "$in": [/^Al/, /^Bo/] } }
```

A regex element never matches a non-string value, so a numeric or boolean field is skipped just as it is with `$regex`.

## Examples

These examples use a `users` bucket.

```kronotop
BUCKET.CREATE users
```

```kronotop
BUCKET.INSERT users DOCS '{"name": "Alice"}'
BUCKET.INSERT users DOCS '{"name": "Alana"}'
BUCKET.INSERT users DOCS '{"name": "Bob"}'
```

**Prefix match.** Find users whose name starts with `Al`:

```kronotop
> BUCKET.QUERY users '{"name": {"$regex": "^Al"}}'
1# "cursor_id" => (integer) 1
2# "entries" =>
   1) {"_id": "6835a1c0e4b0f72a3c000001", "name": "Alice"}
   2) {"_id": "6835a1c0e4b0f72a3c000002", "name": "Alana"}
```

**Case-insensitive match.** The `i` option ignores case:

```kronotop
> BUCKET.QUERY users '{"name": {"$regex": "^alice$", "$options": "i"}}'
1# "cursor_id" => (integer) 1
2# "entries" => 1) {"_id": "6835a1c0e4b0f72a3c000001", "name": "Alice"}
```

**Negation.** Find users whose name does not start with `Al`:

```kronotop
> BUCKET.QUERY users '{"name": {"$not": {"$regex": "^Al"}}}'
1# "cursor_id" => (integer) 1
2# "entries" => 1) {"_id": "6835a1c0e4b0f72a3c000003", "name": "Bob"}
```

`$not` returns every document the pattern does not match. A missing, null, or non-string field never matches the
pattern, so `$not` returns those documents as well.

**Array element match.** `$regex` matches if any string element of an array field matches. Using a `products` bucket
where each document has a `tags` array:

```kronotop
BUCKET.INSERT products DOCS '{"tags": ["red", "green"]}'
BUCKET.INSERT products DOCS '{"tags": ["blue"]}'
```

```kronotop
> BUCKET.QUERY products '{"tags": {"$regex": "een$"}}'
1# "cursor_id" => (integer) 1
2# "entries" => 1) {"_id": "6835a1c0e4b0f72a3c000004", "tags": ["red", "green"]}
```

## Use in vector search

`BUCKET.VECTOR` accepts a `$regex` filter. The filter runs on the search candidates. Only documents whose field matches
the pattern are returned:

```kronotop
> BUCKET.VECTOR products embedding 0.1 0.2 0.3 FILTER '{"label": {"$regex": "^al"}}'
```

See [BUCKET.VECTOR](commands/bucket-vector.md) for the full command syntax.

## Performance and indexing

`$regex` is never resolved by an index. It always runs as a full scan with the pattern applied as a residual filter.
Inspect a query plan with `BUCKET.EXPLAIN`:

```kronotop
> BUCKET.EXPLAIN users '{"name": {"$regex": "^Al"}}'
```

The plan reports a full scan with a `REGEX` predicate on the field.

Query plans are cached by query shape. The pattern is a parameter, not part of the shape, so regex queries on the same
field with different patterns share a single cached plan. Each query still matches with its own pattern.

To narrow a scan, combine `$regex` with an indexed condition on another field. The engine uses the index for candidate
retrieval and applies `$regex` as a filter. Use `BUCKET.EXPLAIN` to confirm.

## Edge cases

| Condition                                | Result                 |
|------------------------------------------|------------------------|
| Field is missing from the document       | Does not match         |
| Field is `null`                          | Does not match         |
| Field is not a string (number, boolean)  | Does not match         |
| Field is an empty array `[]`             | Does not match         |
| Field is an array with a matching string | Matches                |
| `$options` given without `$regex`        | Rejected at parse time |
| Unsupported option character             | Rejected at parse time |
| Invalid or malformed pattern             | Rejected at parse time |
