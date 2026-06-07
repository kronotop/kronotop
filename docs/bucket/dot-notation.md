---
title: "Dot Notation"
description: "Dot notation is the path syntax used to address fields inside documents."
---

## Introduction

Dot notation is the path syntax used to address fields inside documents. Queries, indexes, and filters all use dot
notation to identify which field a predicate or index applies to. A dot-separated string like `"address.city"` tells the
system to navigate into the `address` object and select the `city` field.

## Syntax

A selector is one or more field names separated by dots:

```
"field"
"parent.child"
"parent.child.grandchild"
```

Each segment between dots is resolved left to right against the current value. The starting point is always the root of
the document.

## Traversal Rules

### Top-level fields

A single segment selects a root-level field.

```json
{"name": "Alice", "age": 30}
```

| Selector | Result    |
|----------|-----------|
| `name`   | `"Alice"` |
| `age`    | `30`      |

### Nested documents

When a segment resolves to a document, the next segment selects a field inside that document.

```json
{
  "address": {
    "city": "Istanbul",
    "zip": "34000"
  }
}
```

| Selector       | Result                                 |
|----------------|----------------------------------------|
| `address`      | `{"city": "Istanbul", "zip": "34000"}` |
| `address.city` | `"Istanbul"`                           |
| `address.zip`  | `"34000"`                              |

This works at any depth:

```json
{
  "config": {
    "database": {
      "host": "localhost"
    }
  }
}
```

| Selector               | Result        |
|------------------------|---------------|
| `config.database.host` | `"localhost"` |

### Array numeric indexing

When a segment resolves to an array and the next segment is a number, it selects the element at that zero-based index.

```json
{
  "scores": [95, 87, 92]
}
```

| Selector   | Result |
|------------|--------|
| `scores.0` | `95`   |
| `scores.1` | `87`   |
| `scores.2` | `92`   |
| `scores.5` | *null* |

### Array field collection

When a segment resolves to an array and the next segment is a non-numeric field name, the system iterates through every
element of the array. For each element that is a document, it looks up the field inside that document. The collected
values are returned as an array.

```json
{
  "orders": [
    {"total": 120, "status": "shipped"},
    {"total": 45, "status": "pending"}
  ]
}
```

| Selector        | Result                   |
|-----------------|--------------------------|
| `orders.total`  | `[120, 45]`              |
| `orders.status` | `["shipped", "pending"]` |

Non-document elements in the array are skipped. If no elements match, the result is *null*.

### Multi-level array flattening

When field collection passes through multiple levels of arrays, collected values are flattened into a single array.

```json
{
  "departments": [
    {"teams": [{"name": "Alpha"}, {"name": "Beta"}]},
    {"teams": [{"name": "Gamma"}]}
  ]
}
```

| Selector                 | Result                       |
|--------------------------|------------------------------|
| `departments.teams.name` | `["Alpha", "Beta", "Gamma"]` |

The result is a flat list, not nested arrays.

### Mixed paths

These rules compose. A single selector can mix document traversal, numeric array indexing, and array field collection.

```json
{
  "users": [
    {
      "name": "Alice",
      "address": {"city": "Istanbul"}
    },
    {
      "name": "Bob",
      "address": {"city": "Ankara"}
    }
  ]
}
```

| Selector               | Result                                               |
|------------------------|------------------------------------------------------|
| `users.0`              | `{"name": "Alice", "address": {"city": "Istanbul"}}` |
| `users.0.name`         | `"Alice"`                                            |
| `users.0.address.city` | `"Istanbul"`                                         |
| `users.name`           | `["Alice", "Bob"]`                                   |
| `users.address.city`   | `["Istanbul", "Ankara"]`                             |

## Missing Paths

When a selector does not match any value, the result is *null*. This covers missing fields, out-of-bounds array indexes,
and segments that try to traverse into a primitive. No error is raised.

| Scenario                    | Example selector  | Result |
|-----------------------------|-------------------|--------|
| Field does not exist        | `email`           | *null* |
| Nested field does not exist | `address.country` | *null* |
| Array index out of bounds   | `scores.99`       | *null* |
| Traversal into a primitive  | `name.first`      | *null* |
