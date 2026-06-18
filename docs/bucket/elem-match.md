---
title: "$elemMatch"
description: "$elemMatch tests whether at least one element in an array field satisfies all specified conditions."
---

## Introduction

`$elemMatch` tests whether at least one element in an array field satisfies all specified conditions. It works with two
kinds of arrays: scalar arrays that hold primitive values like numbers or strings, and document arrays that hold objects
with named fields.

Without `$elemMatch`, separate conditions on an array field can match different elements. `$elemMatch` guarantees that a
single element satisfies every condition.

## Scalar arrays

This section uses a `sensors` bucket. Each document has a `readings` field containing an array of integers.

```kronotop
BUCKET.CREATE sensors INDEXES '{"readings": {"bson_type": "int32", "multi_key": true}}'
```

```kronotop
BUCKET.INSERT sensors DOCS '{"sensor_id": "temp-01", "readings": [22, 45, 52]}'
BUCKET.INSERT sensors DOCS '{"sensor_id": "temp-02", "readings": [78, 82, 95]}'
BUCKET.INSERT sensors DOCS '{"sensor_id": "temp-03", "readings": [60, 65, 70]}'
```

**Single condition.** Find sensors with at least one reading above 80:

```kronotop
BUCKET.QUERY sensors '{"readings": {"$elemMatch": {"$gt": 80}}}'
```

Matches temp-02 (82, 95 are above 80).

**Range.** Find sensors with at least one reading between 50 and 70 (inclusive):

```kronotop
BUCKET.QUERY sensors '{"readings": {"$elemMatch": {"$gte": 50, "$lte": 70}}}'
```

Matches temp-01 (52) and temp-03 (60, 65, 70).

**Exact match.** Find sensors with a reading equal to 45:

```kronotop
BUCKET.QUERY sensors '{"readings": {"$elemMatch": {"$eq": 45}}}'
```

Matches temp-01.

**Exclusion.** Find sensors with at least one reading that is not 78:

```kronotop
BUCKET.QUERY sensors '{"readings": {"$elemMatch": {"$ne": 78}}}'
```

Matches all three. Each sensor has at least one reading different from 78.

**Set membership.** Find sensors with a reading of 22, 60, or 95:

```kronotop
BUCKET.QUERY sensors '{"readings": {"$elemMatch": {"$in": [22, 60, 95]}}}'
```

Matches temp-01 (22) and temp-02 (95) and temp-03 (60).

**Alternatives.** Find sensors with a reading above 90 or below 25:

```kronotop
BUCKET.QUERY sensors '{"readings": {"$elemMatch": {"$or": [{"$gt": 90}, {"$lt": 25}]}}}'
```

Matches temp-01 (22 is below 25) and temp-02 (95 is above 90).

## Document arrays

This section uses an `orders` bucket. Each document has an `items` array where each element is an object with product
details.

```kronotop
BUCKET.CREATE orders INDEXES '{"items.category": {"bson_type": "string", "multi_key": true}}'
```

```kronotop
BUCKET.INSERT orders DOCS '{
  "customer": "Alice",
  "items": [
    {"product": "Headphones", "category": "electronics", "price": 79.99, "status": "shipped", "tags": ["sale", "new"]},
    {"product": "Novel", "category": "books", "price": 14.99, "status": "delivered", "tags": ["bestseller"]}
  ]
}'
BUCKET.INSERT orders DOCS '{
  "customer": "Bob",
  "items": [
    {"product": "Laptop", "category": "electronics", "price": 999.99, "status": "processing", "tags": ["sale", "new", "featured"]},
    {"product": "Mouse", "category": "electronics", "price": 29.99, "status": "cancelled", "tags": ["sale"]}
  ]
}'
BUCKET.INSERT orders DOCS '{
  "customer": "Carol",
  "items": [
    {"product": "Desk", "category": "furniture", "price": 249.99, "status": "shipped", "tags": ["new"], "discount": 15}
  ]
}'
```

**Single field condition.** Find orders with at least one item priced above 100:

```kronotop
BUCKET.QUERY orders '{"items": {"$elemMatch": {"price": {"$gt": 100}}}}'
```

Matches Bob (Laptop, 999.99) and Carol (Desk, 249.99).

**Multiple conditions.** Find orders with at least one electronics item above 100. Both conditions must be satisfied by
the same element:

```kronotop
BUCKET.QUERY orders '{"items": {"$elemMatch": {"category": "electronics", "price": {"$gt": 100}}}}'
```

Matches only Bob. Alice has an electronics item (Headphones) but it costs 79.99. Carol's expensive item is furniture,
not electronics.

**Exclusion.** Find orders with at least one item whose status is not "cancelled":

```kronotop
BUCKET.QUERY orders '{"items": {"$elemMatch": {"status": {"$ne": "cancelled"}}}}'
```

Matches all three. Each order has at least one non-cancelled item.

**Set membership.** Find orders with at least one item that is "shipped" or "delivered":

```kronotop
BUCKET.QUERY orders '{"items": {"$elemMatch": {"status": {"$in": ["shipped", "delivered"]}}}}'
```

Matches Alice (shipped Headphones, delivered Novel) and Carol (shipped Desk).

**Negative set.** Find orders with at least one item not in ["cancelled", "processing"]:

```kronotop
BUCKET.QUERY orders '{"items": {"$elemMatch": {"status": {"$nin": ["cancelled", "processing"]}}}}'
```

Matches Alice (shipped, delivered) and Carol (shipped).

**Field existence.** Find orders with at least one item that has a "discount" field:

```kronotop
BUCKET.QUERY orders '{"items": {"$elemMatch": {"discount": {"$exists": true}}}}'
```

Matches only Carol (Desk has discount: 15).

**Alternatives.** Find orders with at least one item that is either electronics above 200 or furniture in stock:

```kronotop
BUCKET.QUERY orders '{
  "items": {
    "$elemMatch": {
      "$or": [
        {"$and": [{"category": "electronics"}, {"price": {"$gt": 200}}]},
        {"$and": [{"category": "furniture"}, {"status": {"$ne": "cancelled"}}]}
      ]
    }
  }
}'
```

Matches Bob (Laptop is electronics above 200) and Carol (Desk is furniture, not cancelled).

**Inner array checks with $all.** Find orders with at least one item tagged both "sale" and "new":

```kronotop
BUCKET.QUERY orders '{"items": {"$elemMatch": {"tags": {"$all": ["sale", "new"]}}}}'
```

Matches Alice (Headphones has both) and Bob (Laptop has both).

**Inner array checks with $size.** Find orders with at least one item that has exactly 3 tags:

```kronotop
BUCKET.QUERY orders '{"items": {"$elemMatch": {"tags": {"$size": 3}}}}'
```

Matches only Bob (Laptop has ["sale", "new", "featured"]).

**Combining $all and $size.** Find orders with at least one item tagged both "sale" and "new" with exactly 2 tags:

```kronotop
BUCKET.QUERY orders '{"items": {"$elemMatch": {"tags": {"$all": ["sale", "new"], "$size": 2}}}}'
```

Matches only Alice (Headphones has ["sale", "new"], exactly 2 tags). Bob's Laptop has those tags but 3 tags total.

## Nested fields

Use dot notation inside `$elemMatch` to reach fields in embedded documents within each array element.

Match orders where at least one item has `details.amount` above 150:

```json
{ "orders": { "$elemMatch": { "details.amount": { "$gte": 150 } } } }
```

Multiple nested paths can appear in the same `$elemMatch`. The same element must satisfy all of them:

```json
{
  "entries": {
    "$elemMatch": {
      "meta.count": { "$gt": 70 },
      "meta.total": { "$gt": 1000 }
    }
  }
}
```

## Nested $elemMatch

`$elemMatch` can be nested inside another `$elemMatch` to query arrays within arrays.

**Two levels.** Find stores with at least one department that has at least one product priced above 400:

```json
{
  "departments": {
    "$elemMatch": {
      "products": {
        "$elemMatch": {
          "price": { "$gt": 400 }
        }
      }
    }
  }
}
```

**Three levels.** Find organizations with a division that has a unit containing a group with a score above 90:

```json
{
  "divisions": {
    "$elemMatch": {
      "units": {
        "$elemMatch": {
          "groups": {
            "$elemMatch": {
              "score": { "$gt": 90 }
            }
          }
        }
      }
    }
  }
}
```

**Nested with multiple conditions.** Find companies with a team that has at least one member who is an engineer at level
4 or above:

```json
{
  "teams": {
    "$elemMatch": {
      "members": {
        "$elemMatch": {
          "role": "engineer",
          "level": { "$gte": 4 }
        }
      }
    }
  }
}
```

## Updating matched array elements

`BUCKET.UPDATE` uses `$elemMatch` in the query filter to locate array elements. Three pieces work together:

- **`$elemMatch`** in the query selects which documents match and identifies the first array element that satisfies all
  conditions.
- **`$`** (the positional operator) in the update path stands in for the index of that matched element.
- **`$set`** or **`$unset`** in the update expression describes what to change.

### The positional `$` operator

The `$` placeholder appears in the update path where you would normally write a numeric index. It resolves to the
position of the first element that satisfied the `$elemMatch` filter. If multiple elements match, only the first one is
affected.

The general form:

```kronotop
BUCKET.UPDATE <bucket> '<query with $elemMatch>' '{"$set": {"<array>.$.<field>": <value>}}'
```

For scalar arrays (no nested field), the path is simply `<array>.$`:

```kronotop
BUCKET.UPDATE <bucket> '<query with $elemMatch>' '{"$set": {"<array>.$": <value>}}'
```

### $set

`$set` changes the value of a field on the matched element, or replaces the element itself in scalar arrays.

**Document array.** Using the `orders` bucket from above, change the status of the first item that is priced at 100 or
above and is currently "processing":

```kronotop
BUCKET.UPDATE orders '{"items": {"$elemMatch": {"price": {"$gte": 100}, "status": "processing"}}}' '{"$set": {"items.$.status": "shipped"}}'
```

Bob's Laptop (price 999.99, status "processing") satisfies both conditions. Its status changes to "shipped". The Mouse
does not match, so it stays unchanged.

**Scalar number array.** Using the `sensors` bucket, replace the first reading that is 85 or above with 100:

```kronotop
BUCKET.UPDATE sensors '{"readings": {"$elemMatch": {"$gte": 85}}}' '{"$set": {"readings.$": 100}}'
```

For temp-02 (readings: [78, 82, 95]), 95 is the first element satisfying the condition. The result is [78, 82, 100].

**Scalar string array.** Replace the first tag equal to "featured" with "promoted":

```kronotop
BUCKET.UPDATE products '{"tags": {"$elemMatch": {"$eq": "featured"}}}' '{"$set": {"tags.$": "promoted"}}'
```

### $unset

`$unset` removes a field from the matched element. The `$` in the path identifies which element to modify, and the field
name after it specifies what to remove.

Remove the `discount` field from the first cancelled item:

```kronotop
BUCKET.UPDATE orders '{"items": {"$elemMatch": {"status": "cancelled"}}}' '{"$unset": ["items.$.discount"]}'
```

Bob's Mouse (status "cancelled") loses its `discount` field. Other items in the same document are not affected.

### Nested $elemMatch with `$`

When `$elemMatch` is nested, the `$` operator refers to the position in the outermost array.

Given a document with an `orders` array where each order has an `items` array:

```kronotop
BUCKET.UPDATE customers '{"orders": {"$elemMatch": {"items": {"$elemMatch": {"name": "Widget", "qty": {"$gte": 5}}}}}}' '{"$set": {"orders.$.shipped": true}}'
```

The outer `$elemMatch` finds the first order that contains a Widget with qty >= 5. The `$` targets that order and sets
`shipped: true` on it. Other orders are not touched.

## Indexes

### Multi-key indexes

A multi-key index on the array field allows the query engine to use the index for candidate retrieval. The `$elemMatch`
condition is then applied as a filter to verify that a single element satisfies all conditions.

Scalar array index:

```kronotop
BUCKET.INDEX CREATE sensors '{"readings": {"bson_type": "int32", "multi_key": true}}'
```

Document array index on a field inside each element:

```kronotop
BUCKET.INDEX CREATE teams '{"members.role": {"bson_type": "string", "multi_key": true}}'
```

### Combining with other indexed fields

When `$elemMatch` appears alongside conditions on other indexed fields, the query engine uses the most selective index
for the initial scan and applies `$elemMatch` as a filter.

For example, if `category` has a single-field index:

```kronotop
BUCKET.QUERY orders '{"category": "electronics", "items": {"$elemMatch": {"price": {"$gt": 100}}}}'
```

The engine scans the `category` index first, then filters results with `$elemMatch`. Use `BUCKET.EXPLAIN` to verify.

### Index-accelerated operators

When a multi-key index exists on the array field, the following operators inside `$elemMatch` can use the index:

| Operator                     | Index behavior         |
|------------------------------|------------------------|
| `$eq`                        | Point lookup           |
| `$gt`, `$gte`, `$lt`, `$lte` | Range scan             |
| `$in`                        | Multiple point lookups |

The remaining operators (`$all`, `$size`, `$exists`, `$not`, `$ne`, `$nin`) are not index-accelerated. When these are
the only conditions, the query falls back to a full scan with the `$elemMatch` filter applied.

## Edge cases

| Condition                                    | Result                                                                                                       |
|----------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| Field is missing from the document           | `$elemMatch` evaluates to false                                                                              |
| Field is `null`                              | `$elemMatch` evaluates to false                                                                              |
| Field is not an array (string, number, etc.) | `$elemMatch` evaluates to false                                                                              |
| Field is an empty array `[]`                 | `$elemMatch` evaluates to false                                                                              |
| Array contains `null` elements               | `null` elements are evaluated normally. `$eq: null` matches them. Comparison operators like `$gt` skip them. |
| Single-element array                         | Normal evaluation. The single element must satisfy all conditions.                                           |

## Supported operators

Quick reference for all operators supported inside `$elemMatch`:

| Operator     | Applies to             | Example inside `$elemMatch`                        |
|--------------|------------------------|----------------------------------------------------|
| `$eq`        | scalar, document field | `{ "$eq": 80 }`                                    |
| `$ne`        | scalar, document field | `{ "$ne": "cancelled" }`                           |
| `$gt`        | scalar, document field | `{ "$gt": 100 }`                                   |
| `$gte`       | scalar, document field | `{ "$gte": 50 }`                                   |
| `$lt`        | scalar, document field | `{ "$lt": 90 }`                                    |
| `$lte`       | scalar, document field | `{ "$lte": 70 }`                                   |
| `$in`        | scalar, document field | `{ "$in": [22, 60, 95] }`                          |
| `$nin`       | scalar, document field | `{ "$nin": ["cancelled", "refunded"] }`            |
| `$all`       | document field (array) | `{ "tags": { "$all": ["sale", "new"] } }`          |
| `$size`      | document field (array) | `{ "tags": { "$size": 3 } }`                       |
| `$exists`    | document field         | `{ "discount": { "$exists": true } }`              |
| `$and`       | scalar, document       | `{ "$and": [{ "price": { "$gt": 10 } }, ...] }`    |
| `$or`        | scalar, document       | `{ "$or": [{ "$gt": 90 }, { "$lt": 25 }] }`        |
| `$not`       | scalar, document field | `{ "status": { "$not": { "$eq": "cancelled" } } }` |
| `$elemMatch` | document field (array) | Nested `$elemMatch` for arrays within arrays       |

`$in`, `$nin`, and `$all` also accept regular expression literals as elements, matched with `$regex` semantics. See
[$regex](regex.md).
