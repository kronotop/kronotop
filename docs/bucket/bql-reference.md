---
title: "BQL Language Reference"
description: "BQL (Bucket Query Language) is the query language of the Bucket data structure, used for document operations."
---

BQL (Bucket Query Language) is the query language of the Bucket data structure, used for document operations. Queries
are written in JSON or BSON format.

## Query Syntax

Queries are JSON objects that specify conditions for matching documents.

### Basic Equality

```json
{ "status": "active" }
{ "age": 25 }
{ "verified": true }
```

### Empty Query (Match All)

```json
{}
```

## Comparison Operators

| Operator | Description           | Example                              |
|----------|-----------------------|--------------------------------------|
| `$eq`    | Equal to              | `{ "age": { "$eq": 25 } }`           |
| `$ne`    | Not equal to          | `{ "status": { "$ne": "deleted" } }` |
| `$gt`    | Greater than          | `{ "age": { "$gt": 18 } }`           |
| `$gte`   | Greater than or equal | `{ "price": { "$gte": 10 } }`        |
| `$lt`    | Less than             | `{ "age": { "$lt": 65 } }`           |
| `$lte`   | Less than or equal    | `{ "price": { "$lte": 100 } }`       |

### Range Queries

Multiple operators on the same field are implicitly combined with AND:

```json
{ "age": { "$gte": 18, "$lt": 65 } }
{ "price": { "$gt": 10, "$lte": 50, "$ne": 25 } }
```

## Array Operators

| Operator     | Description                     | Example                                             |
|--------------|---------------------------------|-----------------------------------------------------|
| `$in`        | Value in array                  | `{ "status": { "$in": ["active", "pending"] } }`    |
| `$nin`       | Value not in array              | `{ "status": { "$nin": ["deleted", "archived"] } }` |
| `$all`       | Array contains all values       | `{ "tags": { "$all": ["urgent", "bug"] } }`         |
| `$size`      | Array has specific length       | `{ "items": { "$size": 3 } }`                       |
| `$elemMatch` | Array element matches condition | `{ "scores": { "$elemMatch": { "$gte": 80 } } }`    |

### $elemMatch Examples

Scalar array: match elements where scores >= 80 AND < 90:

```json
{ "scores": { "$elemMatch": { "$gte": 80, "$lt": 90 } } }
```

Object array: match an item with product="xyz" AND score >= 8:

```json
{ "items": { "$elemMatch": { "product": "xyz", "score": { "$gte": 8 } } } }
```

## Field Operators

| Operator  | Description         | Example                            |
|-----------|---------------------|------------------------------------|
| `$exists` | Field exists or not | `{ "email": { "$exists": true } }` |

Field must exist:

```json
{ "phone": { "$exists": true } }
```

Field must not exist:

```json
{ "deletedAt": { "$exists": false } }
```

## Logical Operators

### $and

Explicit AND of conditions:

```json
{ "$and": [
    { "status": "active" },
    { "age": { "$gte": 18 } }
] }
```

Multiple fields in the same object are implicitly AND:

```json
{ "status": "active", "age": { "$gte": 18 } }
```

### $or

Match any condition:

```json
{ "$or": [
    { "status": "active" },
    { "status": "pending" }
] }
```

### $not

Negate a condition:

```json
{ "price": { "$not": { "$gt": 100 } } }
```

### $nor

Match none of the conditions (equivalent to `$not` + `$or`):

```json
{ "$nor": [
    { "status": "deleted" },
    { "status": "archived" }
] }
```

### Combined Logical Operators

```json
{
    "$and": [
        { "$or": [
            { "status": "active" },
            { "priority": "high" }
        ] },
        { "age": { "$gte": 18 } }
    ]
}
```

## Null Values

Querying for null values:

```json
{ "middleName": { "$eq": null } }
{ "deletedAt": null }
```

## Supported Data Types

| Type         | Example                      | Description                                                       |
|--------------|------------------------------|-------------------------------------------------------------------|
| String       | `"Alice"`                    | UTF-8 string                                                      |
| Int32        | `25`                         | 32-bit integer                                                    |
| Int64        | `9223372036854775807`        | 64-bit integer                                                    |
| Double       | `19.99`                      | 64-bit floating point                                             |
| Decimal128   | `"123.456"`                  | 128-bit decimal                                                   |
| Boolean      | `true`, `false`              | Boolean value                                                     |
| Null         | `null`                       | Null value                                                        |
| DateTime     | BSON DateTime                | Date and time                                                     |
| Timestamp    | BSON Timestamp               | Timestamp                                                         |
| Binary       | BSON Binary                  | Binary data                                                       |
| ObjectId     | `"6835a1c0e4b0f72a3c000001"` | 12-byte unique identifier (auto-detected from 24-char hex string) |
| Versionstamp | BSON Binary (12 bytes)       | FoundationDB Versionstamp                                         |
| Array        | `[1, 2, 3]`                  | Array of values                                                   |
| Document     | `{ "nested": "value" }`      | Nested document                                                   |

## Nested Field Access

Use dot notation for nested fields:

```json
{ "user.address.city": "Istanbul" }
{ "metadata.version": { "$gte": 2 } }
```

## Examples

### Find active users over 18

```json
{ "status": "active", "age": { "$gt": 18 } }
```

### Find orders with specific statuses

```json
{ "orderStatus": { "$in": ["shipped", "delivered"] } }
```

### Find products in the price range

```json
{ "price": { "$gte": 10, "$lte": 100 } }
```

### Find users with verified email

```json
{ "email": { "$exists": true }, "emailVerified": true }
```

### Find documents with tags containing both "urgent" and "bug"

```json
{ "tags": { "$all": ["urgent", "bug"] } }
```

### Complex query with OR and AND

```json
{
    "$or": [
        { "priority": "high", "status": "open" },
        { "dueDate": { "$lt": "2025-01-01" } }
    ]
}
```

## Input/Output Formats

BQL accepts queries in both JSON and BSON formats. Use the `SESSION.ATTRIBUTE` command to configure:

- `input_type`: `json` or `bson`
- `reply_type`: `json` or `bson`

## Type Matching

The query engine enforces strict type matching during predicate evaluation. This behavior is always active and not
configurable.

Non-numeric types (`STRING`, `BOOLEAN`, `DATETIME`, etc.) are strictly separated: a type mismatch always evaluates to
`false`. For numeric types (`INT32`, `INT64`, `DOUBLE`, `DECIMAL128`), lossless numeric widening is supported. An
`INT32` predicate can match an `INT64` value, but a `STRING` predicate never matches an `INT32` value.

### Example

INT32 predicate matches INT32 and INT64 values:

```json
{ "age": { "$eq": 25 } }
```

STRING predicate never matches INT32 values:

```json
{ "age": { "$eq": "25" } }
```

For the full widening rules, common type resolution, and index-side type enforcement (`bucket.index.strict_types`),
see [Strict Types](strict-types.md).

## Limitations

- Decimal128 indexing is not yet supported
- Full-text search is not supported
- Geospatial queries are not supported
- Aggregation pipeline is planned for future releases
