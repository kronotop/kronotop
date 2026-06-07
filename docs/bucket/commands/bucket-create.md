---
title: "BUCKET.CREATE"
sidebar:
  order: 1
description: "Creates a new bucket with optional shard assignment and index definitions."
---

Creates a new bucket with optional shard assignment and index definitions.

## Syntax

```kronotop
BUCKET.CREATE <bucket> [SHARDS <shard-id> [shard-id ...]] [INDEXES <json-schema>] [COLLATION <json-spec>] [IF-NOT-EXISTS]
```

## Parameters

| Parameter       | Type       | Required | Description                                                                                     |
|-----------------|------------|----------|-------------------------------------------------------------------------------------------------|
| `bucket`        | string     | Yes      | Name of the bucket to create.                                                                   |
| `SHARDS`        | integer(s) | No       | One or more shard IDs to assign the bucket to. If omitted, a shard is selected automatically.   |
| `INDEXES`       | JSON       | No       | Index schema defining secondary indexes to create alongside the bucket.                         |
| `COLLATION`     | JSON       | No       | Bucket-level collation spec for locale-aware string ordering. See [Collation](../collation.md). |
| `IF-NOT-EXISTS` | flag       | No       | When specified, the command returns `OK` instead of an error if the bucket already exists.      |

## Return Value

Returns `OK` on success.

## Shard Assignment

When the `SHARDS` parameter is omitted, Kronotop automatically assigns the bucket to a shard using round-robin selection
across available shards. This ensures even distribution of buckets across the cluster. When multiple shard IDs are
provided,
the bucket is assigned to all specified shards.

Regardless of how shards are selected, every shard ID is validated before the bucket is created: the shard must have a
known route. If a shard has no route, the command returns an error and the bucket is not created. A bucket can span
shards
across multiple nodes.

## Index Schema Format

The `INDEXES` parameter accepts a JSON object that can contain single-field indexes, compound indexes, or both:

```
{
  "<field>": { "bson_type": "<type>" [, "multi_key": true] [, "name": "<name>"] },
  "$compound": [ { "fields": [ { "selector": "<field>", "bson_type": "<type>" }, ... ] [, "name": "<name>"] } ]
}
```

### Field selectors

Field selectors use dot notation to address nested fields inside documents. Arrays are traversed automatically. When a
selector crosses an array, each element is evaluated independently.

| Selector       | Targets                                                    |
|----------------|------------------------------------------------------------|
| `name`         | Top-level field `name`.                                    |
| `address.city` | `city` inside the nested object `address`.                 |
| `tags`         | The `tags` field itself (use with `multi_key` for arrays). |
| `orders.total` | `total` inside each element of the `orders` array.         |

See [BUCKET.INDEX CREATE](bucket-index.md#bucketindex-create) for detailed examples of dot notation and array traversal.

### Single-field indexes

Each top-level key (other than `$compound`) is a field selector (dot-notation path). The value defines the index
properties:

```json
{
  "field_name": {
    "bson_type": "type",
    "multi_key": true|false,
    "name": "optional_custom_name"
  }
}
```

| Property    | Type    | Required | Description                                                                                                                     |
|-------------|---------|----------|---------------------------------------------------------------------------------------------------------------------------------|
| `bson_type` | string  | Yes      | The BSON type of the field values. See [Supported BSON Types](bucket-index.md#supported-bson-types).                            |
| `multi_key` | boolean | No       | When `true`, creates a multi-key index for array fields. Each array element generates a separate index entry. Default: `false`. |
| `name`      | string  | No       | Custom name for the index. If omitted, a name is auto-generated from the selector and type.                                     |
| `collation` | object  | No       | Collation spec for locale-aware string ordering. Only valid for `string` type. See [Collation](../collation.md).                |

### Compound indexes

The `$compound` key holds an array of compound index definitions. Each definition specifies an ordered list of fields:

```json
{
  "$compound": [
    {
      "name": "optional_custom_name",
      "fields": [
        { "selector": "field_a", "bson_type": "string" },
        { "selector": "field_b", "bson_type": "int32" }
      ]
    }
  ]
}
```

| Property    | Type   | Required | Description                                                                                                              |
|-------------|--------|----------|--------------------------------------------------------------------------------------------------------------------------|
| `name`      | string | No       | Custom name for the compound index. If omitted, a name is auto-generated.                                                |
| `collation` | object | No       | Collation spec for locale-aware string ordering. Requires at least one `string` field. See [Collation](../collation.md). |

Each field in the `fields` array supports `selector` (required), `bson_type` (required), and `multi_key` (optional,
default `false`).

**Constraints:** A compound index must have at least two fields, at most one field can have `multi_key` enabled, and
each selector must appear exactly once. See [Compound Indexes](../compound-index.md) for detailed rules.

Every bucket automatically includes a primary index. The indexes defined here are secondary indexes created in the same
transaction as the bucket itself.

## Errors

| Error Code            | Description                                                                                |
|-----------------------|--------------------------------------------------------------------------------------------|
| `BUCKETALREADYEXISTS` | A bucket with the same name already exists (suppressed when `IF-NOT-EXISTS` is specified). |
| `ERR`                 | Invalid index schema (e.g., missing or unknown `bson_type`).                               |
| `BUCKETBEINGREMOVED`  | A bucket with the same name was previously removed and has not yet been purged.            |

## Examples

**Create a bucket with the default shard assignment:**

```kronotop
> BUCKET.CREATE users
OK
```

**Create a bucket on specific shards:**

```kronotop
> BUCKET.CREATE users SHARDS 0 1
OK
```

**Create a bucket with secondary indexes:**

```kronotop
> BUCKET.CREATE users INDEXES '{"username": {"bson_type": "string"}, "age": {"bson_type": "int32"}}'
OK
```

**Create a bucket on specific shards with indexes:**

```kronotop
> BUCKET.CREATE users SHARDS 0 1 INDEXES '{
  "username": {"bson_type": "string"},
  "age": {"bson_type": "int32"}
}'
OK
```

**Create a bucket with a named multi-key index:**

```kronotop
> BUCKET.CREATE products INDEXES '{
  "tags": {"bson_type": "string", "multi_key": true, "name": "idx_tags"}
}'
OK
```

**Create a bucket with a collated string index:**

```kronotop
> BUCKET.CREATE users INDEXES '{
  "username": {"bson_type": "string", "collation": {"locale": "tr", "strength": 2}}
}'
OK
```

**Create a bucket with bucket-level collation:**

```kronotop
> BUCKET.CREATE users COLLATION '{"locale": "en", "strength": 2}'
OK
```

All string indexes in this bucket will use case-insensitive English collation by default, unless overridden at the index
level.

**Create a bucket with bucket-level collation and indexes:**

```kronotop
> BUCKET.CREATE products SHARDS 0 1 COLLATION '{"locale": "tr"}' INDEXES '{"name": {"bson_type": "string"}}'
OK
```

The `name` index inherits the bucket's Turkish collation.

**Create a bucket with a compound index:**

```kronotop
> BUCKET.CREATE products INDEXES '{
  "$compound": [{
    "name": "idx_cat_price",
    "fields": [
      {"selector": "category", "bson_type": "string"},
      {"selector": "price", "bson_type": "double"}
    ]
  }]
}'
OK
```

**Create a bucket with single-field and compound indexes:**

```kronotop
> BUCKET.CREATE products INDEXES '{
  "email": {"bson_type": "string"},
  "$compound": [{
    "fields": [
      {"selector": "status", "bson_type": "string"},
      {"selector": "region", "bson_type": "string"}
    ]
  }]
}'
OK
```

**Idempotent creation:**

```kronotop
> BUCKET.CREATE users IF-NOT-EXISTS
OK

> BUCKET.CREATE users IF-NOT-EXISTS
OK
```

**Attempting to create a bucket that already exists:**

```kronotop
> BUCKET.CREATE users
OK

> BUCKET.CREATE users
(error) BUCKETALREADYEXISTS Bucket already exists: users
```
