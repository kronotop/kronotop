---
title: "BUCKET.INDEX"
sidebar:
  order: 10
description: "Manages indexes on bucket fields."
---

Manages indexes on bucket fields. Indexes accelerate queries by allowing efficient lookups on specific fields.

## Subcommands

| Subcommand | Description                                     |
|------------|-------------------------------------------------|
| `CREATE`   | Create a new index on one or more fields.       |
| `LIST`     | List all indexes on a bucket.                   |
| `DESCRIBE` | Get detailed information about an index.        |
| `DROP`     | Drop an existing index.                         |
| `TASKS`    | List background maintenance tasks for an index. |
| `ANALYZE`  | Trigger index statistics analysis.              |

---

## BUCKET.INDEX CREATE

Creates one or more indexes on bucket fields.

### Syntax

```kronotop
BUCKET.INDEX CREATE <bucket> <schema>
```

### Parameters

| Parameter | Type   | Required | Description                                               |
|-----------|--------|----------|-----------------------------------------------------------|
| `bucket`  | string | Yes      | Name of the target bucket. The bucket must already exist. |
| `schema`  | JSON   | Yes      | Index schema defining the fields to index.                |

### Index Schema Format

The schema is a JSON object that can contain single-field indexes, compound indexes, or both:

```
{
  "<field>": { "bson_type": "<type>" [, "multi_key": true] [, "name": "<name>"] },
  "$compound": [ { "fields": [ { "selector": "<field>", "bson_type": "<type>" }, ... ] [, "name": "<name>"] } ]
}
```

#### Field selectors

Field selectors use dot notation to address nested fields inside documents. Arrays are traversed automatically. When a
selector crosses an array, each element is evaluated independently.

| Selector       | Targets                                                    |
|----------------|------------------------------------------------------------|
| `name`         | Top-level field `name`.                                    |
| `address.city` | `city` inside the nested object `address`.                 |
| `tags`         | The `tags` field itself (use with `multi_key` for arrays). |
| `orders.total` | `total` inside each element of the `orders` array.         |

**Example document:**

```json
{
  "username": "alice",
  "address": { "city": "Istanbul", "zip": "34000" },
  "tags": ["admin", "editor"],
  "orders": [
    { "total": 120, "status": "shipped" },
    { "total": 45, "status": "pending" }
  ]
}
```

- Selector `address.city` reaches `"Istanbul"`.
- Selector `tags` with `multi_key: true` indexes each element (`"admin"`, `"editor"`) separately.
- Selector `orders.total` with `multi_key: true` indexes each order's total (`120`, `45`) separately.

#### Single-field indexes

Each top-level key (other than `$compound`) is a field selector. The value defines the index properties:

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
| `bson_type` | string  | Yes      | The BSON type of the field values. See [Supported BSON Types](#supported-bson-types).                                           |
| `multi_key` | boolean | No       | When `true`, creates a multi-key index for array fields. Each array element generates a separate index entry. Default: `false`. |
| `name`      | string  | No       | Custom name for the index. If omitted, a name is auto-generated from the selector and type.                                     |
| `collation` | object  | No       | Collation spec for locale-aware string ordering. Only valid for `string` type. See [Collation](../collation.md).                |

#### Compound indexes

The `$compound` key holds an array of compound index definitions. Each definition specifies an ordered list of fields:

```json
{
  "$compound": [
    {
      "name": "optional_custom_name",
      "fields": [
        { "selector": "field_a", "bson_type": "string" },
        { "selector": "field_b", "bson_type": "int32", "multi_key": false }
      ]
    }
  ]
}
```

Each field in the `fields` array supports:

| Property    | Type    | Required | Description                                                                           |
|-------------|---------|----------|---------------------------------------------------------------------------------------|
| `selector`  | string  | Yes      | Dot-notation path to the field.                                                       |
| `bson_type` | string  | Yes      | The BSON type of the field values. See [Supported BSON Types](#supported-bson-types). |
| `multi_key` | boolean | No       | When `true`, creates a multi-key index entry per array element. Default: `false`.     |

The compound index definition also supports:

| Property    | Type   | Required | Description                                                                                                              |
|-------------|--------|----------|--------------------------------------------------------------------------------------------------------------------------|
| `name`      | string | No       | Custom name for the compound index. If omitted, a name is auto-generated.                                                |
| `collation` | object | No       | Collation spec for locale-aware string ordering. Requires at least one `string` field. See [Collation](../collation.md). |

**Constraints:**

- A compound index must have at least two fields.
- A compound index supports at most 32 fields.
- At most one field can have `multi_key` enabled.
- Each field selector must appear exactly once within a compound index.

See [Compound Indexes](../compound-index.md) for detailed rules including the prefix rule and supported operators.

### Multi-key Index Behavior

When `multi_key` is set to `true`, the index is designed for array fields. Each element in the array creates a separate
index entry, allowing queries to match documents where any array element satisfies the condition.

**Limitations:**

- **Undefined ordering**: Result ordering is undefined with multi-key indexes. Since each document can have multiple
  index entries (one per array element), the order in which documents are returned cannot be guaranteed.
- **Index size**: Multi-key indexes can be significantly larger than regular indexes because each array element creates
  a separate index entry.
- **Type matching**: Only array elements matching the specified `bson_type` are indexed.

### Return Value

Returns `OK` on success.

### Errors

| Error Code           | Description                          |
|----------------------|--------------------------------------|
| `ERR`                | The index already exists.            |
| `ERR`                | The schema is invalid.               |
| `ERR`                | Unknown BSON type.                   |
| `NOSUCHBUCKET`       | The specified bucket does not exist. |
| `BUCKETBEINGREMOVED` | The target bucket is being removed.  |

### Examples

**Create a single-field index:**

```kronotop
> BUCKET.INDEX CREATE users '{"username": {"bson_type": "string"}}'
OK
```

**Create multiple indexes at once:**

```kronotop
> BUCKET.INDEX CREATE users '{"age": {"bson_type": "int32"}, "email": {"bson_type": "string"}}'
OK
```

**Create an index with a custom name:**

```kronotop
> BUCKET.INDEX CREATE users '{"username": {"bson_type": "string", "name": "idx_username"}}'
OK
```

**Create a multi-key index for array fields:**

```kronotop
> BUCKET.INDEX CREATE products '{"tags": {"bson_type": "string", "multi_key": true}}'
OK
```

**Index a nested field using dot notation:**

```kronotop
> BUCKET.INDEX CREATE users '{"address.city": {"bson_type": "string"}}'
OK
```

**Index elements inside an array of objects:**

```kronotop
> BUCKET.INDEX CREATE users '{"orders.total": {"bson_type": "int32", "multi_key": true}}'
OK
```

**Create a single-field index with collation:**

```kronotop
> BUCKET.INDEX CREATE users '{
  "username": {
    "bson_type": "string",
    "collation": {"locale": "tr", "strength": 2}
  }
}'
OK
```

**Create a compound index:**

```kronotop
> BUCKET.INDEX CREATE products '{
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

**Create a compound index with collation:**

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

**Create single-field and compound indexes together:**

```kronotop
> BUCKET.INDEX CREATE products '{
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

---

## BUCKET.INDEX LIST

Lists all indexes defined on a bucket.

### Syntax

```kronotop
BUCKET.INDEX LIST <bucket>
```

### Parameters

| Parameter | Type   | Required | Description         |
|-----------|--------|----------|---------------------|
| `bucket`  | string | Yes      | Name of the bucket. |

### Return Value

Returns an array of index names.

### Errors

| Error Code           | Description                          |
|----------------------|--------------------------------------|
| `NOSUCHBUCKET`       | The specified bucket does not exist. |
| `BUCKETBEINGREMOVED` | The bucket is being removed.         |

### Examples

```kronotop
> BUCKET.INDEX LIST users
1) "primary-index"
2) "selector:username.bsonType:STRING"
3) "selector:age.bsonType:INT32"
```

---

## BUCKET.INDEX DESCRIBE

Gets detailed information about a specific index.

### Syntax

```kronotop
BUCKET.INDEX DESCRIBE <bucket> <index>
```

### Parameters

| Parameter | Type   | Required | Description                    |
|-----------|--------|----------|--------------------------------|
| `bucket`  | string | Yes      | Name of the bucket.            |
| `index`   | string | Yes      | Name of the index to describe. |

### Return Value

Returns a map with the following fields:

| Field        | Type    | Description                                                                                                                            |
|--------------|---------|----------------------------------------------------------------------------------------------------------------------------------------|
| `index_type` | string  | Kind of the index: `single_field`, `compound`, or `vector`.                                                                            |
| `id`         | integer | Index identifier.                                                                                                                      |
| `selector`   | string  | The field selector the index is built on.                                                                                              |
| `bson_type`  | string  | The BSON type of indexed values.                                                                                                       |
| `status`     | string  | Current index status. See [Index Lifecycle](#index-lifecycle).                                                                         |
| `collation`  | map     | Collation configuration (see below). All values are null when no collation is set. Only present for single-field and compound indexes. |
| `statistics` | map     | Index statistics including `cardinality`.                                                                                              |

#### Collation sub-fields

| Field              | Type    | Description                                                                       |
|--------------------|---------|-----------------------------------------------------------------------------------|
| `locale`           | string  | ICU locale identifier (e.g., `"en"`, `"tr"`).                                     |
| `strength`         | integer | Comparison strength level (1-5).                                                  |
| `case_level`       | boolean | Whether to include case-level comparisons.                                        |
| `case_first`       | string  | Sort order of case differences (`"upper"`, `"lower"`, `"off"`).                   |
| `numeric_ordering` | boolean | Whether to compare numeric strings as numbers.                                    |
| `alternate`        | string  | Handling of variable-weight characters (`"non-ignorable"`, `"shifted"`).          |
| `backwards`        | boolean | Whether to reverse secondary-level comparisons (for French).                      |
| `normalization`    | boolean | Whether to perform Unicode normalization.                                         |
| `max_variable`     | string  | Which characters are ignorable when `alternate="shifted"` (`"punct"`, `"space"`). |

### Errors

| Error Code           | Description                          |
|----------------------|--------------------------------------|
| `NOSUCHBUCKET`       | The specified bucket does not exist. |
| `NOSUCHINDEX`        | The specified index does not exist.  |
| `BUCKETBEINGREMOVED` | The bucket is being removed.         |

### Examples

```kronotop
> BUCKET.INDEX DESCRIBE users "selector:username.bsonType:STRING"
index_type -> "single_field"
id -> 2
selector -> "username"
bson_type -> "STRING"
status -> "WAITING"
collation -> {locale -> (nil), strength -> (nil), case_level -> (nil), case_first -> (nil), numeric_ordering -> (nil), alternate -> (nil), backwards -> (nil), normalization -> (nil), max_variable -> (nil)}
statistics -> {cardinality -> 0}
```

---

## BUCKET.INDEX DROP

Drops an existing index from a bucket.

### Syntax

```kronotop
BUCKET.INDEX DROP <bucket> <index>
```

### Parameters

| Parameter | Type   | Required | Description                |
|-----------|--------|----------|----------------------------|
| `bucket`  | string | Yes      | Name of the bucket.        |
| `index`   | string | Yes      | Name of the index to drop. |

### Return Value

Returns `OK` on success. The index is marked as `DROPPED` and a background task is created to clean up the index data.

### Errors

| Error Code           | Description                                      |
|----------------------|--------------------------------------------------|
| `ERR`                | Cannot drop the primary index (`primary-index`). |
| `ERR`                | The index is already in the `DROPPED` status.    |
| `ERR`                | The index has active tasks.                      |
| `NOSUCHBUCKET`       | The specified bucket does not exist.             |
| `NOSUCHINDEX`        | The specified index does not exist.              |
| `BUCKETBEINGREMOVED` | The bucket is being removed.                     |

### Examples

```kronotop
> BUCKET.INDEX DROP users "selector:username.bsonType:STRING"
OK
```

**Attempting to drop the primary index:**

```kronotop
> BUCKET.INDEX DROP users "primary-index"
(error) ERR Cannot drop the primary index
```

---

## BUCKET.INDEX TASKS

Lists background maintenance tasks associated with an index.

### Syntax

```kronotop
BUCKET.INDEX TASKS <bucket> <index>
```

### Parameters

| Parameter | Type   | Required | Description         |
|-----------|--------|----------|---------------------|
| `bucket`  | string | Yes      | Name of the bucket. |
| `index`   | string | Yes      | Name of the index.  |

### Return Value

Returns a map where each key is a task ID and the value contains task details:

**For BUILD tasks:**

| Field    | Type   | Description                            |
|----------|--------|----------------------------------------|
| `kind`   | string | Task type (`BUILD`).                   |
| `cursor` | string | Current position in the build process. |
| `lower`  | string | Lower bound position.                  |
| `upper`  | string | Upper bound position.                  |
| `status` | string | Task status.                           |
| `error`  | string | Error message if failed.               |

**For DROP tasks:**

| Field    | Type   | Description              |
|----------|--------|--------------------------|
| `kind`   | string | Task type (`DROP`).      |
| `status` | string | Task status.             |
| `error`  | string | Error message if failed. |

**For BOUNDARY tasks:**

| Field    | Type   | Description              |
|----------|--------|--------------------------|
| `kind`   | string | Task type (`BOUNDARY`).  |
| `status` | string | Task status.             |
| `error`  | string | Error message if failed. |

**For ANALYZE tasks:**

| Field    | Type   | Description              |
|----------|--------|--------------------------|
| `kind`   | string | Task type (`ANALYZE`).   |
| `status` | string | Task status.             |
| `error`  | string | Error message if failed. |

### Errors

| Error Code           | Description                          |
|----------------------|--------------------------------------|
| `NOSUCHBUCKET`       | The specified bucket does not exist. |
| `BUCKETBEINGREMOVED` | The bucket is being removed.         |

### Examples

```kronotop
> BUCKET.INDEX TASKS users "selector:username.bsonType:STRING"
"5K8G4R000000000000000000" -> {
  kind -> "BUILD"
  cursor -> "5K8G4R000000000000000500"
  lower -> "0000000000000000000000000"
  upper -> "5K8G4R000000000000001000"
  status -> "RUNNING"
  error -> ""
}
```

---

## BUCKET.INDEX ANALYZE

Trigger index statistics analysis. Statistics help the query optimizer make better decisions.

### Syntax

```kronotop
BUCKET.INDEX ANALYZE <bucket> <index>
```

### Parameters

| Parameter | Type   | Required | Description                   |
|-----------|--------|----------|-------------------------------|
| `bucket`  | string | Yes      | Name of the bucket.           |
| `index`   | string | Yes      | Name of the index to analyze. |

### Return Value

Returns `OK` on success. A background task is created to compute index statistics.

### Errors

| Error Code           | Description                                                                                       |
|----------------------|---------------------------------------------------------------------------------------------------|
| `ERR`                | An analysis task already exists for this index.                                                   |
| `ERR`                | The index is not in the `READY` state. Only indexes that have completed building can be analyzed. |
| `NOSUCHBUCKET`       | The specified bucket does not exist.                                                              |
| `NOSUCHINDEX`        | The specified index does not exist.                                                               |
| `BUCKETBEINGREMOVED` | The bucket is being removed.                                                                      |

### Examples

```kronotop
> BUCKET.INDEX ANALYZE users "selector:username.bsonType:STRING"
OK
```

---

## Index Lifecycle

Indexes go through the following states:

| Status     | Description                                                      |
|------------|------------------------------------------------------------------|
| `WAITING`  | Index is created but background building has not started yet.    |
| `BUILDING` | Index is being built by a background task.                       |
| `READY`    | Index is fully built and available for queries.                  |
| `DROPPED`  | Index is marked for deletion; background cleanup is in progress. |
| `FAILED`   | Index building failed due to an error.                           |

---

## Supported BSON Types

The following BSON types can be indexed:

| Type         | Description                                                           |
|--------------|-----------------------------------------------------------------------|
| `string`     | UTF-8 string values.                                                  |
| `int32`      | 32-bit signed integers.                                               |
| `int64`      | 64-bit signed integers.                                               |
| `double`     | 64-bit IEEE 754 floating point.                                       |
| `boolean`    | Boolean values (`true` / `false`).                                    |
| `datetime`   | UTC datetime (milliseconds since Unix epoch).                         |
| `timestamp`  | Internal timestamp type.                                              |
| `binary`     | Binary data.                                                          |
| `objectid`   | ObjectId values.                                                      |
| `decimal128` | 128-bit decimal floating point. Not yet fully supported for indexing. |

---
