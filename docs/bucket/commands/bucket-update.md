---
title: "BUCKET.UPDATE"
sidebar:
  order: 5
description: "Updates documents in a bucket that match a filter expression."
---

Updates documents in a bucket that match a filter expression.

## Syntax

```kronotop
BUCKET.UPDATE <bucket> <query> <update> [SORTBY <field> <ASC|DESC>] [LIMIT <n>] [COLLATION <json-spec>]
```

## Parameters

| Parameter   | Type               | Required | Description                                                                                              |
|-------------|--------------------|----------|----------------------------------------------------------------------------------------------------------|
| `bucket`    | string             | Yes      | Name of the bucket to update.                                                                            |
| `query`     | JSON or BSON       | Yes      | Filter expression to match documents. Use `{}` to match all documents.                                   |
| `update`    | JSON or BSON       | Yes      | Update document with update operators. Cannot be empty.                                                  |
| `SORTBY`    | string + direction | No       | Process documents in sorted order. Requires field name followed by `ASC` or `DESC`.                      |
| `LIMIT`     | integer            | No       | Maximum number of documents to update per batch. Must be non-negative.                                   |
| `COLLATION` | JSON               | No       | Query-level collation spec for locale-aware string comparison. Overrides index collation for this query. |

## Return Value

The command returns a cursor ID and an array of ObjectIds for updated documents. The format depends on the protocol
version.

An ObjectId is a 12-byte unique identifier. The encoding format of returned ObjectIds depends on the session's
`object_id_format` setting:

| Format  | Response Type | Description                               |
|---------|---------------|-------------------------------------------|
| `hex`   | String        | 24-character hex-encoded string (default) |
| `bytes` | Binary        | Raw 12-byte array                         |

To change the format:

```kronotop
SESSION.ATTRIBUTE SET object_id_format hex
SESSION.ATTRIBUTE SET object_id_format bytes
```

**RESP3 (map format):**

```kronotop
1# "cursor_id" => (integer) <cursor-id>
2# "object_ids" =>  1) "6835a1c0e4b0f72a3c000001"
                    2) "6835a1c0e4b0f72a3c000002"
                    ...
```

**RESP2 (array format):**

```kronotop
1) (integer) <cursor-id>
2) 1) "6835a1c0e4b0f72a3c000001"
   2) "6835a1c0e4b0f72a3c000002"
   ...
```

When no documents match the filter, the `object_ids` array is empty.

The `object_ids` array contains only the documents that were actually modified. A matched document whose update would
leave its content unchanged is not modified and does not appear in `object_ids`.

**Auto-commit mode (default):**

The update is committed immediately.

**Transaction mode (within BEGIN/COMMIT):**

The update is not committed until `COMMIT` is called. Use `ROLLBACK` to cancel the update operation.

## Update Operators

The `update` parameter accepts a JSON or BSON document containing one or more of the following operators:

**`$set`**: Sets field values on matched documents.

```kronotop
'{"$set": {"status": "active", "version": 2}}'
```

The `_id` field is immutable and cannot be modified with `$set`.

**`$unset`**: Removes fields from matched documents. Accepts either an array of field names or a document with field
names as keys:

```kronotop
'{"$unset": ["temporary_field", "deprecated_field"]}'
'{"$unset": {"temporary_field": 1, "deprecated_field": 1}}'
```

**`array_filters`**: Applies conditional updates to array elements using positional operators. Each filter is a document
with an identifier and a condition.

Supported filter operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$all`, `$size`, `$exists`.

```kronotop
'{"$set": {"grades.$[elem].score": 100}, "array_filters": [{"elem": {"$gte": 8}}]}'
```

Positional operators target array elements. If the target field is missing or is not an array, the document
is left unchanged.

**`upsert`**: Boolean. When `true`, inserts a new document if no documents match the filter. Positional operators cannot
be used with upsert.

```kronotop
'{"$set": {"status": "active"}, "upsert": true}'
```

After an update, all indexes whose selectors overlap the modified field paths are synchronized with the new document
content.

## Pagination

When using `LIMIT`, use the cursor ID with `BUCKET.ADVANCE` to update more matching documents:

```kronotop
BUCKET.ADVANCE UPDATE <cursor-id>
```

Each call updates the next batch of documents up to the limit.

## Routing

The command must be sent to a node that owns at least one shard assigned to the bucket. If the bucket's shards are all
hosted on other nodes, the server rejects the request with a redirect to the appropriate node.

## Errors

| Error Code              | Description                                                                                                              |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `REJECT`                | The bucket's shards are hosted on another node. The error includes the target address: `REJECT <shardId> <host>:<port>`. |
| `BUCKETBEINGREMOVED`    | The bucket is being removed.                                                                                             |
| `NOSUCHBUCKET`          | The bucket does not exist.                                                                                               |
| `NOSUCHNAMESPACE`       | The namespace does not exist.                                                                                            |
| `NAMESPACEBEINGREMOVED` | The namespace is being removed.                                                                                          |
| `INDEXTYPE_MISMATCH`    | The updated value type does not match the expected index type.                                                           |
| `DUPLICATEKEY`          | Duplicate `_id` encountered during upsert.                                                                               |
| `VECTORINDEXNOTREADY`   | A vector index on the bucket is still bootstrapping. Retry after a short delay.                                          |
| `ERR`                   | Update parameter cannot be empty.                                                                                        |

## Examples

**Update with $set:**

```kronotop
BUCKET.UPDATE users '{"status": "pending"}' '{"$set": {"status": "active"}}'
```

**Update with $unset:**

```kronotop
BUCKET.UPDATE users '{"age": {"$gt": 30}}' '{"$unset": ["temporary_field", "deprecated_field"]}'
```

**Update with $set and $unset:**

```kronotop
BUCKET.UPDATE users '{}' '{"$set": {"version": 2}, "$unset": ["old_field"]}'
```

**Update with array_filters:**

```kronotop
BUCKET.UPDATE students '{}' '{"$set": {"grades.$[elem].passed": true}, "array_filters": [{"elem": {"$gte": 60}}]}'
```

**Update with upsert:**

```kronotop
BUCKET.UPDATE users '{"username": "alice"}' '{"$set": {"username": "alice", "status": "active"}, "upsert": true}'
```

**Update with sorting:**

```kronotop
BUCKET.UPDATE users '{"status": "pending"}' '{"$set": {"status": "active"}}' SORTBY created_at ASC
```

**Update with limit:**

```kronotop
BUCKET.UPDATE users '{"status": "pending"}' '{"$set": {"status": "active"}}' LIMIT 50
```

**Update with collation:**

```kronotop
BUCKET.UPDATE users '{"name": "alice"}' '{"$set": {"verified": true}}' COLLATION '{"locale": "en", "strength": 2}'
```

Updates documents matching `"alice"` using case-insensitive English collation.

**Batch update with pagination:**

```kronotop
> BUCKET.UPDATE users '{"status": "pending"}' '{"$set": {"status": "active"}}' LIMIT 100
1# "cursor_id" => (integer) 1
2# "object_ids" =>... (first 100 updated)

> BUCKET.ADVANCE UPDATE 1
1# "cursor_id" => (integer) 1
2# "object_ids" => ... (next 100 updated)
```

**Update within a transaction:**

```kronotop
BEGIN
BUCKET.UPDATE users '{"status": "pending"}' '{"$set": {"status": "active"}}'
COMMIT
```
