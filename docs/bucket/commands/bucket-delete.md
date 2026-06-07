---
title: "BUCKET.DELETE"
sidebar:
  order: 6
description: "Deletes documents from a bucket that match a filter expression."
---

Deletes documents from a bucket that match a filter expression.

## Syntax

```kronotop
BUCKET.DELETE <bucket> <query> [LIMIT <n>] [COLLATION <json-spec>]
```

## Parameters

| Parameter   | Type         | Required | Description                                                                                              |
|-------------|--------------|----------|----------------------------------------------------------------------------------------------------------|
| `bucket`    | string       | Yes      | Name of the bucket to delete from.                                                                       |
| `query`     | JSON or BSON | Yes      | Filter expression to match documents. Use `{}` to match all documents.                                   |
| `LIMIT`     | integer      | No       | Maximum number of documents to delete per batch. Must be non-negative.                                   |
| `COLLATION` | JSON         | No       | Query-level collation spec for locale-aware string comparison. Overrides index collation for this query. |

Note: `SORTBY` is not supported for delete operations.

## Return Value

The command returns a cursor ID and an array of ObjectIds for deleted documents. The format depends on the protocol
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
2# "object_ids" => 1) "6835a1c0e4b0f72a3c000001"
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

**Auto-commit mode (default):**

The delete operation is committed immediately. Deleted documents cannot be recovered.

**Transaction mode (within BEGIN/COMMIT):**

The delete operation is not committed until `COMMIT` is called. Use `ROLLBACK` to cancel the delete operation.

## Pagination

When using `LIMIT`, use the cursor ID with `BUCKET.ADVANCE` to delete more matching documents:

```kronotop
BUCKET.ADVANCE DELETE <cursor-id>
```

Each call deletes the next batch of documents up to the limit.

## Routing

`BUCKET.DELETE` is a metadata operation and can be executed from any node. The exception is a bucket with a vector
index.
A vector-indexed bucket is pinned to a single shard, and deleting a document must also remove its vector from the local
graph, so the command must be sent to the node that owns that shard. When the shard is hosted on another node, the
server
rejects the request with a redirect to that node.

## Errors

| Error Code              | Description                                                                                                                                                     |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `REJECT`                | Only when the bucket has a vector index: the bucket's shard is hosted on another node. The error includes the target address: `REJECT <shardId> <host>:<port>`. |
| `BUCKETBEINGREMOVED`    | The bucket is being removed.                                                                                                                                    |
| `NOSUCHBUCKET`          | The bucket does not exist.                                                                                                                                      |
| `NOSUCHNAMESPACE`       | The namespace does not exist.                                                                                                                                   |
| `NAMESPACEBEINGREMOVED` | The namespace is being removed.                                                                                                                                 |
| `VECTORINDEXNOTREADY`   | A vector index on the bucket is still bootstrapping. Retry after a short delay.                                                                                 |
| `ERR`                   | `SORTBY` is an unsupported argument.                                                                                                                            |

## Examples

**Delete all documents:**

```kronotop
BUCKET.DELETE users '{}'
```

**Delete with filter:**

```kronotop
BUCKET.DELETE users '{"status": "inactive"}'
```

**Delete with limit:**

```kronotop
BUCKET.DELETE users '{"age": {"$gt": 30}}' LIMIT 50
```

**Delete with collation:**

```kronotop
BUCKET.DELETE users '{"name": "alice"}' COLLATION '{"locale": "en", "strength": 2}'
```

Deletes documents where `name` matches `"alice"` using case-insensitive English collation.

**Batch delete with pagination:**

```kronotop
> BUCKET.DELETE users '{"status": "inactive"}' LIMIT 100
1# "cursor_id" => (integer) 1
2# "object_ids" =>... (first 100 deleted)

> BUCKET.ADVANCE DELETE 1
1# "cursor_id" => (integer) 1
2# "object_ids" => ... (next 100 deleted)
```

**Delete within a transaction:**

```kronotop
BEGIN
BUCKET.DELETE users '{"status": "inactive"}'
COMMIT
```
