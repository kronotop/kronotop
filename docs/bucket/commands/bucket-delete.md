---
title: "BUCKET.DELETE"
sidebar:
  order: 6
description: "Deletes documents from a bucket that match a filter expression."
---

Deletes documents from a bucket that match a filter expression.

## Syntax

```kronotop
BUCKET.DELETE <bucket> <query> [BATCH <n>] [LIMIT <n>] [COLLATION <json-spec>]
```

## Parameters

Keyword names are not case-sensitive, and each keyword can appear at most once.

| Parameter   | Type         | Required | Description                                                                                                                                                                                                                                                |
|-------------|--------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bucket`    | string       | Yes      | Name of the bucket to delete from.                                                                                                                                                                                                                         |
| `query`     | JSON or BSON | Yes      | Filter expression to match documents. Use `{}` to match all documents.                                                                                                                                                                                     |
| `BATCH`     | integer      | No       | Maximum number of documents to delete per batch. Must be non-negative. It does not cap the total number of results, use `LIMIT` for that. Use `BUCKET.ADVANCE` to get the next batch.                                                                      |
| `LIMIT`     | integer      | No       | Maximum total number of documents the cursor deletes across the first call and all `BUCKET.ADVANCE` calls. Must be non-negative. `0` means no limit (default). When the limit is reached, the response carries `cursor_id` `-1` and the cursor is removed. |
| `COLLATION` | JSON         | No       | Query-level collation spec for locale-aware string comparison. Overrides index collation for this query.                                                                                                                                                   |

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

When using `BATCH`, use the cursor ID with `BUCKET.ADVANCE` to delete more matching documents:

```kronotop
BUCKET.ADVANCE DELETE <cursor-id>
```

Each call deletes the next batch of documents up to the batch size.

`BATCH` caps a single call, `LIMIT` caps the whole cursor. Each call deletes at most the smaller of `BATCH` and the
remaining `LIMIT`. The call that reaches the limit returns `cursor_id` `-1` and removes the cursor from the session.
There is no need to call `BUCKET.CLOSE` on it.

## Routing

`BUCKET.DELETE` is a metadata operation and can be executed from any node. The exception is a bucket with a vector
index.
A vector-indexed bucket is pinned to a single shard, and deleting a document must also remove its vector from the local
graph, so the command must be sent to the node that owns that shard. When the shard is hosted on another node, the
server
rejects the request with a redirect to that node.

## Errors

Argument errors:

| Error Code | Error message                                               | Cause                                          |
|------------|-------------------------------------------------------------|------------------------------------------------|
| `ERR`      | `'SORTBY' is an unsupported argument`                       | -                                              |
| `ERR`      | `BATCH argument must be followed by a non-negative integer` | -                                              |
| `ERR`      | `LIMIT argument must be followed by a non-negative integer` | -                                              |
| `ERR`      | `Unknown sort direction: '<value>'`                         | The `SORTBY` direction is not `ASC` or `DESC`. |
| `ERR`      | `Unknown '<keyword>' argument`                              | -                                              |
| `ERR`      | `Duplicate '<keyword>' argument`                            | -                                              |

Namespace errors:

| Error Code              | Error message                         | Cause |
|-------------------------|---------------------------------------|-------|
| `NOSUCHNAMESPACE`       | `No such namespace: '<path>'`         | -     |
| `NAMESPACEBEINGREMOVED` | `Namespace '<path>' is being removed` | -     |

Bucket errors:

| Error Code           | Error message                        | Cause                                                                                                                            |
|----------------------|--------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| `NOSUCHBUCKET`       | `No such bucket: '<bucket>'`         | -                                                                                                                                |
| `BUCKETBEINGREMOVED` | `Bucket '<bucket>' is being removed` | -                                                                                                                                |
| `REJECT`             | `<shardId> <host>:<port>`            | Only when the bucket has a vector index: the bucket's shard is hosted on another member. The message carries the target address. |

Index errors:

| Error Code            | Error message                                                    | Cause                                                                                           |
|-----------------------|------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `VECTORINDEXNOTREADY` | `Vector index '<namespace>/<bucket>/<indexId>' is not ready yet` | The vector index is still being built or recovered. Retry after the background build completes. |

## Examples

**Delete all documents:**

```kronotop
BUCKET.DELETE users '{}'
```

**Delete with filter:**

```kronotop
BUCKET.DELETE users '{"status": "inactive"}'
```

**Delete with a batch size:**

```kronotop
BUCKET.DELETE users '{"age": {"$gt": 30}}' BATCH 50
```

**Delete with a total limit:**

```kronotop
BUCKET.DELETE users '{"age": {"$gt": 30}}' BATCH 50 LIMIT 120
```

Deletes at most 120 documents in total. The first two calls delete 50 each, the third deletes 20 and returns
`cursor_id` `-1`.

**Delete with collation:**

```kronotop
BUCKET.DELETE users '{"name": "alice"}' COLLATION '{"locale": "en", "strength": 2}'
```

Deletes documents where `name` matches `"alice"` using case-insensitive English collation.

**Batch delete with pagination:**

```kronotop
> BUCKET.DELETE users '{"status": "inactive"}' BATCH 100
1# "cursor_id" => (integer) 1
2# "object_ids" =>... (first 100 deleted)

> BUCKET.ADVANCE DELETE 1
1# "cursor_id" => (integer) 1
2# "object_ids" => ... (next 100 deleted)
```

**Batch delete with a limit:**

```kronotop
> BUCKET.DELETE users '{"status": "inactive"}' BATCH 2 LIMIT 3
1# "cursor_id" => (integer) 2
2# "object_ids" => ... (2 deleted)

> BUCKET.ADVANCE DELETE 2
1# "cursor_id" => (integer) -1
2# "object_ids" => ... (1 deleted)

> BUCKET.ADVANCE DELETE 2
(error) ERR No previous query context found for 'delete' operation with the given cursor id
```

**Delete within a transaction:**

```kronotop
BEGIN
BUCKET.DELETE users '{"status": "inactive"}'
COMMIT
```
