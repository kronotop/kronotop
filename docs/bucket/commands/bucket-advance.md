---
title: "BUCKET.ADVANCE"
sidebar:
  order: 7
description: "Advances a cursor to fetch or process the next batch of documents."
---

Advances a cursor to fetch or process the next batch of documents.

## Syntax

```kronotop
BUCKET.ADVANCE <operation> <cursor-id>
```

## Arguments

| Argument    | Type    | Required | Description                                                                                          |
|-------------|---------|----------|------------------------------------------------------------------------------------------------------|
| `operation` | string  | Yes      | The operation type. Must be `QUERY`, `DELETE`, or `UPDATE`.                                          |
| `cursor-id` | integer | Yes      | The cursor ID returned by the initial command (`BUCKET.QUERY`, `BUCKET.DELETE`, or `BUCKET.UPDATE`). |

## Return Value

The return value depends on the operation type.

**QUERY operation:**

Returns the next batch of matching documents. The format is the same as `BUCKET.QUERY`.

The encoding format of returned documents depends on the session's `reply_type` setting:

| Format | Response Type | Description                     |
|--------|---------------|---------------------------------|
| `bson` | Binary        | BSON-encoded document (default) |
| `json` | String        | JSON-encoded document           |

RESP3 (map format):

The response is a map with two keys: `cursor_id` (integer) and `entries` (array of documents).

```kronotop
1# "cursor_id" => (integer) <cursor-id>
2# "entries" => [<document-1>, <document-2>, ...]
```

RESP2 (array format):

The response is an array with two elements: the cursor ID and a nested array of documents.

```kronotop
1) (integer) <cursor-id>
2) 1) <document-1>
   2) <document-2>
   ...
```

An empty `entries` array with a positive `cursor_id` means this call found no documents and the cursor is still open.
Call `BUCKET.ADVANCE` again to continue. See [Partial and empty batches](../cursor-based-streaming.md#partial-and-empty-batches) for the reason.

A `cursor_id` of `-1` means the result set has ended or the `LIMIT` of the initial command was reached. The cursor is
removed and cannot be advanced again. This response carries the last documents, or an empty array if the previous
batch was full.

**DELETE operation:**

Deletes the next batch of matching documents and returns their ObjectIds.

The encoding format of returned ObjectIds depends on the session's `object_id_format` setting (configurable via
`SESSION.ATTRIBUTE SET object_id_format <bytes|hex>`).

RESP3 (map format):

The response is a map with two keys: `cursor_id` (integer) and `object_ids` (array of ObjectIds).

```kronotop
1# "cursor_id" => (integer) <cursor-id>
2# "object_ids" => [<object-id-1>, <object-id-2>, ...]
```

RESP2 (array format):

The response is an array with two elements: the cursor ID and a nested array of ObjectIds.

```kronotop
1) (integer) <cursor-id>
2) 1) <object-id-1>
   2) <object-id-2>
   ...
```

An empty `object_ids` array and a `cursor_id` of `-1` have the same meaning as in the QUERY operation.

**UPDATE operation:**

Updates the next batch of matching documents and returns their ObjectIds.

The format is the same as the DELETE operation.

## Cursor Lifecycle

Cursors are created by `BUCKET.QUERY`, `BUCKET.DELETE`, or `BUCKET.UPDATE` commands. Each cursor:

- Is bound to the session that created it
- Stores the query context (filter, sort, batch size, limit)
- Tracks the current position in the result set
- Respects the original batch size from the initial command
- Respects the original `LIMIT` from the initial command
- Is removed once the result set ends or the limit is reached

The cursor ID must match the operation type. For example, a cursor created by `BUCKET.QUERY` can only be used with
`BUCKET.ADVANCE QUERY`.

## Errors

Argument errors:

| Error Code | Error message                      | Cause                                                     |
|------------|------------------------------------|-----------------------------------------------------------|
| `ERR`      | `Unknown operation: '<operation>'` | The operation type is not `QUERY`, `DELETE`, or `UPDATE`. |

Bucket errors:

| Error Code           | Error message                        | Cause                                                                                                                                        |
|----------------------|--------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `ERR`                | `No such cursor: '<cursor-id>'`      | The cursor id is unknown, or the cursor was removed at the end of the result set, by `LIMIT`, by the `CLOSE` argument, or by `BUCKET.CLOSE`. |
| `BUCKETBEINGREMOVED` | `Bucket '<bucket>' is being removed` | -                                                                                                                                            |

## Examples

**Paginate through query results:**

```kronotop
> BUCKET.QUERY users '{}' BATCH 100
1# "cursor_id" => (integer) 1
2# "entries" => [...] (first 100 documents)

> BUCKET.ADVANCE QUERY 1
1# "cursor_id" => (integer) 1
2# "entries" => [...] (next 100 documents)

> BUCKET.ADVANCE QUERY 1
1# "cursor_id" => (integer) -1
2# "entries" => [] (empty)
```

**Batch delete with pagination:**

```kronotop
> BUCKET.DELETE users '{"status": "inactive"}' BATCH 50
1# "cursor_id" => (integer) 1
2# "object_ids" => [...] (first 50 deleted ObjectIds)

> BUCKET.ADVANCE DELETE 1
1# "cursor_id" => (integer) 1
2# "object_ids" => [...] (next 50 deleted ObjectIds)
```

**Batch update with pagination:**

```kronotop
> BUCKET.UPDATE users '{"status": "pending"}' '{"$set": {"status": "active"}}' BATCH 50
1# "cursor_id" => (integer) 1
2# "object_ids" => [...] (first 50 updated ObjectIds)

> BUCKET.ADVANCE UPDATE 1
1# "cursor_id" => (integer) 1
2# "object_ids" => [...] (next 50 updated ObjectIds)
```
