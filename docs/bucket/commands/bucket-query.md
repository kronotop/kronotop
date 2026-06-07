---
title: "BUCKET.QUERY"
sidebar:
  order: 4
description: "Queries documents from a bucket using a filter expression."
---

Queries documents from a bucket using a filter expression.

## Syntax

```kronotop
BUCKET.QUERY <bucket> <query> [SORTBY <field> <ASC|DESC>] [RESULTSORT <field> <ASC|DESC>] [PROJECTION <spec>] [LIMIT <n>] [COLLATION <json-spec>]
```

## Parameters

| Parameter    | Type               | Required | Description                                                                                                                                                                                                                   |
|--------------|--------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bucket`     | string             | Yes      | Name of the bucket to query.                                                                                                                                                                                                  |
| `query`      | JSON or BSON       | Yes      | Filter expression to match documents. Use `{}` to match all documents.                                                                                                                                                        |
| `SORTBY`     | string + direction | No       | Sort results by a field. Requires field name followed by `ASC` or `DESC`.                                                                                                                                                     |
| `RESULTSORT` | string + direction | No       | Sort each result batch in memory by any field (indexed or not). Requires field name followed by `ASC` or `DESC`. Does not guarantee global ordering across `BUCKET.ADVANCE` calls. See [RESULTSORT](../sortby.md#resultsort). |
| `PROJECTION` | JSON or BSON       | No       | Projection specification that controls which fields appear in returned documents. Use `{"field": 1}` for inclusion or `{"field": 0}` for exclusion. See [Projection](../projection.md).                                       |
| `LIMIT`      | integer            | No       | Maximum number of documents to return per batch. Must be non-negative. When not specified, the session's default limit is used (default: 100, configurable via `SESSION.ATTRIBUTE SET limit <n>`).                            |
| `COLLATION`  | JSON               | No       | Query-level collation spec for locale-aware string comparison. Overrides index collation for this query.                                                                                                                      |

## Return Value

The command returns a cursor ID and matching documents. The format depends on the protocol version.

Each returned document includes an `_id` field (ObjectId) that serves as the document's primary key.

The encoding format of returned documents depends on the session's `reply_type` setting:

| Format | Response Type | Description                     |
|--------|---------------|---------------------------------|
| `bson` | Binary        | BSON-encoded document (default) |
| `json` | String        | JSON-encoded document           |

To change the format:

```kronotop
SESSION.ATTRIBUTE SET reply_type bson
SESSION.ATTRIBUTE SET reply_type json
```

**RESP3 (map format):**

The response is a map with two keys: `cursor_id` (integer) and `entries` (array of documents).

```kronotop
1# "cursor_id" => (integer) <cursor-id>
2# "entries" => [<document-1>, <document-2>, ...]
```

**RESP2 (array format):**

The response is an array with two elements: the cursor ID and a nested array of documents.

```kronotop
1) (integer) <cursor-id>
2) 1) <document-1>
   2) <document-2>
   ...
```

**Cursor ID:**

The cursor ID is used to fetch more results with `BUCKET.ADVANCE`. Each query creates a new cursor that stores the query
context in the session.
The cursor tracks the position in the result set for pagination.

## Pagination

Results are returned in batches. Use the cursor ID with `BUCKET.ADVANCE` to get more results:

```kronotop
BUCKET.ADVANCE QUERY <cursor-id>
```

When there are no more results, the command returns an empty result set.

The cursor maintains its state across calls:

- Query context (filter, sort, limit)
- Current position in the result set
- Transaction context (if within an explicit transaction)

## Snapshot Reads

`BUCKET.QUERY` honors the session's `SNAPSHOTREAD` setting. When `SNAPSHOTREAD ON` is active, index scans use snapshot
isolation, so they will not cause transactions to conflict with concurrent writes.
See [SNAPSHOTREAD](../../transactions/commands/snapshotread.md) for details.

## Routing

`BUCKET.QUERY` can be executed from any node. When the query is sent to a node that does not own the bucket's shards,
it still returns correct results, but with higher latency because the data is read from the owning nodes. For best
performance,
use `BUCKET.LOCATE` to find the node that owns the bucket's shards and send the query there.

## Errors

| Error Code              | Description                     |
|-------------------------|---------------------------------|
| `NOSUCHBUCKET`          | The bucket does not exist.      |
| `BUCKETBEINGREMOVED`    | The bucket is being removed.    |
| `NOSUCHNAMESPACE`       | The namespace does not exist.   |
| `NAMESPACEBEINGREMOVED` | The namespace is being removed. |

## Examples

The following examples assume `reply_type` is set to `json`.

**Query all documents:**

```kronotop
BUCKET.QUERY users '{}'
```

Response (RESP3):

```kronotop
1# "cursor_id" => (integer) 1
2# "entries" =>
   1) {"_id": "6a240c7b5da17d872dc0e102", "name": "Bob", "age": 25, "status": "active"}
   2) {"_id": "6a240c7b5da17d872dc0e103", "name": "Carol", "age": 35, "status": "inactive"}
   3) {"_id": "6a240c875da17d872dc0e104", "name": "Henry", "age": 31, "scores": [75, 100, 100]}
```

**Query with filter:**

```kronotop
BUCKET.QUERY users '{"name": "Alice"}'
```

**Query with sorting:**

```kronotop
BUCKET.QUERY users '{}' SORTBY age DESC
```

**Query with limit:**

```kronotop
BUCKET.QUERY users '{"status": "active"}' LIMIT 10
```

**Query with sorting and limit:**

```kronotop
BUCKET.QUERY users '{"status": "active"}' SORTBY age ASC LIMIT 5
```

**Query with projection:**

```kronotop
BUCKET.QUERY users '{"status": "active"}' PROJECTION '{"name": 1, "email": 1}'
```

**Query with in-memory result sort (no index required):**

```kronotop
BUCKET.QUERY users '{"status": "active"}' RESULTSORT score ASC LIMIT 10
```

**Query with collation override:**

```kronotop
BUCKET.QUERY users '{"name": "alice"}' COLLATION '{"locale": "en", "strength": 2}'
```

This performs a case-insensitive match using English locale rules, regardless of the index's collation setting.

**Pagination:**

```kronotop
> BUCKET.QUERY users '{}' LIMIT 100
1# "cursor_id" => (integer) 1
2# "entries" => [...] (first 100 documents)

> BUCKET.ADVANCE QUERY 1
1# "cursor_id" => (integer) 1
2# "entries" =>  [...] (next batch of documents)
```
