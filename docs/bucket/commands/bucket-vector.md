---
title: "BUCKET.VECTOR"
sidebar:
  order: 11
description: "Performs vector similarity search on a bucket using a vector index backed by JVector, with optional post-filtering to combine similarity ranking with structured query predicates."
---

Performs vector similarity search on a bucket using a vector index backed
by [JVector](https://github.com/datastax/jvector), with optional post-filtering to combine similarity ranking with
structured query predicates.

> **Note:** `BUCKET.VECTOR` does not provide ACID transaction guarantees. The search operates on a graph index that is
> updated asynchronously after a transaction commit, and matching documents are read directly from the storage engine
> outside of a transaction. Newly inserted or deleted vectors may not immediately appear in search results.

## Syntax

```kronotop
BUCKET.VECTOR <bucket> <selector> <vector> [FILTER <expression>] [PROJECTION <spec>] [TOP <n>] [THRESHOLD <n>] [MAX-SCAN-CANDIDATES <n>] [OVERQUERY <n>]
```

## Parameters

| Parameter             | Type           | Required | Description                                                                                                                                                                                                                                                |
|-----------------------|----------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bucket`              | string         | Yes      | Name of the bucket to search.                                                                                                                                                                                                                              |
| `selector`            | string         | Yes      | Field selector identifying the vector index to use. Supports dot notation for nested fields (e.g., `data.embedding`).                                                                                                                                      |
| `vector`              | JSON or BINARY | Yes      | Query vector provided as either a JSON array of numbers (e.g., `[0.1, 0.2, 0.3]`) or a raw binary blob of packed floats. Must have the same number of dimensions as the vector index.                                                                      |
| `FILTER`              | JSON or BSON   | No       | A BQL filter expression to post-filter vector search results. The format is auto-detected: if the input starts with `{` it is parsed as JSON, otherwise as BSON. Only documents matching both the vector similarity and the filter condition are returned. |
| `PROJECTION`          | JSON or BSON   | No       | Projection specification that controls which fields appear in returned documents. Use `{"field": 1}` for inclusion or `{"field": 0}` for exclusion. See [Projection](../projection.md).                                                                    |
| `TOP`                 | integer        | No       | Maximum number of results to return. Must be non-negative. Default: `10`.                                                                                                                                                                                  |
| `THRESHOLD`           | number         | No       | Minimum similarity score. Results with a score below this value are excluded. Default: `0.0`.                                                                                                                                                              |
| `MAX-SCAN-CANDIDATES` | integer        | No       | Maximum number of vector candidates to examine during filtered search. Must be a positive integer. Limits how far the search explores the vector graph.                                                                                                    |
| `OVERQUERY`           | number         | No       | Multiplier that controls how many extra candidates the graph traversal examines beyond the requested TOP. Must be `>= 1.0`. Higher values may improve recall at the cost of latency.                                                                       |

## Binary Vector Format

For SDK developers, the binary format is a contiguous array of IEEE 754 single-precision (32-bit) floats in *
*little-endian** byte order. Each float occupies 4 bytes, so a vector with _N_ dimensions is exactly _N × 4_ bytes.

The format is auto-detected: if the first byte is `[` (0x5B), the payload is parsed as a JSON array; otherwise it is
treated as a binary blob.

**Example:** encoding a 3-dimensional vector `[0.1, 0.2, 0.3]` in Python:

```python
import struct

vector = [0.1, 0.2, 0.3]
binary = struct.pack(f"<{len(vector)}f", *vector)  # 12 bytes, little-endian
```

## Return Value

The command returns an array of results ordered by similarity score (highest first). Each result contains a similarity
score and the matching document.

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

**RESP3 (array of maps):**

Each result is a map with `score` (float) and `entry` (document bytes):

```kronotop
1) score -> (double) 0.99
   entry -> <document-bytes>
2) score -> (double) 0.87
   entry -> <document-bytes>
...
```

**RESP2 (array of pairs):**

Each result is a two-element array containing the score as a string and the document bytes:

```kronotop
1) 1) "0.99"
   2) <document-bytes>
2) 1) "0.87"
   2) <document-bytes>
...
```

When no results match, an empty array is returned.

## Filtered Search

When `FILTER` is provided, the command applies post-filtering to vector search results. The search first retrieves
vector candidates ordered by similarity, then evaluates the filter expression against each candidate's document.

If the initial batch of candidates does not yield enough results that pass the filter, the search automatically fetches
additional candidates from the vector graph in progressively larger batches until:

- Enough results are found to satisfy `TOP`, or
- The `MAX-SCAN-CANDIDATES` limit is reached, or
- The vector graph is exhausted.

The `MAX-SCAN-CANDIDATES` parameter provides an upper bound on how many candidates are examined. This is useful for
controlling latency when the filter is highly selective and most candidates do not match.

## Routing

The command must be sent to a node that owns at least one shard assigned to the bucket. If the bucket's shards are all
hosted on other nodes, the server rejects the request with a redirect to the appropriate node.

## Errors

| Error Code              | Description                                                                                                              |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `ERR`                   | No vector index exists for the given selector.                                                                           |
| `ERR`                   | The query vector dimensions do not match the index dimensions.                                                           |
| `ERR`                   | An argument is invalid (negative TOP, OVERQUERY below 1.0, or non-positive MAX-SCAN-CANDIDATES).                         |
| `REJECT`                | The bucket's shards are hosted on another node. The error includes the target address: `REJECT <shardId> <host>:<port>`. |
| `VECTORINDEXNOTREADY`   | The vector index is still being built or recovered. Retry after the background build completes.                          |
| `NOSUCHBUCKET`          | The bucket does not exist.                                                                                               |
| `BUCKETBEINGREMOVED`    | The bucket is being removed.                                                                                             |
| `NOSUCHNAMESPACE`       | The namespace does not exist.                                                                                            |
| `NAMESPACEBEINGREMOVED` | The namespace is being removed.                                                                                          |

## Examples

The examples below use human-readable input and output. Configure the session first:

```kronotop
SESSION.ATTRIBUTE SET input_type json
SESSION.ATTRIBUTE SET reply_type json
SESSION.ATTRIBUTE SET object_id_format hex
```

**Create the vector index:**

```kronotop
BUCKET.INDEX CREATE products '{
  "$vector": {"field": "embedding", "dimensions": 3, "distance": "cosine"}
}'
```

Creates a 3-dimensional cosine index on the `embedding` field. The index is built asynchronously; searches return
`VECTORINDEXNOTREADY` until the build completes.

**Insert documents with vectors:**

```kronotop
BUCKET.INSERT products DOCS '{"label": "alpha", "embedding": [0.1, 0.2, 0.3]}' '{"label": "beta", "embedding": [0.4, 0.5, 0.6]}' '{"label": "gamma", "embedding": [0.7, 0.8, 0.9]}'
```

Each `embedding` value must be an array with the same number of dimensions as the index. Vectors are added to the
index asynchronously after the insert commits.

**Basic vector search:**

```kronotop
BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]'
```

Response (RESP3):

```kronotop
1) 1# "score" => (double) 1.0
   2# "entry" => {"_id": "6a252f086c85cddddbd8918e", "label": "beta", "embedding": [0.4, 0.5, 0.6]}
2) 1# "score" => (double) 0.9990954399108887
   2# "entry" => {"_id": "6a252f086c85cddddbd8918f", "label": "gamma", "embedding": [0.7, 0.8, 0.9]}
3) 1# "score" => (double) 0.9873158931732178
   2# "entry" => {"_id": "6a252f086c85cddddbd8918d", "label": "alpha", "embedding": [0.1, 0.2, 0.3]}
```

**Search with a filter:**

```kronotop
> BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' FILTER '{"label": "beta"}'
1) 1# "score" => (double) 1.0
   2# "entry" => {"_id": "6a252f086c85cddddbd8918e", "label": "beta", "embedding": [0.4, 0.5, 0.6]}
```

Only documents where `label` equals `"beta"` are returned.

**Limit results with TOP:**

```kronotop
> BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' TOP 5
1) 1# "score" => (double) 1.0
   2# "entry" => {"_id": "6a252f086c85cddddbd8918e", "label": "beta", "embedding": [0.4, 0.5, 0.6]}
2) 1# "score" => (double) 0.9990954399108887
   2# "entry" => {"_id": "6a252f086c85cddddbd8918f", "label": "gamma", "embedding": [0.7, 0.8, 0.9]}
3) 1# "score" => (double) 0.9873158931732178
   2# "entry" => {"_id": "6a252f086c85cddddbd8918d", "label": "alpha", "embedding": [0.1, 0.2, 0.3]}
```

**Set a minimum similarity threshold:**

```kronotop
> BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' THRESHOLD 0.95
1) 1# "score" => (double) 1.0
   2# "entry" => {"_id": "6a252f086c85cddddbd8918e", "label": "beta", "embedding": [0.4, 0.5, 0.6]}
2) 1# "score" => (double) 0.9990954399108887
   2# "entry" => {"_id": "6a252f086c85cddddbd8918f", "label": "gamma", "embedding": [0.7, 0.8, 0.9]}
3) 1# "score" => (double) 0.9873158931732178
   2# "entry" => {"_id": "6a252f086c85cddddbd8918d", "label": "alpha", "embedding": [0.1, 0.2, 0.3]}
```

Only results with a similarity score of 0.95 or higher are returned.

**Combine filter and threshold:**

```kronotop
> BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' FILTER '{"label": "alpha"}' THRESHOLD 0.97
1) 1# "score" => (double) 0.9873158931732178
   2# "entry" => {"_id": "6a252f086c85cddddbd8918d", "label": "alpha", "embedding": [0.1, 0.2, 0.3]}
```

Returns only documents matching the filter that also meet the minimum similarity score.

**Control filtered search depth:**

```kronotop
BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' FILTER '{"label": "target"}' TOP 3 MAX-SCAN-CANDIDATES 100
```

Examines at most 100 vector candidates while looking for 3 results that match the filter.

**Increase recall with OVERQUERY:**

```kronotop
> BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' OVERQUERY 2.0
1) 1# "score" => (double) 1.0
   2# "entry" => {"_id": "6a252f086c85cddddbd8918e", "label": "beta", "embedding": [0.4, 0.5, 0.6]}
2) 1# "score" => (double) 0.9990954399108887
   2# "entry" => {"_id": "6a252f086c85cddddbd8918f", "label": "gamma", "embedding": [0.7, 0.8, 0.9]}
3) 1# "score" => (double) 0.9873158931732178
   2# "entry" => {"_id": "6a252f086c85cddddbd8918d", "label": "alpha", "embedding": [0.1, 0.2, 0.3]}
```

**Search with projection:**

```kronotop
> BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' PROJECTION '{"label": 1}'
1) 1# "score" => (double) 1.0
   2# "entry" => {"_id": "6a252f086c85cddddbd8918e", "label": "beta"}
2) 1# "score" => (double) 0.9990954399108887
   2# "entry" => {"_id": "6a252f086c85cddddbd8918f", "label": "gamma"}
3) 1# "score" => (double) 0.9873158931732178
   2# "entry" => {"_id": "6a252f086c85cddddbd8918d", "label": "alpha"}
```

Returns only `_id` and `label` for each result.

**Exclude embedding from results:**

```kronotop
> BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' PROJECTION '{"embedding": 0}'
1) 1# "score" => (double) 1.0
   2# "entry" => {"_id": "6a252f086c85cddddbd8918e", "label": "beta"}
2) 1# "score" => (double) 0.9990954399108887
   2# "entry" => {"_id": "6a252f086c85cddddbd8918f", "label": "gamma"}
3) 1# "score" => (double) 0.9873158931732178
   2# "entry" => {"_id": "6a252f086c85cddddbd8918d", "label": "alpha"}
```

Returns all fields except the embedding array, reducing response size.
