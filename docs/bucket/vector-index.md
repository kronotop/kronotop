---
title: "Vector Indexes"
description: "A vector index enables approximate nearest neighbor (ANN) search on document fields that contain fixed-dimension numeric vectors."
---

## Introduction

A vector index enables approximate nearest neighbor (ANN) search on document fields that contain fixed-dimension numeric
vectors.
It is backed by [JVector](https://github.com/datastax/jvector), a high-performance graph-based vector search library.
Use vector indexes when your documents contain embeddings and you need similarity search.

## Creation

Vector indexes are defined with the `$vector` key in the index schema:

```kronotop
BUCKET.INDEX CREATE products '{
  "$vector": {"field": "embedding", "dimensions": 3, "distance": "cosine"}
}'
```

A vector index on a nested field using dot notation:

```kronotop
BUCKET.INDEX CREATE products '{
  "$vector": {"field": "data.embedding", "dimensions": 128, "distance": "euclidean"}
}'
```

With an explicit name:

```kronotop
BUCKET.INDEX CREATE products '{
  "$vector": {"name": "emb_idx", "field": "embedding", "dimensions": 768, "distance": "dot_product"}
}'
```

If `name` is omitted, a name is auto-generated from the selector, dimensions, and distance function.

| Parameter    | Required | Description                                                   |
|--------------|----------|---------------------------------------------------------------|
| `field`      | Yes      | Document field containing the vector. Supports dot notation.  |
| `dimensions` | Yes      | Number of dimensions (must be >= 1).                          |
| `distance`   | Yes      | Similarity function: `cosine`, `euclidean`, or `dot_product`. |
| `name`       | No       | Index name. Auto-generated if omitted.                        |

See [BUCKET.INDEX CREATE](commands/bucket-index.md#bucketindex-create) for the full command reference.

## Distance functions

The distance function determines how similarity is measured between two vectors.

| Function      | Score range | Best for                                                                    |
|---------------|-------------|-----------------------------------------------------------------------------|
| `cosine`      | 0 to 1      | Normalized embeddings where only direction matters (e.g., text embeddings). |
| `euclidean`   | 0 to 1      | Spatial data where absolute distance matters.                               |
| `dot_product` | unbounded   | Magnitude-aware similarity where vector length carries meaning.             |

Cosine similarity ignores vector magnitude. Two vectors pointing in the same direction score 1.0 regardless of length.
Dot product does not normalize, so vectors with larger magnitudes produce higher scores.

## How vector search works

**Graph-based search.** The index builds an HNSW (Hierarchical Navigable Small World) graph where each vector is a node
connected to its approximate nearest neighbors. Searching traverses this graph starting from an entry point, greedily
following edges toward the query vector. This gives sub-linear search time.

**Product Quantization.** After enough vectors are indexed (configurable, default 1000), the system automatically trains
a Product Quantization (PQ) model. PQ compresses each vector into a compact code, reducing memory usage significantly.
During graph construction, PQ-scored comparisons replace exact comparisons. At search time, exact scores are used
against the full vectors for final ranking, so recall is not degraded by the compression.

**Multi-tier storage.** Vectors start in an in-memory index. When the in-memory index exceeds a size threshold (
configurable, default 256 MB), it is flushed to disk. Search spans both in-memory and on-disk indexes, merging results
from all tiers by similarity score.

## Post-filtering

Vector search can be combined with structured query predicates using the `FILTER` parameter on `BUCKET.VECTOR`.
Filtering is applied after similarity ranking. The search first retrieves vector candidates ordered by similarity, then
evaluates the filter expression against each candidate's document.

If the initial batch of candidates does not yield enough results that pass the filter, the search automatically fetches
additional candidates by resuming the graph traversal in fixed-size batches until:

- Enough results are found to satisfy `TOP`, or
- The `MAX-SCAN-CANDIDATES` limit is reached, or
- The vector graph is exhausted.

`MAX-SCAN-CANDIDATES` provides a safety cap to control latency when the filter is highly selective and most candidates
do not match.

## Search parameters

| Parameter             | Default | Description                                                                                      |
|-----------------------|---------|--------------------------------------------------------------------------------------------------|
| `TOP`                 | 10      | Maximum number of results to return.                                                             |
| `THRESHOLD`           | 0.0     | Minimum similarity score. Results below this value are excluded.                                 |
| `OVERQUERY`           | 1.0     | Multiplier (>= 1.0) for extra candidates examined beyond TOP before reranking with exact scores. |
| `MAX-SCAN-CANDIDATES` | 10000   | Upper bound on candidates examined during filtered search.                                       |
| `FILTER`              | none    | BQL expression to post-filter results by document fields.                                        |
| `PROJECTION`          | none    | Projection specification to control which fields are returned.                                   |
| `COLLATION`           | none    | Collation specification for string comparison in filters.                                        |

**OVERQUERY** must be >= 1.0. When omitted, uses the server default configured via `bucket.vector.default_overquery` (
1.0 by default). Increase above 1.0 to improve recall when Product Quantization is active. PQ-scored graph traversal may
miss some true neighbors; overquerying compensates by collecting extra candidates and reranking with exact scores.
Values between 1.5 and 3.0 are typical.

**THRESHOLD** is useful for discarding low-quality matches. With cosine distance, a threshold of 0.8 means only vectors
with at least 80% directional similarity are returned.

**MAX-SCAN-CANDIDATES** is only relevant when `FILTER` is provided. Limits worst-case latency when most candidates fail
the filter.

See [BUCKET.VECTOR](commands/bucket-vector.md) for the full command reference.

## Constraints

- **No ACID transaction guarantees.** The searchable graph index is updated asynchronously after the transaction commit.
  Matching documents are read directly from the storage engine outside of a transaction. Newly inserted or deleted
  vectors may not immediately appear in or disappear from search results.
- **Single-shard buckets only.** Vector indexes cannot be created on buckets that span multiple shards.
- **Dimensions must be >= 1.** The `dimensions` parameter must be a positive integer.
- **Query vector must match index dimensions.** The vector passed to `BUCKET.VECTOR` must have exactly the same number
  of elements as the index's declared dimensions.
- **One vector index per field.** Each field selector can have at most one vector index.
- **Unique names.** Index names must be unique across all indexes (single-field, compound, and vector) in the bucket.

An update that modifies the vector field, or replaces a parent field that contains it, refreshes the vector index entry
for that document.

## Practical example

Create a single-shard bucket with a vector index on `embedding`:

```kronotop
BUCKET.CREATE products SHARDS 0 INDEXES '{
  "$vector": {"field": "embedding", "dimensions": 3, "distance": "cosine"}
}'
```

Insert some documents:

```kronotop
BUCKET.INSERT products DOCS '{"label": "alpha", "embedding": [0.1, 0.2, 0.3]}'
BUCKET.INSERT products DOCS '{"label": "beta", "embedding": [0.4, 0.5, 0.6]}'
BUCKET.INSERT products DOCS '{"label": "gamma", "embedding": [0.7, 0.8, 0.9]}'
BUCKET.INSERT products DOCS '{"label": "delta", "embedding": [0.9, 0.1, 0.0]}'
```

**Basic similarity search**, find the 2 most similar vectors to `[0.4, 0.5, 0.6]`:

```kronotop
BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' TOP 2
```

Returns `beta` (exact match, score 1.0) and `gamma` (the closest neighbor).

**Filtered search**, only return documents where the label is "gamma":

```kronotop
BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' FILTER '{"label": {"$eq": "gamma"}}'
```

**Threshold search**, only results with similarity >= 0.95:

```kronotop
BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' THRESHOLD 0.95
```

**Overquery for better recall:**

```kronotop
BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' TOP 5 OVERQUERY 2.0
```
