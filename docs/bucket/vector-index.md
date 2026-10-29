---
title: "Vector Indexes"
description: "A vector index enables approximate nearest neighbor (ANN) search on document fields that contain fixed-dimension numeric vectors."
---

## Introduction

A vector index enables approximate nearest neighbor (ANN) search on document fields that contain fixed-dimension numeric
vectors.

It is backed by [JVector](https://github.com/datastax/jvector), a graph-based vector search library. Use vector indexes when your documents contain 
embeddings and you need similarity search.

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

If `name` is omitted, Kronotop builds a name from the selector, dimensions, and distance function.

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
| `cosine`      | 0 to 1      | Embeddings where only direction matters (e.g., text embeddings).            |
| `euclidean`   | 0 to 1      | Spatial data where absolute distance matters.                               |
| `dot_product` | 0 to 1      | Unit-length embeddings. Faster than cosine because it skips normalization.  |

Cosine similarity ignores vector magnitude. Two vectors pointing in the same direction score 1.0 regardless of length.
Cosine rejects the zero vector because its direction is undefined.

Dot product requires every vector to have unit length. Vectors with a different norm are rejected on insert and update.
Every component must be a finite number that fits in a 32-bit float.

## How vector search works

The index builds an HNSW (Hierarchical Navigable Small World) graph. Each vector is a node connected to its approximate
nearest neighbors. A search starts from an entry point and follows edges toward the query vector. This gives sub-linear
search time.

After enough vectors are indexed (1000 by default, configurable), Kronotop trains a Product Quantization (PQ) model. PQ
compresses each vector into a short code, which cuts memory use. During graph construction, PQ scores replace exact
comparisons. At search time, the final ranking uses exact scores against the full vectors, so compression does not
reduce recall.

Vectors start in an in-memory index. When it grows past a size threshold (256 MB by default, configurable), Kronotop
flushes it to disk. A search covers both the in-memory and the on-disk indexes and merges results from all tiers by
similarity score.

## Post-filtering

Vector search can be combined with structured query predicates using the `FILTER` argument on `BUCKET.VECTOR`.
Filtering is applied after similarity ranking. The search first retrieves vector candidates ordered by similarity, then
evaluates the filter expression against each candidate's document.

If the first batch of candidates does not produce enough matches, the search resumes the graph traversal in fixed-size
batches until:

- Enough results are found to satisfy `TOP`, or
- The `MAX-SCAN-CANDIDATES` limit is reached, or
- The vector graph is exhausted.

`MAX-SCAN-CANDIDATES` caps the number of candidates examined. This bounds latency when the filter is very selective and
most candidates do not match.

## Search arguments

| Argument              | Default | Description                                                                                      |
|-----------------------|---------|--------------------------------------------------------------------------------------------------|
| `TOP`                 | 10      | Maximum number of results to return.                                                             |
| `THRESHOLD`           | 0.0     | Minimum similarity score. Results below this value are excluded.                                 |
| `OVERQUERY`           | 1.0     | Multiplier (>= 1.0) for extra candidates examined beyond TOP before reranking with exact scores. |
| `MAX-SCAN-CANDIDATES` | 10000   | Upper bound on candidates examined during filtered search.                                       |
| `FILTER`              | none    | BQL expression to post-filter results by document fields.                                        |
| `PROJECTION`          | none    | Projection specification to control which fields are returned.                                   |
| `COLLATION`           | none    | Collation specification for string comparison in filters.                                        |

`OVERQUERY` must be >= 1.0. If omitted, the server default from `bucket.vector.default_overquery` applies (1.0 by
default). Raise it above 1.0 to improve recall when Product Quantization is active. PQ-scored graph traversal can miss
some true neighbors. Overquerying collects extra candidates and reranks them with exact scores. Values between 1.5 and
3.0 are typical.

`THRESHOLD` discards weak matches. With cosine distance, a threshold of 0.8 returns only vectors with at least 80%
directional similarity.

`MAX-SCAN-CANDIDATES` only matters when `FILTER` is given. It limits worst-case latency when most candidates fail the
filter.

See [BUCKET.VECTOR](commands/bucket-vector.md) for the full command reference.

## Constraints

- The graph index gives no ACID transaction guarantees. It is not part of the transaction. Kronotop updates it after
  the commit, then sends the reply. Matching documents are read directly from the storage engine outside a transaction.
  A search from another connection can run between the commit and the graph update, so it can miss new vectors or
  still return deleted ones.
- Vector indexes work only on single-shard buckets. They cannot be created on buckets that span multiple shards.
- `dimensions` must be a positive integer (>= 1).
- The vector passed to `BUCKET.VECTOR` must have exactly as many elements as the index's declared dimensions.
- Each field selector can have at most one vector index.
- Index names must be unique across all indexes (single-field, compound, and vector) in the bucket.

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

Find the 2 vectors most similar to `[0.4, 0.5, 0.6]`:

```kronotop
BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' TOP 2
```

Returns `beta` (exact match, score 1.0) and `gamma` (the closest neighbor).

Return only documents where the label is "gamma":

```kronotop
BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' FILTER '{"label": {"$eq": "gamma"}}'
```

Return only results with similarity >= 0.95:

```kronotop
BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' THRESHOLD 0.95
```

Overquery for better recall:

```kronotop
BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' TOP 5 OVERQUERY 2.0
```
