---
title: "Introduction"
description: "A transactional document database with a logical database per tenant, and one commit across all of them."
---

Kronotop is a distributed, transactional document database built on [FoundationDB](https://www.foundationdb.org/).

Each tenant or app gets its own logical database, called a namespace, and one cluster runs many of them.
A namespace is a keyspace prefix, so creating one costs nothing and the prefix alone keeps namespaces apart.
Isolation lives in the keyspace, so application code carries no tenant filter. One transaction can commit across
any number of namespaces.

Each namespace is a full database: documents, an ordered key-value store, secondary indexes, and vector search,
under one transaction. Bucket stores the documents and carries the indexes and vector search. ZMap is a plain
ordered key-value store for the data that does not fit a document. Both speak over one RESP connection. Document
bodies live in Volume, an append-only storage engine on local disk that replicates from a primary to a standby.
The same model fits an AI agent: a namespace per agent holds its memory, vector search runs over those documents,
and one transaction updates the documents and ZMap state, such as counters, together.

## Namespaces

[Namespaces](namespaces/index.md) are lightweight logical databases built on FoundationDB's directory layer, with
hierarchical, dot-separated paths. Each namespace has its own keyspace. Buckets, indexes, and ZMap keys in one
namespace are invisible to another.

## Data models

Bucket stores BSON documents and provides a query language, BQL, with comparison, logical, and array operators.
Buckets support single-field, compound, and vector indexes. Vector indexes use [JVector](https://github.com/datastax/jvector),
HNSW with automatic Product Quantization, and the `cosine`, `euclidean`, and `dot_product` distance functions.
Results are ranked by similarity first, then filtered with BQL predicates (post-filtering).

ZMap is a RESP-compatible proxy over FoundationDB's ordered key-value API. Keys and values are opaque byte sequences,
and keys are stored in lexicographic order. ZMap provides typed numeric operations (int64, float64, decimal128),
conflict-free atomic mutations through FoundationDB's atomic primitives, and range operations over the ordered key
space.

## Transactions

[Transactions](transactions/index.md) are strictly serializable, inherited from FoundationDB. Each command runs in
auto-commit mode by default. `BEGIN` and `COMMIT` group commands into a single atomic unit. A single transaction can
atomically span multiple namespaces. Snapshot reads are available for read-heavy workloads where strict
serializability is not required.

## Wire protocol

Kronotop speaks RESP2 and RESP3 and works with existing RESP-compatible clients. `kronotop-cli` or `valkey-cli` can
connect directly.

## Getting started

The [Quickstart](quickstart.md) starts a minimal cluster with Docker Compose and walks through inserting and querying
a document.
