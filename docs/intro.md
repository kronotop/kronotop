---
title: "Introduction"
description: "Kronotop gives every AI agent, tenant, or app its own logical database on one cluster, with transactions that span all of them."
---

Kronotop is a distributed, transactional document database built on [FoundationDB](https://www.foundationdb.org/).

Each tenant, app, or AI agent gets its own logical database, called a namespace, and one cluster runs many of them.
A namespace is a keyspace prefix, so creating one costs nothing and the prefix alone keeps namespaces apart.
One transaction can commit across any number of namespaces.

Inside a namespace you work with two data models over one RESP connection. Bucket stores documents and gives you
secondary indexes and vector search. ZMap is a plain ordered key-value store for the data that does not fit a
document. You can mix both in one transaction. Document bodies live on local disk in Volume, a segment-based storage
engine that replicates from a primary to a standby.

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
