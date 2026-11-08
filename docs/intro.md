---
title: "Introduction"
description: "A transactional document database with a logical database per tenant, and one commit across all of them."
---

Kronotop is a distributed, transactional document database built on [FoundationDB](https://www.foundationdb.org/).

Each tenant or app gets its own logical database, called a namespace, and one cluster runs many of them. A namespace
is a key prefix in FoundationDB, so creating one costs almost nothing. The prefix keeps namespaces apart, so
application code does not need a tenant filter. One transaction can commit across any number of namespaces.

Each namespace is a full database with documents, an ordered key-value store, secondary indexes, and vector search.
Bucket stores the documents and holds the indexes and vector search. ZMap is a plain ordered key-value store for data
that does not fit in a document. Both use the same RESP connection. Document bodies live in Volume, an append-only
storage engine on local disk that replicates from a primary to a standby.

The same model works for AI agents. Each agent gets a namespace that holds its memory, vector search runs over those
documents, and one transaction updates the documents and ZMap state, such as counters, together.

## Namespaces

[Namespaces](namespaces/index.md) are lightweight logical databases built on FoundationDB's directory layer. Their
paths are hierarchical and separated by dots. Each namespace has its own keyspace, so buckets, indexes, and ZMap keys
in one namespace are invisible to another.

## Data models

Bucket stores BSON documents and has a query language, BQL, with comparison, logical, and array operators. Buckets
support single-field, compound, and vector indexes. Vector indexes are built with[JVector](https://github.com/datastax/jvector).
They use HNSW with automatic Product Quantization and support the `cosine`, `euclidean`, and `dot_product` distance functions. 
Vector search ranks results by similarity first and then filters them with BQL predicates (post-filtering).

ZMap is a RESP layer over FoundationDB's ordered key-value API. Keys and values are opaque byte sequences, and keys
are stored in lexicographic order. ZMap has typed numeric operations (int64, float64, decimal128), conflict-free
atomic mutations through FoundationDB's atomic primitives, and range operations over the ordered keyspace.

## Transactions

[Transactions](transactions/index.md) are strictly serializable. This guarantee comes from FoundationDB. By default,
each command runs in auto-commit mode. `BEGIN` and `COMMIT` group commands into one atomic unit, and one transaction
can span several namespaces. Snapshot reads are available for read-heavy workloads that do not need strict
serializability.

## Wire protocol

Kronotop speaks RESP2 and RESP3 and works with existing RESP-compatible clients. You can connect with `kronotop-cli`
or `valkey-cli`.

## Getting started

The [Quickstart](quickstart.md) starts a minimal cluster with Docker Compose and shows how to insert and query a
document.
