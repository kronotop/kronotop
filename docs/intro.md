---
title: "Introduction"
description: "Kronotop is a distributed multi-model database built on FoundationDB."
---

Kronotop is a distributed multi-model database built on [FoundationDB](https://www.foundationdb.org/).

It has two data models behind one RESP interface: **Bucket**, a document model with secondary indexes and vector search,
and **ZMap**, an ordered key-value model. Both live inside namespaces and share the same transaction model. Document
bodies
are stored on the local disk by **Volume**, a segment-based storage engine with primary-standby replication.

## Data Models

**Bucket** stores BSON documents and provides a query language (BQL) with comparison, logical, and array operators.
Buckets support single-field, compound, and vector indexes. Vector indexes are powered
by [JVector](https://github.com/datastax/jvector),
use HNSW with automatic Product Quantization, and support `cosine`, `euclidean`, and `dot_product` distance functions.
Results are ranked by similarity first, then filtered with BQL predicates (post-filtering).

**ZMap** is a RESP-compatible proxy over FoundationDB's ordered key-value API. Keys and values are opaque byte
sequences.
Keys are stored in lexicographic order. ZMap provides typed numeric operations (int64, float64, decimal128),
conflict-free
atomic mutations through FoundationDB's atomic primitives, and range operations over the ordered key space.

## Namespaces

[Namespaces](namespaces/index.md) are lightweight logical databases built on FoundationDB's directory layer with
hierarchical, dot-separated paths. Each namespace has its own keyspace; buckets, indexes, and ZMap keys in one namespace
are invisible to another.

## Transactions

[Transactions](transactions/index.md) are strictly serializable, inherited from FoundationDB. Each command runs
in auto-commit mode by default. `BEGIN` and `COMMIT` group commands into a single atomic unit. A single transaction can
atomically span multiple namespaces. Snapshot reads are available for read-heavy workloads where strict serializability
is not required.

## Wire Protocol

Kronotop speaks RESP2 and RESP3 and works with existing RESP-compatible clients. `kronotop-cli` or `valkey-cli` can
connect directly.

## Getting Started

The [Quickstart](quickstart.md) starts a minimal cluster with Docker Compose and walks through inserting and querying a
document.
