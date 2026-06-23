# Kronotop

[![Website](https://img.shields.io/badge/kronotop.com-docs-00c5d1?labelColor=061827)](https://kronotop.com) [![Build](https://img.shields.io/github/actions/workflow/status/kronotop/kronotop/maven.yaml?branch=main&label=build)](https://github.com/kronotop/kronotop/actions/workflows/maven.yaml) [![Release](https://img.shields.io/github/v/release/kronotop/kronotop)](https://github.com/kronotop/kronotop/releases/latest) [![Docker](https://img.shields.io/badge/ghcr.io-kronotop%2Fkronotop-2496ED?logo=docker&logoColor=white)](https://github.com/kronotop/kronotop/pkgs/container/kronotop) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![Discord](https://img.shields.io/discord/1004765247212638239.svg?label=&logo=discord&logoColor=ffffff&color=7389D8&labelColor=6A7EC2)](https://discord.gg/VPRNvdh2C)

Kronotop is an open-source, distributed, transactional document database built on [FoundationDB](https://github.com/apple/foundationdb).

> Millions of databases. One cluster.

Kronotop gives every AI agent, tenant, or app its own logical database, called a namespace, and runs many
namespaces on a single cluster. A namespace is a prefix in the keyspace, so creating one is free, and isolation comes
from the prefix, not from application code. One transaction still commits across any number of namespaces.

Beyond its document model, Kronotop provides a transactional ordered key-value data structure (ZMap) that participates
in the same strictly serializable transaction boundary.

Kronotop currently provides:

- **Bucket**: document model with secondary indexes and vector search
- **ZMap**: ordered key-value model with range queries and conflict-free counters
- **Volume**: internal storage engine that backs data models with segment-based I/O and primary-standby replication
- **Namespaces**: lightweight logical isolation built on FoundationDB directories

Kronotop speaks RESP2 and RESP3 and works with existing RESP-compatible clients.

With `kronotop-cli` or `valkey-cli`, a single transaction can atomically write a document and update a counter across
isolated namespaces:

```
BEGIN

# In the sales namespace: record a new order.
NAMESPACE USE production.sales
BUCKET.INSERT orders DOCS '{"item": "keyboard", "qty": 2, "price": 49.99}'

# In the inventory namespace: decrement stock, conflict-free.
NAMESPACE USE production.inventory
ZINC.I64 keyboard -2

COMMIT
```

Read it back with the Bucket Query Language (BQL):

```
NAMESPACE USE production.sales
BUCKET.QUERY orders '{ "qty": { "$gte": 2 } }'
1# "cursor_id" => (integer) 1
2# "entries" => 1) {"_id": "6a133c8806bf494c9e7e00cb", "item": "keyboard", "qty": 2, "price": 49.99}
```

Jump to [Quickstart](#quickstart) for more details and quickly spin up a cluster.

Join our [Discord](https://discord.gg/VPRNvdh2C) to ask questions, share feedback, or follow development.

## Project Status

Kronotop is currently in developer preview.

The core architecture and transaction model are largely stable, but APIs, internal formats, and operational behavior
may still change before the first stable release. Documentation may occasionally lag behind the implementation.

See the [roadmap](ROADMAP.md) for where Kronotop is headed.

## Contents

- [Quickstart](#quickstart)
- [Building from Source](#building-from-source)
- [Core Concepts](#core-concepts)
- [Benchmarks](#benchmarks)
- [Use Cases](#use-cases)
- [Namespaces](#namespaces)
- [Bucket](#bucket)
- [ZMap](#zmap)
- [Transactions](#transactions)
- [Clustering](#clustering)
- [Why These Building Blocks?](#why-these-building-blocks)
- [Contribution](#contribution)
- [Support](#support)
- [About the Name](#about-the-name)
- [License](#license)

## Quickstart

The fastest way to try Kronotop is with Docker Compose. Prebuilt jars are also published on the
[Releases page](https://github.com/kronotop/kronotop/releases); download them and run with `java -jar`,
which requires Java 25 or newer.

Download the Docker Compose file:

```bash
curl -O https://kronotop.com/kronotop-quickstart.yaml
```

Start the cluster:

```bash
docker compose -f kronotop-quickstart.yaml up
```

This starts a minimal cluster: one FoundationDB instance, one Kronotop primary node, and one standby node.
The primary owns bucket shard 0; the standby replicates it for failover.

Kronotop listens on two ports: 5484 for client traffic and 3320 for cluster administration. Once all containers are
running, connect to the admin port with `kronotop-cli`:

```bash
docker run --rm -it --platform linux/amd64 --network kronotop \
  ghcr.io/kronotop/kronotop:latest kronotop-cli -h kronotop-primary -p 3320
```

Alternatively, you can use `kronotop-cli`, `redis-cli` or `valkey-cli` if you have them installed locally:

```bash
valkey-cli -p 3320
```

Verify the cluster is up:

```
kronotop-primary:3320> PING
PONG
kronotop-primary:3320> KR.ADMIN DESCRIBE-CLUSTER
1# "metadata_version" => "1.0.0"
2# "cluster_name" => "development"
3# "bucket" =>
   1# (integer) 0 =>
      1# "primary" => "f627bf46f8627333a064de5c388d0316cc223a54"
      2# "standbys" => 1) "e159f73fb21cd6ac80639b2cc1e087e8330cd947"
      3# "status" => "READWRITE"
      4# "linked_volumes" => 1) "bucket-shard-0"
```

Now connect to the client port and try inserting and querying a document:

```bash
docker run --rm -it --platform linux/amd64 --network kronotop \
  ghcr.io/kronotop/kronotop:latest kronotop-cli -h kronotop-primary -p 5484
```

`kronotop-cli` sets these automatically. If you are using `redis-cli` or `valkey-cli`, set the session attributes
manually
for human-readable output:

```
SESSION.ATTRIBUTE SET input_type json
SESSION.ATTRIBUTE SET reply_type json
SESSION.ATTRIBUTE SET object_id_format hex
```

Create a bucket:

```
kronotop-primary:5484> BUCKET.CREATE orders
OK
```

Insert a document:

```
kronotop-primary:5484> BUCKET.INSERT orders DOCS '{"item": "keyboard", "qty": 2, "price": 49.99}'
1) "6a133c8806bf494c9e7e00cb"
```

Query it back:

```
kronotop-primary:5484> BUCKET.QUERY orders '{"qty": {"$gte": 1}}'
1# "cursor_id" => (integer) 1
2# "entries" => 1) {"_id": "6a133c8806bf494c9e7e00cb", "item": "keyboard", "qty": 2, "price": 49.99}
```

## Building from Source

To build Kronotop from source and run a node locally, see the [Building and Running guide](BUILDING.md). It covers
prerequisites, installing FoundationDB, the Maven build, starting a node, and bootstrapping a cluster.

## Core Concepts

### Namespaces

Lightweight logical databases with keyspace-level data isolation, built
on [FoundationDB’s directory layer](https://apple.github.io/foundationdb/developer-guide.html#directories).

### Data Models

Bucket (documents) and ZMap (ordered key-value) share the same protocol and can participate in the same ACID
transaction.

### Transactions

Strictly serializable by default, inherited from FoundationDB. Both auto-commit and explicit modes are supported.

### Storage

Metadata and indexes live in FoundationDB. Document bodies are stored in Volume, Kronotop’s segment-based storage engine
with primary-standby replication. ZMap data is stored directly in FoundationDB.

## Benchmarks

The following results are from a read-only query workload against the Bucket data model. The setup uses three EC2 hosts
in the same availability zone: FoundationDB on an `m5d.4xlarge` (16 vCPU, 64 GB, NVMe), Kronotop on an `m5d.2xlarge`
(8 vCPU, 32 GB, NVMe), and the benchmark client on an `m5.2xlarge`. FoundationDB runs in `single` redundancy mode
with no replication.

50,000 documents were loaded into a single bucket. The setup used single-field indexes on `category` (string), `age` (
int32),
`score` (double), and a compound index on `(category, age)`.

Each scenario ran 50,000 queries across 50 virtual threads. The benchmarks do not use bulk reads or writes, and the
client does not pipeline requests. Every query runs as a single FoundationDB transaction, so the numbers reflect real
OLTP workloads where each request is an independent transaction.

| Scenario                 | Throughput | P50      | P99      |
|--------------------------|------------|----------|----------|
| Indexed equality (`$eq`) | 11,751 qps | 4.11 ms  | 6.12 ms  |
| Indexed range (`$gt`)    | 11,209 qps | 4.29 ms  | 6.35 ms  |
| Compound index           | 11,082 qps | 4.36 ms  | 6.49 ms  |
| Sort + limit 10          | 23,008 qps | 2.10 ms  | 2.88 ms  |
| Full scan (unindexed)    | 1,958 qps  | 24.33 ms | 50.02 ms |

See the [full benchmark results and deployment guide](benchmarks/README.md) for details.

## Use Cases

- AI agent context store with namespace-level isolation, vector search, and transactional updates.
- Horizontally scalable transactional document store.
- Lease-based distributed locks with atomic acquire, safe release, and fencing tokens from versionstamped commits.
- ZMap-based ordered key-value workloads: range scans, counters, and atomic mutations.

## Namespaces

Namespaces are lightweight logical databases with hierarchical, dot-separated paths. Each namespace has its own
keyspace; buckets, indexes, and ZMap keys in one namespace are invisible to another. Each session starts in the `global`
namespace.

Create a namespace:

```
127.0.0.1:5484> NAMESPACE CREATE production.sales
OK
```

Discover the current namespace of the session:

```
127.0.0.1:5484> NAMESPACE CURRENT
"global"
```

Switch to the `production.sales` namespace:

```
127.0.0.1:5484> NAMESPACE USE production.sales
OK
127.0.0.1:5484> NAMESPACE CURRENT
"production.sales"
```

See the [namespace documentation](https://kronotop.com/docs/namespaces) for the full command reference.

## Bucket

Bucket is Kronotop's document data model. It stores BSON documents and provides a query language with comparison,
logical,
and array operators. Bucket supports single field, compound, and vector indexes as secondary index.

A bucket's life starts with the `BUCKET.CREATE` command:

```
127.0.0.1:5484> BUCKET.CREATE orders
OK
```

After getting `OK`, the bucket is ready to use.

The Bucket data model supports JSON-like documents and stores them internally in BSON format.

```
127.0.0.1:5484> BUCKET.INSERT orders DOCS '{"item": "keyboard", "qty": 2, "price": 49.99}'
1) "6a133ce406bf494c9e7e00cc"
```

`BUCKET.INSERT` creates a one-off transaction, inserts the given document into the `orders` bucket, and returns a RESP
array containing the generated `ObjectId` values.

Buckets can be queried with `BUCKET.QUERY`. The Bucket Query Language (BQL) provides familiar semantics for querying
BSON documents.

The following example queries documents with a `qty` field greater than one.

```
127.0.0.1:5484> BUCKET.QUERY orders '{"qty": {"$gte": 1}}'
1# "cursor_id" => (integer) 2
2# "entries" => 1) {"_id": "6a133ce406bf494c9e7e00cc", "item": "keyboard", "qty": 2, "price": 49.99}
```

See the [bucket documentation](https://kronotop.com/docs/bucket) for the query language reference, indexing, and
sorting.

### Cursor-based Streaming

Every `BUCKET.QUERY`, `BUCKET.DELETE`, and `BUCKET.UPDATE` command returns results in batches through a cursor.
Each call produces the next batch and advances the cursor's position. Outside an explicit transaction, each
`BUCKET.ADVANCE` call runs in its own short-lived transaction, keeping FoundationDB's 5-second transaction limit 
out of the way. Cursors are session-scoped and released automatically when the session disconnects.

```
127.0.0.1:5484> BUCKET.QUERY products '{}' LIMIT 2
1# "cursor_id" => (integer) 0
2# "entries" =>
   1) {"_id": "69ce80c76597b10d87d134ff", "category": "books", "price": 19.99, "name": "The Disconnected"}
   2) {"_id": "69ce80c76597b10d87d13500", "category": "electronics", "price": 499.99, "name": "Wireless Headphones"}
```

Fetch the next batch:

```
127.0.0.1:5484> BUCKET.ADVANCE QUERY 0
1# "cursor_id" => (integer) 0
2# "entries" => (empty array)
```

An empty batch means the result set is exhausted. Close the cursor when done:

```
127.0.0.1:5484> BUCKET.CLOSE QUERY 0
OK
```

See the [cursor-based streaming documentation](https://kronotop.com/docs/bucket/cursor-based-streaming) for batch
sizing, scan budgets, sorted cursors, and best practices.

### Indexing

Secondary indexes can be created during bucket creation or managed later with the `BUCKET.INDEX` command family.

The following example creates a secondary index on the `price` field:

```
127.0.0.1:5484> BUCKET.INDEX CREATE orders '{"price": {"bson_type": "double"}}'
OK
```

Since no explicit name was provided, Kronotop generates one automatically:

```
127.0.0.1:5484> BUCKET.INDEX LIST orders
1) "primary-index"
2) "selector:price.bsonType:DOUBLE"
```

`primary-index` is the default primary index for the bucket. `selector:price.bsonType:DOUBLE` is the newly created
secondary index.

Indexes can be inspected with `BUCKET.INDEX DESCRIBE`:

```
127.0.0.1:5484> BUCKET.INDEX DESCRIBE orders selector:price.bsonType:DOUBLE
1# "index_type" => "single_field"
2# "id" => (integer) -3099127558460888373
3# "selector" => "price"
4# "bson_type" => "DOUBLE"
5# "status" => "READY"
6# "statistics" =>
   1# "cardinality" => (integer) 1
```

`BUCKET.INDEX DESCRIBE` returns metadata and statistics about the index.

### Query planning

Kronotop ships with a rule-based query planner. A query goes through four stages: parsing, logical planning, physical
planning, and optimization.

The logical planner normalizes the query tree. It flattens nested operators, eliminates double negation, detects
contradictions
and tautologies, removes redundant conditions, and folds constants. The physical planner then maps the normalized tree
to scan operations, and the optimizer iteratively applies rules such as range scan consolidation, index intersection,
and selectivity-based ordering.

`BUCKET.EXPLAIN` shows the final physical plan for a query. The following example demonstrates how the `price` index we
created above is picked up by the planner:

```
127.0.0.1:5484> BUCKET.EXPLAIN orders '{"price": {"$gte": 24.3}}'
1# "is_cached" => (false)
2# "plan" =>
   1# "planner_version" => (integer) 1
   2# "nodeType" => "IndexScan"
   3# "id" => (integer) 2
   4# "scanType" => "INDEX_SCAN"
   5# "index" => "selector:price.bsonType:DOUBLE"
   6# "selector" => "price"
   7# "operator" => "GTE"
   8# "operand" => "Param[ref=ParamRef[index=0]]"
```

### Vector Search

Bucket supports approximate nearest neighbor (ANN) search on document vector fields, powered
by [JVector](https://github.com/datastax/jvector). The graph
index uses HNSW with automatic Product Quantization for memory efficiency and supports three distance functions:
`cosine`,
`euclidean`, and `dot_product`.

Results are ranked by similarity first, then filtered with BQL predicates (post-filtering). Search behavior is tunable
via `TOP`, `THRESHOLD`, `OVERQUERY`, and `MAX-SCAN-CANDIDATES`. The graph index is updated asynchronously after commit,
so vector search does not carry ACID guarantees.

Create a bucket with a vector index:

```
127.0.0.1:5484> BUCKET.CREATE products SHARDS 0 INDEXES '{"$vector": {"field": "embedding", "dimensions": 3, "distance": "cosine"}}'
OK
```

Insert a document with a vector embedding:

```
127.0.0.1:5484> BUCKET.INSERT products DOCS '{"label": "alpha", "embedding": [0.1, 0.2, 0.3]}'
1) "6a0ee480029d1d2fdee1d700"
```

Run a similarity search:

```
127.0.0.1:5484> BUCKET.VECTOR products embedding '[0.4, 0.5, 0.6]' TOP 2
1) 1# "score" => (double) 0.9873158931732178
   2# "entry" => {"_id": "6a0ee480029d1d2fdee1d700", "label": "alpha", "embedding": [0.1, 0.2, 0.3]}
```

See the [vector search documentation](https://kronotop.com/docs/bucket/vector-index) for configuration details and query
options.

## ZMap

ZMap is a RESP-compatible proxy over FoundationDB's ordered key-value API. Unlike Bucket, ZMap does not use Volume.
It operates as a direct RESP-to-FoundationDB proxy layer. Keys and values are opaque byte sequences. ZMap does not
interpret their contents. The examples below use JSON as values for readability, but any byte payload works.
FoundationDB's key size limit (10 KB) and value size limit (100 KB) apply directly.

Keys are stored in lexicographic order, and all operations inherit FoundationDB's ACID transaction guarantees.
ZMap provides typed numeric commands for int64 (conflict-free via FoundationDB's atomic `ADD`), float64 and decimal128
(read-modify-write), conflict-free atomic mutations, and efficient range operations over the ordered key space.

Set and read a value:

```
127.0.0.1:5484> ZSET user:1001 '{"name": "alice", "role": "admin"}'
OK
127.0.0.1:5484> ZGET user:1001
{"name": "alice", "role": "admin"}
```

Atomic increment - `ZINC.I64` maps to FoundationDB's native atomic `ADD`, so concurrent increments on the same key never
conflict. `ZINC.F64` and `ZINC.D128` perform a read-modify-write cycle and can conflict under contention:

```
127.0.0.1:5484> ZINC.I64 page-views 1
OK
127.0.0.1:5484> ZGET.I64 page-views
(integer) 1
```

Ordered range scan:

```
127.0.0.1:5484>  ZGETRANGE user:1000 user:1999 LIMIT 3
1) 1) "user:1001"
   2) {"name": "alice", "role": "admin"}
```

`ZMUTATE` exposes FoundationDB's conflict-free atomic mutations such as `ADD`, `BIT_AND`, `BIT_OR`, `BIT_XOR`, `MIN`,
`MAX`, `COMPARE_AND_CLEAR`, and others, through a single command. These primitives are useful for building distributed
counters, flags, and coordination mechanisms without transaction conflicts.

See the [ZMap documentation](https://kronotop.com/docs/zmap) for the full command reference, atomic mutations, and range
operations.

The [recipes](recipes/) directory shows how to compose these primitives into higher-level coordination patterns.
See the [distributed lock on ZMap](recipes/distributed-lock-on-zmap.md) draft recipe for a lease-based lock with a fencing token.

## Transactions

Kronotop transactions are thin wrappers around FoundationDB transactions. By default, each command runs in auto-commit
mode. The system creates a transaction, executes the command, and commits it. To group multiple commands into a single atomic
unit, wrap them in `BEGIN` - `COMMIT` block:

```
127.0.0.1:5484> BEGIN
OK
127.0.0.1:5484> ZSET key1 100
OK
127.0.0.1:5484> ZSET key2 200
OK
127.0.0.1:5484> COMMIT
OK
```

`ROLLBACK` discards all uncommitted changes. Switching namespaces within a transaction is allowed, a single `BEGIN` -
`COMMIT` block can atomically span multiple namespaces. Snapshot reads are available via `SNAPSHOTREAD ON` for read-heavy
workloads where strict serializability is not required. Snapshot reads do not create read conflict ranges, so they will 
not conflict with concurrent writes.

Transactions inherit FoundationDB's constraints: a 5-second time window starting from the first read and a 10 MB
transaction size limit. For Bucket, only metadata passes through FoundationDB; document bodies are stored in the Volume
layer, so the size limit does not apply to document bodies.

See the [transaction documentation](https://kronotop.com/docs/transactions) for snapshot reads, cross-namespace
transactions,
and the transaction time window.

## Clustering

Kronotop is designed to run as a distributed cluster. Data is partitioned into shards, and each shard is assigned to a
cluster member. FoundationDB handles all cluster coordination. So there is no separate consensus layer or gossip
protocol.

Volumes replicate data using a primary-standby model. Each shard has one primary and one or more standbys. Failover and
shard reassignment are managed through the admin command interface.

See the [cluster administration documentation](https://kronotop.com/docs/admin/cluster/operations-guide) for cluster
initialization, shard routing, and replication configuration.

## Why These Building Blocks?

### FoundationDB

FoundationDB provides strictly serializable transactions, automatic sharding, and fault tolerance. Kronotop inherits
these guarantees instead of implementing its own consensus layer.

### RESP

Kronotop needs a lightweight request-response protocol. Developers already know how RESP works, and most mainstream
languages have a RESP client. That means Kronotop SDKs are thin wrappers, not ground-up implementations.

### Java

The original plan was to build on FoundationDB Record Layer and Apache Calcite, or at least reuse some of their
components. Both projects are in Java. After implementing key components, the project shifted toward developing a native document
layer. The FoundationDB Java client and the Directory Layer are officially supported. Being on the JVM also gives access to
libraries like [JVector](https://github.com/datastax/jvector), which powers Kronotop's vector search. These were the main reasons behind the language choice.

## Contribution

Kronotop is not accepting external contributions at this time. This policy will change as the project stabilizes. Until
then, all pull requests will be closed regardless of their content.
For bug reports and suggestions, please use the **Issues** section.

## Support

- [GitHub Issues](https://github.com/kronotop/kronotop/issues)
- [Discord](https://discord.gg/VPRNvdh2C)
- Support & inquiries: [contact@kronotop.com](mailto:contact@kronotop.com)

## About the Name

[Chronotope](https://en.wikipedia.org/wiki/Chronotope) is a concept from literary theory introduced
by [Mikhail Bakhtin](https://en.wikipedia.org/wiki/Mikhail_Bakhtin). It refers to the relationship between time
and space in a narrative. An early goal of the project was to bring OLTP and OLAP workloads together under different 
data models. The name felt appropriate: the idea of time and space aligned with that ambition, and nobody in the
database world seemed to be using it. Kronotop is the Turkish phonetic spelling of Chronotope.

## License

Kronotop is licensed under the [Apache License 2.0](LICENSE).

---

FoundationDB is a trademark of Apple Inc. Kronotop is an independent project and is not affiliated with, endorsed by,
or sponsored by Apple Inc.