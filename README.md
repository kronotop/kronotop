# Kronotop

**The Distributed Document Database with ACID Integrity**

Kronotop is a distributed, transactional document database designed for horizontal scalability. It provides a robust
foundation for applications needing to manage large volumes of documents while ensuring strong consistency guarantees
for critical metadata operations. By leveraging FoundationDB as its transactional backend for metadata and indexes,
Kronotop delivers [ACID](https://apple.github.io/foundationdb/developer-guide.html#transaction-basics) integrity, offering reliability often sought in demanding environments.

Kronotop features an [MQL-like query language](https://www.mongodb.com/docs/manual/reference/operator/) and uses the [RESP3](https://redis.io/docs/latest/develop/reference/protocol-spec/) wire protocol, ensuring broad compatibility
with the Redis client ecosystem. It implements core Redis in-memory data structures like Strings and Hashes, alongside its
own specialized structures: ZMap (an ordered key-value store acting as a RESP proxy for FoundationDB) and Bucket (designed for storing
JSON-like documents). While document bodies are stored directly on local filesystems, Kronotop uses BSON as the default 
data format to organize and store within Buckets, with JSON also available.

*Kronotop is built for developers seeking the flexibility of a document model combined with the transactional safety and
scalability powered by FoundationDB.*

**Warning**: Kronotop is in its early stages of development. The API is unstable and likely to change in future
releases.

See [Getting started](#getting-started) and [Documentation](#documentation) sections.

Join the [Discord channel](https://discord.gg/Nyy4Afpr) to discuss.

## At a Glance

- **Developer-Focused Design:**  
  Built for developers who need the flexibility of a document model combined with strong transactional integrity, high
  performance, and operational simplicity.

- **ACID Transactions:**  
  Relies on **FoundationDB** as a transactional metadata and indexing store, offering ACID guarantees critical for
  consistency in cluster operations and data structures.

- **Native Document-Oriented Storage:**  
  Introduces **Bucket** — a specialized structure for storing JSON-like documents, backed by FoundationDB's
  transactional core.

- **Namespaces – Logical Isolation for ZMaps and Buckets:**  
  Namespaces enable multi-tenancy and logical separation across data structures.  
  Internally, it's a lightweight abstraction over FoundationDB’s Directory Layer.

- **RESP3 & RESP2 Wire Protocol Compatibility:**  
  Kronotop communicates over the [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/) protocol,
  ensuring seamless interoperability with the vast ecosystem of Redis clients across different programming languages.

- **Built for Horizontal Scalability:**  
  The system is natively designed for sharding and horizontal scaling, making it ideal for growing workloads without
  compromising performance or reliability.

- **Flexible Deployment Topologies:**  
  Supports both **single-master** and **multi-master** cluster configurations, enabling diverse deployment strategies to
  suit varying consistency and availability needs.

- **Partial Redis Cluster Specification Support:**  
  Implements key aspects of the Redis Cluster protocol, providing familiarity for teams migrating from Redis or building
  distributed applications.

- **ZMap – FoundationDB-Powered Ordered Key-Value Store:**  
  A high-performance, ordered key-value store built on top of FoundationDB.  
  ZMap acts as a Redis protocol proxy, bridging the RESP interface with FoundationDB’s transactional API.

- **Volume – Storage Engine with Replication:**  
  A storage engine designed to support **primary-standby replication**, allowing for durability and high availability of
  persistent components like Buckets.

- **Efficient Binary Data Handling:**  
  Uses **BSON** as the default storage format for structured documents, with optional JSON support for broader
  interoperability.

- **In-Memory and Durable Data Structures:**  
  Combines Redis-like in-memory structures (Strings, Hashes) with persistent, FoundationDB-backed storage layers like
  ZMap and Buckets.

## Table of Contents

* [Getting started](#getting-started)
    * [Initializing a Kronotop cluster](#initializing-a-kronotop-cluster)
* [Redis compatibility](#redis-compatibility)
* [Support](#support)
* [Documentation](#documentation)
  * API
    * [Transaction Management](docs/api/transaction-management.md)
    * [Session Management](docs/api/session-management.md)
    * [Namespaces](docs/api/namespaces.md)
    * [ZMap](docs/api/zmap.md)
  * [Cluster Administration](docs/cluster/cluster-administration.md)
  * [Task Management](docs/admin/task-management.md)
  * [Storage Engine](docs/volume/volume.md)
  * [Why Kronotop Runs on the Java Platform](docs/why-java-platform.md)
* [License](#license)

## Getting started

It's easy to try Kronotop with Docker Compose:

```bash
curl -o kronotop-demo.yaml https://raw.githubusercontent.com/kronotop/kronotop/refs/heads/main/docker/kronotop-demo.yaml
```

Then, you can run the following command to create a single-member Kronotop cluster for demonstration purposes:

```bash
docker compose -f kronotop-demo.yaml up 
```

If everything goes okay, you should be able to connect to the primary node via `redis-cli`:

```
redis-cli -p 3320 -c
127.0.0.1:3320> PING
PONG
```

A cluster member serves from two ports:

* *5484* for the client communication,
* *3320* for the internal traffic and administrative commands.

### Initializing a Kronotop cluster

Before using Kronotop in your project, you first need to initialize the cluster. `KR.ADMIN INITIALIZE-CLUSTER` command
creates the cluster's layout on the FoundationDB and initializes the cluster:

```
127.0.0.1:3320> KR.ADMIN INITIALIZE-CLUSTER
OK
```

Then, we must set the shard's primary ownership and make the shards operable. Currently, we only have a running Kronotop
instance in the cluster. It's good enough for demonstration purposes. We can assign all shards to this member.

First, we should run `KR.ADMIN DESCRIBE-MEMBER` command to learn id of the current member:

```
127.0.0.1:3320> KR.ADMIN DESCRIBE-MEMBER
1# member_id => a0dc14d811a285834c187ddc20549de7c1c1a381
2# status => RUNNING
3# process_id => AAAOz0CfYCoAAAAA
4# external_host => 127.0.0.1
5# external_port => (integer) 5484
6# internal_host => 127.0.0.1
7# internal_port => (integer) 3320
8# latest_heartbeat => (integer) 8227
```

We need `member_id` from this response. The following command sets the primary owner of all Redis shards;

```
127.0.0.1:3320> KR.ADMIN ROUTE SET PRIMARY REDIS * a0dc14d811a285834c187ddc20549de7c1c1a381
OK
```

Now we are ready to make our all Redis shards writable:

```
127.0.0.1:3320> KR.ADMIN SET-SHARD-STATUS REDIS * READWRITE
OK
```

If everything is okay, we can start using the newly formed Kronotop cluster:

```
redis-cli -p 5484 -c
127.0.0.1:5484> SET mykey "Hello"
OK
127.0.0.1:5484> GET mykey
"Hello"
```

All in-memory data will be persisted and replicated by the storage engine. See [Storage Engine](docs/volume/volume.md) section
for the details.

## Redis compatibility

Kronotop uses RESP3 as the client protocol. The reasoning behind this is simple: there are many high-quality Redis
client implementations in all languages, and almost everyone has some experience with Redis.

Despite the main focus on building a transactional document database using FoundationDB as a metadata store,
implementing the most common Redis data structures is on the roadmap. Currently, Kronotop already has partial 
support for *String*and *Hash* data structures.

## Support

Please join [Discord channel](https://discord.gg/Nyy4Afpr) for instant chat or create an Issue or Discussion on GitHub.

For invoiced sponsoring/support contracts, please contact us at *burak {dot} sezer {at} kronotop {dot} com*.

## Documentation
* API
  * [Transaction Management](docs/api/transaction-management.md)
  * [Session Management](docs/api/session-management.md)
  * [Namespaces](docs/api/namespaces.md)
  * [ZMap](docs/api/zmap.md)
* [Cluster Administration](docs/cluster/cluster-administration.md)
* [Task Management](docs/admin/task-management.md)
* [Storage Engine](docs/volume/volume.md)
* [Why Kronotop Runs on the Java Platform](docs/why-java-platform.md)

## License

The Apache License, Version 2.0 - see LICENSE for more details.
