# Kronotop

Kronotop is a Redis-compatible, distributed and transactional document database backed by [FoundationDB](https://www.foundationdb.org/).

The main focus of Kronotop is building a document database that supports [MQL-like query language](https://www.mongodb.com/docs/manual/reference/operator/), ACID transactions
and on-disk storage engine with a primary-standby replication model.

See [Getting started](#getting-started) section.

## At a glance

* Horizontally scalable and sharded by default,
* Supports single or multi master cluster topologies for different deployment strategies,
* Supports [Redis Serialization Protocol](https://redis.io/docs/latest/develop/reference/protocol-spec/), you can use any Redis client to connect the cluster.
* Partly supports [Redis cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/),
* Uses FoundationDB as the metadata store for cluster management and data structures,
* Implemented in Java and requires JDK 21+,

## What we have

Kronotop is still in its early stages, but we have the following features with a strong foundation.

* ZMap, Redis protocol proxy for FoundationDB API,
* Namespaces for isolating ZMaps and Buckets, basically it's a thin layer around FoundationDB's directory layer,
* Volume, storage engine implementation with a primary-standby replication model,
* Clustering with single or multi-master deployment scenarios,
* Partial support for some Redis data structures: String and Hash.

## Plans for the foreseeable future

* Design and implement a data structure called **Bucket** to store JSON-like documents,
* **Bucket** data structure will support an [MQL-like query language](https://www.mongodb.com/docs/manual/reference/operator/) and transactions backed by FoundationDB,
* Provide the most common Redis data structures such as String, Hash, Sorted Sets, etc.

## Redis compatibility

Kronotop uses RESP3 as the client protocol. The reasoning behind this is simple: there are many high-quality Redis client implementations
in all languages, and almost everyone has some experience with Redis.

Despite the main focus is building a transactional document database by using FoundationDB as a metadata store, implementing the
most common Redis data structures is on the roadmap. Currently, Kronotop has already partial support for *String* and *Hash* data structures.

## Getting started

It's easy to try Kronotop with Docker Compose:

```
curl -o kronotop-cluster.yaml https://raw.githubusercontent.com/kronotop/kronotop/refs/heads/main/docker/docker-compose.yaml
```

Then, you can run the following command to create a single-member Kronotop cluster for demonstration purposes:

```
docker compose -f kronotop-cluster.yaml up 
```

If everything goes okay, you should be able to connect to the primary node via `redis-cli`:

```
redis-cli -p 3320 -c
127.0.0.1:3320> ping
PONG
```

A cluster member serves from two ports:

* *5484* for the client communication,
* *3320* for the internal traffic and administrative commands.

### Initializing a Kronotop cluster

Before using Kronotop in your project, you first need to initialize the cluster. `KR.ADMIN INITIALIZE-CLUSTER` command creates
the cluster's layout on the FoundationDB and initializes the cluster:

```
127.0.0.1:3320> KR.ADMIN INITIALIZE-CLUSTER
OK
```

Then, we need to set the shard primary and standby ownership. To do that, you should start with listing the cluster members with
`KR.ADMIN LIST-MEMBERS` command:

```
127.0.0.1:3320> KR.ADMIN LIST-MEMBERS
1# a2760dae3b348b2fc05469aa730361be23c7af05 =>
   1# status => RUNNING
   2# process_id => AAAAAAAgvM8AAAAA
   3# external_host => 172.21.0.3
   4# external_port => (integer) 5484
   5# internal_host => 172.21.0.3
   6# internal_port => (integer) 3320
   7# latest_heartbeat => (integer) 133
```

As you can see, we have a running member. The key of this Redis map is the member ID.
We will use it to set the primary owner of all Redis shards.

The following command sets the primary owner of all Redis shards;

```
127.0.0.1:3320> KR.ADMIN ROUTE SET PRIMARY REDIS * a2760dae3b348b2fc05469aa730361be23c7af05
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

All in-memory data will be persisted and replicated by the storage engine. See [Storage Engine](#storage-engine) section for the details.

## Unique features

### Buckets

Imagine this:

```
127.0.0.1:5484> BUCKET.INSERT users "{ username: 'kronotop', status: 'ALIVE' }"
```

Then, you can query the `users` bucket with a query language that similar to [MongoDB query language](https://www.mongodb.com/docs/manual/reference/):

```
127.0.0.1:5484> BUCKET.FIND users "{ status: 'ALIVE' }"
1) { username: 'kronotop', status: 'ALIVE' }
```

### ZMap

Kronotop also provides a new data structure called ZMap. ZMap is simply a RESP proxy for FoundationDB API.

### Namespaces

Namespaces 

### Storage Engine

#### Replication

## Architecture
