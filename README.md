# Kronotop

Kronotop is a Redis-compatible, distributed and transactional document database backed by [FoundationDB](https://www.foundationdb.org/).

The main focus of Kronotop is building a document database that supports [MQL-like query language](https://www.mongodb.com/docs/manual/reference/operator/), ACID transactions
and on-disk storage engine with a primary-standby replication model.

Kronotop is still in its early stages of development. The API is unstable and might be changed in the future releases.

See [Getting started](#getting-started) section.

## At a glance

* Horizontally scalable and sharded by default,
* Supports single or multi-master cluster topologies for different deployment strategies,
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
curl -o kronotop-demo.yaml https://raw.githubusercontent.com/kronotop/kronotop/refs/heads/main/docker/kronotop-demo.yaml
```

Then, you can run the following command to create a single-member Kronotop cluster for demonstration purposes:

```
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

## Features

### Transaction management

Kronotop exposes the FoundationDB API to Redis clients. This section explains transaction management commands.

*Note that not all parts of the FoundationDB API are implemented yet.*

#### BEGIN

`BEGIN` creates a new transaction and set it to the current session. 

**Example:**

```
127.0.0.1:5484> BEGIN
OK
```

If there is another transaction in progress in the current session, it returns a `TRANSACTION` error message.

```
127.0.0.1:5484> BEGIN
(error) TRANSACTION there is already a transaction in progress.
```

#### COMMIT

`COMMIT` commits 

**Example:**

```
127.0.0.1:5484> COMMIT
OK
```

FoundationDB currently does not support transactions running for over five seconds. If the transaction is too old, 
it returns `TRANSACTIONOLD` error. 

```
127.0.0.1:5484> COMMIT
(error) TRANSACTIONOLD transaction is too old to perform reads or be committed
```

If there is no transaction in progress in the current session, it returns `TRANSACTION` error.

```
127.0.0.1:5484> COMMIT
(error) TRANSACTION there is no transaction in progress.
```

#### ROLLBACK

`ROLLBACK` cancels the current session's in-progress transaction.

**Example:**

```
127.0.0.1:5484> ROLLBACK
OK
```

If no transaction is in progress in the current session, it returns a `TRANSACTION` error message.

```
127.0.0.1:5484> ROLLBACK
(error) TRANSACTION there is no transaction in progress.
```

#### GETAPPROXIMATESIZE

`GETAPPROXIMATESIZE` returns the approximated size of the commit, which is the summation of mutations, read conflict ranges, 
and write conflict ranges. This can be called multiple times before a transaction commit.

```
127.0.0.1:5484> GETAPPROXIMATESIZE
(integer) 619
```

#### GETREADVERSION

`GETREADVERSION` returns the version at which the reads for the in-progress transaction will access the database.

```
127.0.0.1:5484> GETREADVERSION
16275608010704
```

#### SNAPSHOTREAD

`SNAPSHOTREAD ON|OFF` enables or disables snapshot reads for the in-progess transaction.

In the FoundationDB context, snapshots a special-purpose, read-only view of the database. Reads done through this interface 
are known as "snapshot reads." Snapshot reads selectively relax FoundationDB's isolation property, reducing [Transaction 
conflicts](https://apple.github.io/foundationdb/developer-guide.html#conflict-ranges) but making reasoning about concurrency harder.  For more information about how to use snapshot reads correctly, 
see Using [snapshot reads](https://apple.github.io/foundationdb/developer-guide.html#snapshot-reads).

**Example:**

```
127.0.0.1:5484> SNAPSHOTREAD ON
OK
```

### ZMap

Kronotop also provides a new data structure called ZMap. ZMap is simply a RESP proxy for FoundationDB API.

**One-Off Transactions**

Kronotop will create a new transaction and automatically commit changes to the database for the ad-hoc commands.

In other terms, you do not need to do this for every command:

```
127.0.0.1:5484> BEGIN
OK
127.0.0.1:5484> ZSET mykey "Hello"
OK
127.0.0.1:5484> COMMIT
OK
```

One-off transaction feature is especially useful for running commands in CLI.

**ZMap and Namespaces**

A namespace prefixes all data stored in a ZMap. The current namespace can be inspected by running the following command:

```
127.0.0.1:5484> NAMESPACE CURRENT
global
```

`global` is the default namespace. Namespaces create isolation between data structures. This is how it works for ZMap

```
127.0.0.1:5484> NAMESPACE CURRENT
global
127.0.0.1:5484> ZSET mykey "Hello"
OK
127.0.0.1:5484> ZGET mykey
"Hello"
127.0.0.1:5484> NAMESPACE CREATE global.child-namespace
OK
127.0.0.1:5484> NAMESPACE USE global.child-namespace
OK
127.0.0.1:5484> ZGET mykey
(nil)
```

See [Namespaces](#namespaces) section for further information.

#### ZSET

`ZSET` sets the value for a given key. This will not affect the database until `COMMIT` is called.

**Example:**

```
127.0.0.1:5484> ZSET mykey "Hello"
OK
```

#### ZGET

`ZGET` gets a value from the database. The call will return `nil` if the key is not present in the database.

**Example:**

```
127.0.0.1:5484> ZGET mykey
"Hello"
```

#### ZGETKEY

`ZGETKEY` 

#### ZGETRANGE

#### ZGETRANGESIZE

#### ZMUTATE

#### ZDEL

`ZDEL` clears a given key from the database. This will not affect the database until `COMMIT` is called. 

**Example:**

```
127.0.0.1:5484> ZDEL mykey
OK
```

#### ZDELPREFIX

#### ZDELRANGE

### Namespaces

Namespace is a thin layer around FoundationDB's powerful [directory](https://apple.github.io/foundationdb/developer-guide.html#directories) concept. Namespaces are a recommended approach for administering applications. 
Each application should create or open at least one directory to manage its subspaces. Namespaces are identified by 
hierarchical paths analogous to the paths in a Unix-like file system.

The hierarchy between directories is denoted by joining them with dot character. Let's assume that you have different 
microservices and want to isolate their databases. You can create a namespace for each microservice under the `production` namespace.

If you have three microservices named `users`, `orders` and `products`, your namespace hierarchy might look like the following:

* `production.users`
* `production.orders`
* `production.products`

All data structures used by these namespaces will be isolated.

**Note:** Redis data structures are not stored under namespaces. Currently, namespaces cover only ZMap data structure. 

#### NAMESPACE CREATE

`NAMESPACE CREATE` creates a new namespace with the given hierarchy.

**Example:**

```
127.0.0.1:5484> NAMESPACE CREATE global.users 
```

#### NAMESPACE REMOVE

`NAMESPACE REMOVE` removes the given namespace hierarchy. It's the equivalent of `DROP DATABASE` command in other databases.
So you should be careful when start typing this command. The result is irreversible. 

**Example:**

```
127.0.0.1:5484> NAMESPACE REMOVE global.users
OK 
```

You cannot remove the default namespace:

```
127.0.0.1:5484> NAMESPACE REMOVE global
(error) ERR Cannot remove the default namespace: 'global'
```

#### NAMESPACE CURRENT

`NAMESPACE CURRENT` command returns the current namespace in use for the session. The default namespace is in use if the 
client doesn't set a different namespace by calling `NAMESPACE USE` command.

**Example:**

```
127.0.0.1:5484> NAMESPACE CURRENT
global 
```

#### NAMESPACE USE

`NAMESPACE USE` sets an existing namespace to be used by the session.

**Example:**

```
127.0.0.1:5484> NAMESPACE USE global.users
OK 
```

It returns `NOSUCHNAMESPACE` error if the namespace doesn't exist:

```
127.0.0.1:5484> namespace use global.clients
(error) NOSUCHNAMESPACE No such namespace: global.clients
```

#### NAMESPACE EXISTS

`NAMESPACE EXISTS` checks a namespace whether exists or not.

**Example:**

```
127.0.0.1:5484> NAMESPACE EXISTS global.users
(integer) 1
```

It returns `1` for existing namespaces and `0` for non-existing ones.

#### NAMESPACE LIST

`NAMESPACE LIST` lists the namespaces under the given hierarchy. 

**Example:**

```
127.0.0.1:5484> NAMESPACE LIST
1) global
```

It can take an optional parameter of namespace hierarchy. 

```
127.0.0.1:5484> NAMESPACE LIST global
1) users
```

It returns `NOSUCHNAMESPACE` error if the given hierarchy is invalid:

```
127.0.0.1:5484> NAMESPACE LIST global.clients
(error) NOSUCHNAMESPACE No such namespace: global.clients
```

#### NAMESPACE MOVE

`NAMESPACE MOVE` moves the rightmost namespace to another location in the given hierarchy.

**Example:**

```
127.0.0.1:5484> NAMESPACE MOVE global.users staging.users
OK
```

Let's check the result:

```
127.0.0.1:5484> NAMESPACE LIST global
(empty array)
127.0.0.1:5484> NAMESPACE LIST staging
1) users
```

### Storage Engine

#### Replication

## Architecture
