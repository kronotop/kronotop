# Kronotop

Kronotop is a distributed and transactional document database backed by [FoundationDB](https://www.foundationdb.org/).

Kronotop project aims to build a document database that supports [MQL-like query language](https://www.mongodb.com/docs/manual/reference/operator/), [ACID transactions](https://apple.github.io/foundationdb/developer-guide.html#transaction-basics)
and on-disk storage engine with a primary-standby replication model.

Kronotop is still in its early stages of development. The API is unstable and might be changed in the future releases.

See [Getting started](#getting-started) section.

Join the [Discord channel](https://discord.gg/Nyy4Afpr) to discuss.

## At a glance

* Uses [RESP3](https://redis.io/docs/latest/develop/reference/protocol-spec/) as the wire protocol, so it is compatible with all Redis clients,
* Horizontally scalable and sharded by default,
* Supports *single* or *multi-master* cluster topologies for different deployment strategies and use cases,
* Partly supports [Redis cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/),
* Uses FoundationDB as the metadata store for cluster management and data structures,
* Implemented in Java and requires JDK 21+,

## What we have

Kronotop is still in its early stages, but we have the following features with a strong foundation.

* ZMap, an ordered key-value store. Simply, Redis protocol proxy for FoundationDB API,
* Namespaces for isolating ZMaps and Buckets, basically it's a thin layer around FoundationDB's directory layer,
* Volume, storage engine implementation with a primary-standby replication model,
* Clustering with single or multi-master deployment scenarios,
* Partial support for some Redis data structures: String and Hash.

## Plans for the foreseeable future

* Design and implement a data structure called **Bucket** to store JSON-like documents,
* **Bucket** data structure will support
  an [MQL-like query language](https://www.mongodb.com/docs/manual/reference/operator/) and transactions backed by
  FoundationDB,
* Provide the most common Redis data structures such as String, Hash, Sorted Sets, etc.

## Table of Contents

* [Getting started](#getting-started)
    * [Initializing a Kronotop cluster](#initializing-a-kronotop-cluster)
* [Redis compatibility](#redis-compatibility)
* [Features](#features)
    * [Transaction management](#transaction-management)
        * [BEGIN](#begin)
        * [COMMIT](#commit)
        * [ROLLBACK](#rollback)
        * [GETAPPROXIMATESIZE](#getapproximatesize)
        * [GETREADVERSION](#getreadversion)
        * [SNAPSHOTREAD](#snapshotread)
    * [ZMap](#zmap)
        * [ZSET](#zset)
        * [ZGET](#zget)
        * [ZGETKEY](#zgetkey)
        * [ZGETRANGE](#zgetrange)
        * [ZGETRANGESIZE](#zgetrangesize)
        * [ZMUTATE](#zmutate)
        * [ZDEL](#zdel)
        * [ZDELRANGE](#zdelrange)
    * [Namespaces](#namespaces)
        * [NAMESPACE CREATE](#namespace-create)
        * [NAMESPACE REMOVE](#namespace-remove)
        * [NAMESPACE CURRENT](#namespace-current)
        * [NAMESPACE USE](#namespace-use)
        * [NAMESPACE EXISTS](#namespace-exists)
        * [NAMESPACE LIST](#namespace-list)
        * [NAMESPACE MOVE](#namespace-move)
* [Storage Engine](#storage-engine)
    * [Design](#design)
    * [Replication](#replication)
    * [Vacuuming](#vacuuming)

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
creates
the cluster's layout on the FoundationDB and initializes the cluster:

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

All in-memory data will be persisted and replicated by the storage engine. See [Storage Engine](#storage-engine) section
for the details.

## Redis compatibility

Kronotop uses RESP3 as the client protocol. The reasoning behind this is simple: there are many high-quality Redis
client implementations
in all languages, and almost everyone has some experience with Redis.

Despite the main focus on building a transactional document database using FoundationDB as a metadata store,
implementing
the most common Redis data structures is on the roadmap. Currently, Kronotop has already partial support for *String*
and *Hash*
data structures.

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

`COMMIT` commits changes to the database.

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

`GETAPPROXIMATESIZE` returns the approximated size of the commit, which is the summation of mutations, read conflict
ranges,
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

In the FoundationDB context, snapshots a special-purpose, read-only view of the database. Reads done through this
interface
are known as "snapshot reads." Snapshot reads selectively relax FoundationDB's isolation property, reducing [Transaction
conflicts](https://apple.github.io/foundationdb/developer-guide.html#conflict-ranges) but making reasoning about
concurrency harder. For more information about how to use snapshot reads correctly,
see Using [snapshot reads](https://apple.github.io/foundationdb/developer-guide.html#snapshot-reads).

**Example:**

```
127.0.0.1:5484> SNAPSHOTREAD ON
OK
```

### ZMap

Kronotop also provides a new data structure called ZMap. ZMap is simply a RESP proxy for FoundationDB API.

FoundationDB’s core data model is an ordered key-value store. Also known as an ordered associative array, map, or
dictionary,
this is a common data structure composed of a collection of key-value pairs in which all keys are unique. Starting with
this simple model,
an application can create higher-level data models by mapping their elements to individual keys and values.

See the [Data Modeling](https://apple.github.io/foundationdb/data-modeling.html) section on FoundationDB documents.

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

`ZGETKEY` returns the key referenced by the specified `KeySelector`.

The default key selector is `FIRST_GREATER_OR_EQUAL`, `KEY_SELECTOR` portion of the command is optional.

Available key selectors:

* `FIRST_GREATER_OR_EQUAL`,
* `FIRST_GREATER_THAN`,
* `LAST_LESS_THAN`,
* `LAST_LESS_OR_EQUAL`

**Example:**

```
127.0.0.1:5484> zset key-0 value-0
OK
127.0.0.1:5484> zset key-1 value-1
OK
127.0.0.1:5484> zset key-2 value-2
OK
127.0.0.1:5484> zset key-3 value-3
OK
127.0.0.1:5484> ZGETKEY key-0 KEY_SELECTOR FIRST_GREATER_THAN
"key-1"
```

#### ZGETRANGE

`ZGETRANGE` gets an ordered range of keys and values from the database. The *begin* and *end* keys can be specified by
key selectors, with the begin `BEGIN_KEY_SELECTOR` inclusive and the end `END_KEY_SELECTOR` exclusive.

The default `BEGIN_KEY_SELECTOR` is `FIRST_GREATER_OR_EQUAL`.
The default `END_KEY_SELECTOR` is `FIRST_GREATER_THAN`.

Available key selectors:

* FIRST_GREATER_OR_EQUAL,
* FIRST_GREATER_THAN,
* LAST_LESS_THAN,
* LAST_LESS_OR_EQUAL

The key selector section of this command is optional.

**Example:***

```
127.0.0.1:5484> ZGETRANGE key-0 key-3
1) 1) "key-0"
   2) "value-0"
2) 1) "key-1"
   2) "value-1"
3) 1) "key-2"
   2) "value-2"
```

With `LIMIT` parameter:

```
127.0.0.1:5484> ZGETRANGE key-0 key-5 LIMIT 3
1) 1) "key-0"
   2) "value-0"
2) 1) "key-1"
   2) "value-1"
3) 1) "key-2"
   2) "value-2"
```

With `LIMIT` and `REVERSE` parameters:

```
127.0.0.1:5484> ZGETRANGE key-0 key-5 LIMIT 3 REVERSE
1) 1) "key-4"
   2) "value-4"
2) 1) "key-3"
   2) "value-3"
3) 1) "key-2"
   2) "value-2"
```

Range from beginning to end, get an ordered set or key/value pairs:

```
127.0.0.1:5484> ZGETRANGE * *
1) 1) "key-0"
   2) "value-0"
2) 1) "key-1"
   2) "value-1"
3) 1) "key-2"
   2) "value-2"
4) 1) "key-3"
   2) "value-3"
5) 1) "key-4"
   2) "value-4"
6) 1) "key-5"
   2) "value-5"
7) 1) "key-6"
   2) "value-6"
```

Explicitly using key selectors, please note that you can use the key selectors individually.

```
127.0.0.1:5484> ZGETRANGE key-2 key-5 BEGIN_KEY_SELECTOR FIRST_GREATER_THAN
1) 1) "key-3"
   2) "value-3"
2) 1) "key-4"
   2) "value-4"
```

#### ZGETRANGESIZE

`ZGETRANGESIZE` gets an estimate for the number of bytes stored in the given range.

```
127.0.0.1:5484> zgetrangesize key-0 key-9
(integer) 0
```

_FoundationDB says:_

> Note: the estimated size is calculated based on the sampling done by FDB server. The sampling algorithm works roughly
> in
> this way: the larger the key-value pair is, the more likely it would be sampled and the more accurate its sampled size
> would be.
> And due to that reason, it is recommended to use this API to query against large ranges for accuracy considerations.
> For a rough reference, if the returned size is larger than 3MB, one can consider the size to be accurate.

#### ZMUTATE

`ZMUTATE` runs an atomic operation on the given key.

Available mutation types:

* `ADD`: Performs an addition of little-endian integers.
* `APPEND_IF_FITS`: Appends param to the end of the existing value already in the database at the given key (or creates
  the key and sets the value to param if the key is empty).
* `BIT_AND`: Performs a bitwise and operation.
* `BIT_OR`: Performs a bitwise or operation.
* `BIT_XOR`: Performs a bitwise xor operation.
* `BYTE_MAX`: Performs lexicographic comparison of byte strings.
* `BYTE_MIN`: Performs lexicographic comparison of byte strings.
* `COMPARE_AND_CLEAR`: Performs an atomic compare and clear operation.
* `MAX`: Performs a little-endian comparison of byte strings.
* `MIN`: Performs a little-endian comparison of byte strings.
* `SET_VERSIONSTAMPED_KEY`: Transforms key using a versionstamp for the transaction.
* `SET_VERSIONSTAMPED_VALUE`: Transforms param using a versionstamp for the transaction.

**Example:**

```
127.0.0.1:5484> ZSET key-0 value-0
OK
127.0.0.1:5484> ZMUTATE key-0 value COMPARE_AND_CLEAR
OK
127.0.0.1:5484> ZGET key-0
"value-0"
127.0.0.1:5484> ZMUTATE key-0 value-0 COMPARE_AND_CLEAR
OK
127.0.0.1:5484> ZGET key-0
(nil)
```

#### ZDEL

`ZDEL` clears a given key from the database. This will not affect the database until `COMMIT` is called.

**Example:**

```
127.0.0.1:5484> ZDEL mykey
OK
```

#### ZDELRANGE

`ZDELRANGE` clears the keys in the given range. The *begin* is inclusive, *end* is exclusive.

**Example:**

```
127.0.0.1:5484> ZDELRANGE key-1 key-4
OK
127.0.0.1:5484> ZGETRANGE * *
1) 1) "key-0"
   2) "value-0"
2) 1) "key-4"
   2) "value-4"
3) 1) "key-5"
   2) "value-5"
4) 1) "key-6"
   2) "value-6"
```

Using `*` asterisk character to set a boundary:

Wipe out all keys in the current namespace:

```
127.0.0.1:5484> ZDELRANGE * *
OK
127.0.0.1:5484> ZGETRANGE * *
(empty array)
```

Clear all keys from the beginning to `key-3`:

```
127.0.0.1:5484> ZDELRANGE * key-3
OK
127.0.0.1:5484> ZGETRANGE * *
1) 1) "key-3"
   2) "value-3"
2) 1) "key-4"
   2) "value-4"
3) 1) "key-5"
   2) "value-5"
```

Clear all keys from `key-3` to end of the range:

```
127.0.0.1:5484> ZDELRANGE key-3 *
OK
127.0.0.1:5484> ZGETRANGE * *
1) 1) "key-0"
   2) "value-0"
2) 1) "key-1"
   2) "value-1"
3) 1) "key-2"
   2) "value-2"
```

### Namespaces

Namespace is a thin layer around FoundationDB's
powerful [directory](https://apple.github.io/foundationdb/developer-guide.html#directories) concept. Namespaces are a
recommended approach for administering applications.
Each application should create or open at least one directory to manage its subspaces. Namespaces are identified by
hierarchical paths analogous to the paths in a Unix-like file system.

The hierarchy between directories is denoted by joining them with dot character. Let's assume that you have different
microservices and want to isolate their databases. You can create a namespace for each microservice under the
`production` namespace.

If you have three microservices named `users`, `orders` and `products`, your namespace hierarchy might look like the
following:

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

`NAMESPACE REMOVE` removes the given namespace hierarchy. It's the equivalent of `DROP DATABASE` command in other
databases.
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

## Storage Engine

### Design

Kronotop has a storage engine implementation named **Volume**. It's responsible for storing all data on the disks and
replicate it from the primary members to the standbys.

Volume has a simple yet elegant design philosophy. It stores all metadata information on the FoundationDB cluster. The
data itself has
stored on the disk and organized as **segments**.

A segment is just a pre-allocated file on the disk and is seen as a huge byte array. When you want to append some
entries to the volume,
all data has been appended to the latest segment and then flushed. If the flush call succeeds, the metadata has been
committed to the
FoundationDB cluster in a single transaction. This process could be depicted as the following for easy digestion:

- An outer layer of Kronotop tries to append some data to the volume,
- Volume receives the request and finds the latest segment to append the data,
- If there is no open segment, a new segment will be created,
- If the latest segment doesn't have enough free space, a new segment will be created and the previous segment marked as
  readonly,
- Segment implementation will find a location for the given amount of data and append it to the underling pre-allocated
  file,
- The modified segment will be flushed to ensure all buffered changes are written to disk.
- Volume implementation will commit metadata changes in a single transaction and will return IDs in FoundationDBs
  version stamp format.

### Replication

The **asynchronous** replication process is also handled by the volume itself. When a new entry has been added to the
volume, an entry will be added to the
data structure named *SegmentLog* and standbys will be triggered to fetch the changes from the primary. Volume uses
the [watch mechanism](https://apple.github.io/foundationdb/developer-guide.html#watches)
to implement this feature.

The standby member copies all data from the primary, this happens in two stages:

* Snapshot
* Streaming

In *snapshot* stage, the standby volume reads the `SegmentLog` data structure and copies all data chunk by chunk.
When the snapshot stage has completed, the *streaming* stage has started and a `watch` monitors a key constantly to
fetch the
latest changes from the primary.

Volume also supports synchronous replication, but it's a part of Redis Volume Syncer mechanism.

### Vacuuming

Volume supports manual vacuuming. A system administrator can start a vacuum process, and it has been processed in the
background.
Vacuum process scans the Volume metadata and evicts the entries from a segment if the garbage ratio of the segment
exceeds a certain number.

