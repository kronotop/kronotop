# Volume management

## Introduction

Kronotop uses a custom-built storage engine named Volume. As the core persistence layer for this distributed, transactional 
document store, Volume is responsible for reliably storing all document data on local disks and managing data replication 
between cluster members.

See the [design](design.md) document in for details of the volume design and implementation.

## Commands

Volume admin commands have been exposed via the management port. The default port for management commands is `3320`.

Use `redis-cli` command to access the cluster:

```shell
redis-cli -3 -p 3320
```

Volume admin commands are designed as a subcommand of `volume.admin` command

### LIST

`VOLUME.ADMIN LIST` command lists the currently open volumes by the cluster member you connected.

```
127.0.0.1:3320> VOLUME.ADMIN LIST
 1) redis-shard-0
 2) bucket-shard-0
 3) bucket-shard-1
 4) bucket-shard-6
 5) redis-shard-6
 6) redis-shard-5
 7) redis-shard-4
 8) bucket-shard-2
 9) redis-shard-3
10) bucket-shard-3
11) redis-shard-2
12) bucket-shard-4
13) redis-shard-1
14) bucket-shard-5
```

### DESCRIBE

`VOLUME.ADMIN DESCRIBE` command returns all metadata information of the specified volume.

```
127.0.0.1:3320> VOLUME.ADMIN DESCRIBE bucket-shard-1
1# name => bucket-shard-1
2# status => READWRITE
3# data_dir => kronotop-data/development/99f14bd9e6f9e95953c2f0740846b08508eb97b4/bucket/shards/1
4# segment_size => (integer) 1048576
5# segments =>
   1# 0000000000000000005 =>
      1# size => (integer) 1048576
      2# free_bytes => (integer) 1048528
      3# used_bytes => (integer) 48
      4# garbage_ratio => (double) 0.0
      5# cardinality => (integer) 2
127.0.0.1:3320>
```

***Error Cases***

It returns an error if there is no volume given name or the name is invalid:

```
127.0.0.1:3320> VOLUME.ADMIN DESCRIBE bucket-shard-100
(error) ERR Volume: 'bucket-shard-100' is not open
```

### SET-STATUS

`VOLUME-ADMIN SET-STATUS` command changes the status of the specified volume.


**Valid Status Values**

* `READONLY`: The volume is set to read-only. Write operations will be rejected.
* `READWRITE`: The volume is fully operational. Both read and write operations are allowed.
* `INOPERABLE`: The volume is marked as non-operational. All operations will be rejected.

```
127.0.0.1:3320> VOLUME-ADMIN SET-STATUS bucket-shard-1 READONLY
OK
```

***Error Cases***

If an invalid status is provided:

```
127.0.0.1:3320> VOLUME-ADMIN SET-STATUS bucket-shard-1 READ
(error) ERR Invalid volume status: READ
```

If the volume does not exist or is not open:

```
127.0.0.1:3320> volume.admin set-status bucket-shard-100 READONLY
(error) ERR Volume: 'bucket-shard-100' is not open
```

### REPLICATIONS

`VOLUME.ADMIN REPLICATIONS` command returns detailed metadata for all active or historical replication sessions 
involving the current node. It is primarily used for debugging and monitoring the state of replication pipelines.

**Output Fields**

The keys of the root hash are the replication slot ids. Each entry corresponds to a replication context and includes the following fields:

* `shard_kind`: The type of shard being replicated (e.g., REDIS, BUCKET, etc.).
* `shard_id`: Unique integer ID of the shard.
* `active`: Indicates whether the replication session is currently active (true or false).
* `stale`: If true, the replication session is outdated and should be discarded.
* `replication_stage`: Current stage of the replication process (SNAPSHOT, STREAMING, etc.).
* `completed_stages`: List of stages that have already been completed.
* `latest_segment_id`: ID of the most recent segment received.
* `received_versionstamped_key`: Versionstamped key of the latest data received.
* `latest_versionstamped_key`: Versionstamped key of the latest data applied.

```
127.0.0.1:3320> VOLUME.ADMIN replications
1# 000035QAH1NMI0000000xxxx =>
   1# shard_kind => REDIS
   2# shard_id => (integer) 1
   3# active => (true)
   4# stale => (false)
   5# replication_stage => STREAMING
   6# completed_stages => 1) SNAPSHOT
   7# latest_segment_id => (integer) 0
   8# received_versionstamped_key =>
   9# latest_versionstamped_key =>
```

This command is useful for tracking the progress and health of replication sessions across distributed nodes.
