# Volume management

* [Introduction](#introduction)
* [Commands](#commands)
  * [LIST](#list)
  * [DESCRIBE](#describe)
  * [SET-STATUS](#set-status)
  * [REPLICATIONS](#replications)
  * [VACUUM](#vacuum)
  * [STOP-VACUUM](#stop-vacuum)
  * [CLEANUP-ORPHAN-FILES](#cleanup-orphan-files)
  * [MARK-STALE-PREFIXES](#mark-stale-prefixes)
  
## Introduction

Kronotop uses a custom-built storage engine named Volume. As the core persistence layer for this distributed, transactional 
document store, Volume is responsible for reliably storing all document data on local disks and managing data replication 
between cluster members.

See the [volume](volume.md) documentation for details of the storage engine design and implementation.

## Commands

Volume administration commands are exposed through the **management interface**, which listens on a dedicated TCP port. 
By default, this port is `3320`.

To interact with the cluster using admin commands, you can use the standard `redis-cli` tool:

```shell
redis-cli -3 -p 3320
```

This connects to the management port with RESP3 support enabled (-3), allowing you to issue volume-level administrative commands directly.

Volume admin commands are designed as a subcommand of `VOLUME.ADMIN` command

### LIST

`VOLUME.ADMIN LIST`  command returns a list of all volumes currently **opened and managed** by the cluster member you are connected to.

This provides a snapshot of the local volume state, which is useful for inspecting active shards, verifying deployments, 
or diagnosing issues related to volume allocation.

**Syntax**

```
VOLUME.ADMIN LIST
```

**Example**

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

`VOLUME.ADMIN DESCRIBE` command provides detailed information about the internal state and layout of a specific volume (shard). 
This includes storage path, status, segment size, and statistics per segment.

**Syntax**

```
VOLUME.ADMIN DESCRIBE
```

***Output Fields*** 

* `name`: Name of the volume (e.g., bucket-shard-1).
* `status`: Current status of the volume. Possible values:
    - `READONLY`
    - `READWRITE`
    - `INOPERABLE`
* `data_dir`: Path to the directory where this shard's data is stored on disk.
* `segment_size`: Size (in bytes) of each storage segment. This determines the maximum data a single segment can contain.
* `segments`: A list of segment entries, keyed by segment ID (e.g., 0000000000000000005). Each segment includes:
  * `size`: Total size (in bytes) of the segment. Should match `segment_size`.
  * `free_bytes`: Number of unused bytes in the segment.
  * `used_bytes`: Number of bytes currently used to store data.
  * `garbage_ratio`: The ratio of reclaimable (deleted or overwritten) bytes to total segment size. A high ratio may indicate the need for vacuum.
  * `cardinality`: Number of unique keys stored in the segment.

**Example**

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

**Syntax**

```
VOLUME.ADMIN SET-STATUS <bucket-name> <valid-status-value>
```

**Valid Status Values**

* `READONLY`: The volume is set to read-only. Write operations will be rejected.
* `READWRITE`: The volume is fully operational. Both read and write operations are allowed.
* `INOPERABLE`: The volume is marked as non-operational. All operations will be rejected.

**Example**

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
127.0.0.1:3320> VOLUME-ADMIN SET-STATUS bucket-shard-100 READONLY
(error) ERR Volume: 'bucket-shard-100' is not open
```

### REPLICATIONS

`VOLUME.ADMIN REPLICATIONS` command returns detailed metadata for all active or historical replication sessions 
involving the current node. It is primarily used for debugging and monitoring the state of replication pipelines.

**Syntax**

```
VOLUME.ADMIN REPLICATIONS
```

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

**Example**

```
127.0.0.1:3320> VOLUME.ADMIN REPLICATIONS
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

### VACUUM

`VOLUME.ADMIN VACUUM` command initiates a manual vacuum operation on the specified volume (shard). It is used to reclaim 
disk space by removing segments with excessive obsolete (garbage) data.

**Syntax:**

```
VOLUME.ADMIN VACUUM <volume-name> <allowed-garbage-ratio>
```

**Arguments**

* `volume-name`: Name of the volume to vacuum (e.g., bucket-shard-1).
* `allowed-garbage-ratio`: A floating-point value (e.g., 10.2) representing the minimum percentage of garbage in a segment required 
for it to be considered eligible for vacuuming. Segments with lower ratios are ignored.

**Behavior**

* Scans all segments of the given volume.
* Selects segments where `garbage_ratio >= allowed-garbage-ratio`.
* Migrates live data to new segments and deletes the old ones.
* Frees up disk space and improves overall segment efficiency.

**Example**

```
127.0.0.1:3320> VOLUME.ADMIN VACUUM bucket-shard-1 10.2
OK
```

This command will vacuum only those segments in *bucket-shard-1* that have a garbage ratio equal to or higher than 10.2%.

**Error Cases**

If the volume does not exist or is not open:

```
127.0.0.1:3320> VOLUME.ADMIN VACUUM bucket-shard-110 10.2
(error) ERR Volume: 'bucket-shard-110' is not open
```

If `allowed-garbage-ratio` argument is not a double or out of range:

```
127.0.0.1:3320> VOLUME.ADMIN VACUUM bucket-shard-1 foo
(error) ERR value is not a double or out of range
```

**Notes**

Vacuuming is an **explicit, manual** operation in Kronotop. There is **no automatic vacuuming** at this time.

It can be safely run while the volume is online and serving read/write traffic.

To assess whether vacuuming is needed, use the [VOLUME.ADMIN DESCRIBE](#describe) command and check `garbage_ratio` values per segment.

### STOP-VACUUM

`VOLUME.ADMIN STOP VACUUM` command stops a running vacuum command on the specified volume.

**Syntax:**

```
VOLUME.ADMIN VACUUM STOP-VACUUM <volume-name>
```

**Arguments**

* `volume-name`: Name of the volume to vacuum (e.g., bucket-shard-1).

**Example**

```
127.0.0.1:3320> VOLUME.ADMIN STOP-VACUUM bucket-shard-1
OK
```

**Error Cases**

If there is no vacuum task found on the specified volume:

```
127.0.0.1:3320> VOLUME.ADMIN STOP-VACUUM bucket-shard-1
(error) ERR Vacuum task not found on bucket-shard-1
```

If the specified volume is not open or does not exist:

```
127.0.0.1:3320> volume.admin stop-vacuum bucket-shard-110
(error) ERR Volume: 'bucket-shard-110' is not open
```

### CLEANUP-ORPHAN-FILES

The `VOLUME.ADMIN CLEANUP-ORPHAN-FILES` command performs a manual cleanup of orphaned files on the specified volume (shard). 
Orphan files are leftover segment or metadata files that are no longer tracked by the volume's internal state—typically 
resulting from crashes, interrupted operations, or failed vacuum cycles.

**Syntax:**

```
VOLUME.ADMIN CLEANUP-ORPHAN-FILES <volume-name>
```

**Arguments**

* `volume-name`: Name of the volume to scan and clean (e.g., bucket-shard-1).

**Example**

```
127.0.0.1:3320> VOLUME.ADMIN CLEANUP-ORPHAN-FILES bucket-shard-1
  1) kronotop-data/development/99f14bd9e6f9e95953c2f0740846b08508eb97b4/bucket/shards/1/segments/0000000000000000037
  2) kronotop-data/development/99f14bd9e6f9e95953c2f0740846b08508eb97b4/bucket/shards/1/segments/0000000000000000038
```

**Behavior**

* Scans the volume’s data directory on disk.
* Compares actual files on the filesystem with the volume's metadata on FoundationDB.
* Deletes these orphaned files permanently to reclaim disk space and maintain storage hygiene.

**Error Cases**

If the specified volume is not open or does not exist:

```
127.0.0.1:3320> VOLUME.ADMIN CLEANUP-ORPHAN-FILES bucket-shard-10
(error) ERR Volume: 'bucket-shard-10' is not open
```

**Notes**

* This is a **manual** operation; Kronotop does not automatically detect or delete orphaned files.
* Safe to execute while the volume is online and serving traffic. However, it’s recommended to monitor logs for any anomalies before and after cleanup.
* This operation does not touch valid segments, even if they appear unused—only unreferenced files are deleted.
* Use in conjunction with `VOLUME.ADMIN DESCRIBE` and `ls -l` on the data directory to verify disk state before/after cleanup if needed.

### MARK-STALE-PREFIXES

`VOLUME.ADMIN MARK-STALE-PREFIXES` command starts a background task that scans the metadata of all volumes to identify and 
mark stale key prefixes.

In Kronotop, a prefix is the logical grouping for keys stored in a volume. Over time, some of these prefixes may become 
obsolete due to deletions. These stale prefixes must be explicitly marked before a vacuum operation can safely remove 
related data from the disk.

This marking process ensures that:

* Segments containing only stale keys are eligible for garbage collection.
* Prefix-level tracking remains consistent with the actual key state.

**Behavior**

* Scans all metadata entries across all volumes.
* Detects prefixes that are no longer active or referenced.
* Marks them internally as **stale**, making associated data eligible for cleanup.
* Runs asynchronously in the background and may take time depending on dataset size.

**Syntax:**

```
VOLUME.ADMIN MARK-STALE-PREFIXES <argument> 
```

**Arguments**

* `START`: Initiates the background scanning task. Required before running VACUUM.
* `STOP`: Gracefully stops the ongoing stale prefix scan, if any.
* `REMOVE`: Removes a stopped task completely.


**Example**

```
127.0.0.1:3320> VOLUME.ADMIN MARK-STALE-PREFIXES START
OK
```

**NOTES**

* Marking stale prefixes is a required prerequisite for a successful `VOLUME.ADMIN VACUUM` operation.
* Running this command does not remove any data by itself—it only flags candidates for future cleanup.
* For environments with frequent data churn, it is recommended to periodically mark stale prefixes before initiating vacuum cycles.
* Only one instance of the scan can run at a time.