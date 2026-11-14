---
title: "Volume"
sidebar:
  label: "Overview"
description: "Volume is Kronotop's local storage engine."
---

Volume is Kronotop's local storage engine. It stores document bodies in the local filesystem. All metadata (entry
locations, versioning, segment accounting, and replication state) lives in FoundationDB.

FoundationDB is built for small key-value pairs and limits a value to 100 KB. Document bodies often exceed this limit,
so the volume layer writes them to the local disk and keeps only small pointers in FoundationDB. Metadata gets the
transactional guarantees of FoundationDB, and document content gets fast sequential disk I/O.

Every shard owns exactly one volume. Users work with buckets through `BUCKET.*` commands and do not see volumes during
normal operation. Operators manage volumes with `VOLUME.ADMIN` commands on the management port.

## Dual-storage model

| Layer    | Stored in    | What it holds                                                      |
|----------|--------------|--------------------------------------------------------------------|
| Metadata | FoundationDB | Entry locations, versioning, segment accounting, replication state |
| Content  | Local disk   | Raw document bytes in segment files                                |

On a write, Kronotop appends the content to a segment file and flushes it to disk. It commits the metadata to
FoundationDB in a single transaction only after the flush succeeds, so metadata never points to data that is not on
disk yet.

On a read, Kronotop looks up the entry's metadata to find the segment ID and byte offset, then reads the content
directly from the segment file. Read-only queries take the metadata from a cache. Transactional reads take it from
FoundationDB.

A delete removes the metadata from FoundationDB and leaves the content bytes on disk. That space becomes garbage until
[vacuum](#vacuum-and-space-reclamation) reclaims it.

An update appends the new content to a segment, possibly a different one, and atomically moves the metadata pointer to
the new location. The old content becomes garbage, the same as after a delete.

## Segments

A segment is a fixed-size, pre-allocated, append-only file on the local disk. Entries are written one after another
from the start of the file, and an entry never spans two segments.

When the current segment fills up, Kronotop creates a new one. Older segments only serve reads. The default segment
size for buckets is 4 GiB. You can change it with `volume.segment_size`.

Each segment tracks these space metrics:

| Metric             | Description                                                        |
|--------------------|--------------------------------------------------------------------|
| Cardinality        | Number of live entries in the segment                              |
| Used bytes         | Total bytes occupied by live entries                               |
| Garbage percentage | Fraction of the segment consumed by entries whose metadata is gone |

Use `VOLUME.ADMIN DESCRIBE` to see per-segment statistics:

```kronotop
127.0.0.1:3320> VOLUME.ADMIN DESCRIBE shard-0
```

## Volume status

A volume is in one of three states:

| Status       | Behavior                                 |
|--------------|------------------------------------------|
| `READWRITE`  | Default. Reads and writes are permitted. |
| `READONLY`   | Reads succeed; writes are rejected.      |
| `INOPERABLE` | All operations are rejected.             |

Set a volume to `READONLY` for planned maintenance or before you decommission a node. Use `INOPERABLE` when the
storage has failed or the volume must go fully offline.

```kronotop
127.0.0.1:3320> VOLUME.ADMIN SET-STATUS shard-0 READONLY
OK
```

See [VOLUME.ADMIN SET-STATUS](commands/volume-admin-set-status.md) for the full command reference.

## Replication

Volume replication is asynchronous and runs from the primary to the standbys. Each shard's volume replicates on its
own. Standbys pull data from the primary and keep a copy of all segment content.

Replication has two phases:

1. Segment transfer: when a standby joins or falls behind, it copies the existing segment data from the primary in
   chunks until it reaches the primary's current write position.
2. Change data capture: after it catches up, the standby streams new mutations from a changelog in FoundationDB and
   applies them to its local segments as they arrive.

Kronotop stores replication progress in FoundationDB. A standby can restart at any time and continue where it stopped
without copying data it already has.

Replication starts when cluster routing assigns a standby. Operators can stop and start it by hand for maintenance:

```kronotop
127.0.0.1:3320> VOLUME.ADMIN REPLICATION STOP shard-0
OK
127.0.0.1:3320> VOLUME.ADMIN REPLICATION START shard-0
OK
```

Use [VOLUME.INSPECT REPLICATION](commands/volume-inspect-replication.md) to check the current stage, cursor position,
and status of a standby.

For protocol details, changelog structure, and consistency guarantees, see
[Replication Internals](https://github.com/kronotop/kronotop/blob/main/internals/volume/replication.md).

## Vacuum and space reclamation

Deletes and updates leave unreachable content in segments, and this garbage takes up more disk space over time.
Vacuum reclaims it.

Vacuum scans the segments whose garbage percentage is above a given threshold. It moves their remaining live entries
into the current writable segment and deletes the empty segment files.

To run vacuum, start it with a garbage threshold as a percentage. Only segments above the threshold are processed.
Check progress with `STATUS`. When vacuum completes, `DROP` removes its metadata.

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM START shard-0 30
OK
127.0.0.1:3320> VOLUME.ADMIN VACUUM STATUS shard-0
...
127.0.0.1:3320> VOLUME.ADMIN VACUUM DROP shard-0
OK
```

Only one vacuum can run on a volume at a time.

Changelog pruning is a separate operation. `VOLUME.ADMIN PRUNE-CHANGELOG` removes old replication changelog entries
from FoundationDB to free metadata storage. It does not touch segment files.

For the full vacuum command reference, see [VOLUME.ADMIN VACUUM](commands/volume-admin-vacuum.md). For routine
maintenance procedures, see the [Operations Guide](operations-guide.md).

## Volumes, shards, and buckets

Each shard owns exactly one volume, and volumes are named after their shard: `shard-0`, `shard-1`, and so on. A bucket
spans one or more shards, so its documents are spread across one or more volumes.

Inside a volume, a prefix keeps each bucket's data separate. When you delete a bucket with
[BUCKET.REMOVE](../bucket/commands/bucket-remove.md) and [BUCKET.PURGE](../bucket/commands/bucket-purge.md), Kronotop
cleans up its prefix and data. After a namespace purge, orphaned prefix references may need a manual cleanup scan. See
the [Operations Guide](operations-guide.md#stale-prefix-cleanup) for details.

For sharding and bucket management from the user's side, see [Bucket](../bucket/index.md#sharding).

## Volume lifecycle

A volume has two parts: its metadata in FoundationDB and its segment files on the local disk of a member. Kronotop
creates them at different times.

[KR.ADMIN INITIALIZE-CLUSTER](../admin/cluster/commands/kr-admin-initialize-cluster.md) and
[KR.ADMIN ADD-SHARD](../admin/cluster/commands/kr-admin-add-shard.md) create each shard and its volume metadata in
FoundationDB. They write nothing to disk, and no member has the volume open yet.

Only the owners of a shard open its volume on the physical medium. The owners are the primary and the standbys of the
shard. A member opens the volume when [KR.ADMIN ROUTE](../admin/cluster/commands/kr-admin-route.md) assigns it as the
primary or a standby, and on startup for every shard it already owns. If the volume directory does not exist under the
member's `data_dir`, the member creates it. A member that does not own the shard never opens its volume and keeps no
files for it. When ownership moves to another member, the old owner closes the volume.

`VOLUME.ADMIN` commands work only on volumes that are open on the connected member. On other members they fail with
`ERR Volume: '<name>' is not open`. To see every volume in the cluster, use
[KR.ADMIN DESCRIBE-CLUSTER](../admin/cluster/commands/kr-admin-describe-cluster.md). The `linked_volume` field of each
shard names its volume.

A new shard shows up in `VOLUME.ADMIN LIST` only after a member takes ownership of it:

```kronotop
127.0.0.1:3320> KR.ADMIN ADD-SHARD
(integer) 1
127.0.0.1:3320> VOLUME.ADMIN LIST
1) "shard-0"
127.0.0.1:3320> KR.ADMIN ROUTE SET PRIMARY 1 9578792400e4cd34f9c87700d6592402553117e6
OK
127.0.0.1:3320> VOLUME.ADMIN LIST
1) "shard-0"
2) "shard-1"
```

## Monitoring

The `VOLUME.STATS` commands on the management port report volume health and performance:

| Command                    | Description                                                |
|----------------------------|------------------------------------------------------------|
| `VOLUME.STATS`             | Volume-wide overview: status, capacity, garbage percentage |
| `VOLUME.STATS OPCOUNTERS`  | Operation counters (appends, deletes, reads, updates)      |
| `VOLUME.STATS SEGMENTS`    | Per-segment size, usage, and garbage breakdown             |
| `VOLUME.STATS REPLICATION` | Replication state for a specific standby                   |
| `VOLUME.STATS RESET`       | Reset operation counters to zero                           |

See [VOLUME.STATS Commands](../monitoring/volume/index.md) for the full reference.

## Admin commands

| Command                                                                            | Description                                |
|------------------------------------------------------------------------------------|--------------------------------------------|
| [VOLUME.ADMIN LIST](commands/volume-admin-list.md)                                 | List volumes open on the connected member  |
| [VOLUME.ADMIN DESCRIBE](commands/volume-admin-describe.md)                         | Show metadata and per-segment statistics   |
| [VOLUME.ADMIN SET-STATUS](commands/volume-admin-set-status.md)                     | Change a volume's operational status       |
| [VOLUME.ADMIN LIST-SEGMENTS](commands/volume-admin-list-segments.md)               | List segment IDs for a volume              |
| [VOLUME.ADMIN VACUUM](commands/volume-admin-vacuum.md)                             | Start, stop, or inspect garbage collection |
| [VOLUME.ADMIN REPLICATION](commands/volume-admin-replication.md)                   | Start or stop replication on a standby     |
| [VOLUME.ADMIN PRUNE-CHANGELOG](commands/volume-admin-prune-changelog.md)           | Remove old changelog entries               |
| [VOLUME.ADMIN CLEANUP-ORPHAN-FILES](commands/volume-admin-cleanup-orphan-files.md) | Remove orphaned segment files from disk    |
| [VOLUME.INSPECT REPLICATION](commands/volume-inspect-replication.md)               | Inspect replication state for a standby    |
| [VOLUME.INSPECT CURSOR](commands/volume-inspect-cursor.md)                         | Show the write cursor for a volume         |

For step-by-step maintenance procedures, see the [Operations Guide](operations-guide.md).
