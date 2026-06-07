---
title: "Volume"
sidebar:
  label: "Overview"
description: "Volume is Kronotop's local storage engine."
---

---

## Overview

Volume is Kronotop's local storage engine. It stores document body content in the local filesystem while all metadata
(entry locations, versioning, segment accounting, and replication state) lives in FoundationDB.

FoundationDB is optimized for small key-value pairs and enforces a 100 KB value-size limit. Document bodies routinely
exceed this limit, so the volume layer offloads bulk content to the local disk and keeps only lightweight pointers in
FoundationDB. This gives Kronotop the transactional guarantees of FoundationDB for metadata with high-throughput
sequential I/O for document content.

Every shard owns exactly one volume. During normal operation, users interact with buckets through `BUCKET.*` commands
and never see volumes directly. Operators manage volumes through `VOLUME.ADMIN` commands on the management port.

---

## Dual-Storage Model

| Layer        | Stored In    | What It Holds                                                      |
|--------------|--------------|--------------------------------------------------------------------|
| **Metadata** | FoundationDB | Entry locations, versioning, segment accounting, replication state |
| **Content**  | Local disk   | Raw document bytes in segment files                                |

**Write path:** Content is appended to a segment file and flushed to disk first. Only after the flush succeeds is the
metadata committed to FoundationDB in a single transaction. This ordering guarantees that metadata never references
data that has not been persisted.

**Read path:** The entry's metadata is looked up, either from a cache for read-only queries or from FoundationDB for
transactional reads, to find the segment ID and byte offset. The content is then read directly from the segment file.

**Deletes** remove metadata from FoundationDB but leave the content bytes on disk. The space they occupied becomes
garbage, reclaimed later by [vacuum](#vacuum-and-space-reclamation).

**Updates** append the new content to a segment (possibly a different one) and atomically swap the metadata pointer to
the new location. The old content becomes garbage, just like a logical delete.

---

## Segments

A segment is a fixed-size, pre-allocated, append-only file on the local disk. Entries are written sequentially from the
beginning of the file. An entry is never split across segments.

When the current segment fills up, a new segment is created automatically. Old segments accept no further writes. They
serve only reads. The default segment size for buckets is 4 GiB, configurable via `bucket.volume.segment_size`.

Each segment tracks space accounting metrics:

| Metric                 | Description                                                        |
|------------------------|--------------------------------------------------------------------|
| **Cardinality**        | Number of live entries in the segment                              |
| **Used bytes**         | Total bytes occupied by live entries                               |
| **Garbage percentage** | Fraction of the segment consumed by entries whose metadata is gone |

Use `VOLUME.ADMIN DESCRIBE` to inspect per-segment statistics:

```kronotop
127.0.0.1:3320> VOLUME.ADMIN DESCRIBE bucket-shard-0
```

---

## Volume Status

A volume operates in one of three states:

| Status       | Behavior                                 |
|--------------|------------------------------------------|
| `READWRITE`  | Default. Reads and writes are permitted. |
| `READONLY`   | Reads succeed; writes are rejected.      |
| `INOPERABLE` | All operations are rejected.             |

Set a volume to `READONLY` for planned maintenance or before decommissioning a node. Use `INOPERABLE` when the
underlying storage has failed or the volume must be taken fully offline.

```kronotop
127.0.0.1:3320> VOLUME.ADMIN SET-STATUS bucket-shard-0 READONLY
OK
```

See [VOLUME.ADMIN SET-STATUS](commands/volume-admin-set-status.md) for the full command reference.

---

## Replication

Volume replication is an asynchronous, primary-to-standby system. Each shard's volume is replicated independently:
standby nodes pull data from the primary to maintain a copy of all segment content.

Replication proceeds in two phases:

1. **Segment transfer**: When a standby joins or falls behind, it copies existing segment data from the primary in
   chunks until it reaches the primary's current write position.
2. **Change data capture**: Once caught up, the standby continuously streams incremental mutations from a changelog
   maintained in FoundationDB, applying them to its local segments in real time.

All replication progress is persisted in FoundationDB, so a standby can restart at any time and resume from exactly
where it left off without re-transferring data it already has.

Replication starts automatically when a standby is assigned via cluster routing. Operators can stop and start it
manually for maintenance:

```kronotop
127.0.0.1:3320> VOLUME.ADMIN REPLICATION STOP bucket-shard-0
OK
127.0.0.1:3320> VOLUME.ADMIN REPLICATION START bucket-shard-0
OK
```

Use [VOLUME.INSPECT REPLICATION](commands/volume-inspect-replication.md) to check the current stage, cursor position,
and status of a standby.

For protocol details, changelog structure, and consistency guarantees, see
[Replication Internals](https://github.com/kronotop/kronotop/blob/main/internals/volume/replication.md).

---

## Vacuum and Space Reclamation

Deletes and updates leave behind unreachable content in segments. Over time this garbage accumulates, consuming disk
space that could be reclaimed. Vacuum is the process that reclaims it.

Vacuum scans segments whose garbage percentage exceeds a given threshold, evacuates their remaining live entries into
the
current writable segment, and destroys the emptied segment files. This consolidates live data and frees disk space.

The operator workflow is:

1. **Start**: Launch vacuum with a garbage threshold (percentage). Only segments above this threshold are processed.
2. **Monitor**: Check progress with `STATUS`.
3. **Clean up**: After completion, `DROP` removes the vacuum metadata.

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM START bucket-shard-0 30
OK
127.0.0.1:3320> VOLUME.ADMIN VACUUM STATUS bucket-shard-0
...
127.0.0.1:3320> VOLUME.ADMIN VACUUM DROP bucket-shard-0
OK
```

Only one vacuum can run per volume at a time.

**Changelog pruning** is a separate, complementary operation. `VOLUME.ADMIN PRUNE-CHANGELOG` removes old replication
changelog entries from FoundationDB to reclaim metadata storage. It does not affect segment files.

For the full vacuum command reference, see
[VOLUME.ADMIN VACUUM](commands/volume-admin-vacuum.md). For routine maintenance procedures, see the
[Operations Guide](operations-guide.md).

---

## Volumes, Shards, and Buckets

Each shard owns exactly one volume. A bucket spans one or more shards, so a bucket's documents are distributed across
one or more volumes. Volumes are named after their shard: `bucket-shard-0`, `bucket-shard-1`, and so on.

Within a volume, each bucket's data is isolated by a prefix. When a bucket is deleted via
[BUCKET.REMOVE](../bucket/commands/bucket-remove.md) and [BUCKET.PURGE](../bucket/commands/bucket-purge.md), its prefix
and associated data are cleaned up automatically.
After namespace-level purges, orphaned prefix references may require a manual cleanup scan. See
the [Operations Guide](operations-guide.md#stale-prefix-cleanup) for details.

For the user-facing perspective on sharding and bucket management, see [Bucket](../bucket/index.md#sharding).

---

## Monitoring

Volume health and performance can be inspected through the `VOLUME.STATS` family of commands on the management port:

| Command                    | Description                                                |
|----------------------------|------------------------------------------------------------|
| `VOLUME.STATS`             | Volume-wide overview: status, capacity, garbage percentage |
| `VOLUME.STATS OPCOUNTERS`  | Operation counters (appends, deletes, reads, updates)      |
| `VOLUME.STATS SEGMENTS`    | Per-segment size, usage, and garbage breakdown             |
| `VOLUME.STATS REPLICATION` | Replication state for a specific standby                   |
| `VOLUME.STATS RESET`       | Reset operation counters to zero                           |

See [VOLUME.STATS Commands](../monitoring/volume/index.md) for the full reference.

---

## Admin Commands

| Command                                                                            | Description                                |
|------------------------------------------------------------------------------------|--------------------------------------------|
| [VOLUME.ADMIN LIST](commands/volume-admin-list.md)                                 | List all volumes on the connected member   |
| [VOLUME.ADMIN DESCRIBE](commands/volume-admin-describe.md)                         | Show metadata and per-segment statistics   |
| [VOLUME.ADMIN SET-STATUS](commands/volume-admin-set-status.md)                     | Change a volume's operational status       |
| [VOLUME.ADMIN LIST-SEGMENTS](commands/volume-admin-list-segments.md)               | List segment IDs for a volume              |
| [VOLUME.ADMIN VACUUM](commands/volume-admin-vacuum.md)                             | Start, stop, or inspect garbage collection |
| [VOLUME.ADMIN REPLICATION](commands/volume-admin-replication.md)                   | Start or stop replication on a standby     |
| [VOLUME.ADMIN PRUNE-CHANGELOG](commands/volume-admin-prune-changelog.md)           | Remove old changelog entries               |
| [VOLUME.ADMIN MARK-STALE-PREFIXES](commands/volume-admin-mark-stale-prefixes.md)   | Scan and clear orphaned prefix references  |
| [VOLUME.ADMIN CLEANUP-ORPHAN-FILES](commands/volume-admin-cleanup-orphan-files.md) | Remove orphaned segment files from disk    |
| [VOLUME.INSPECT REPLICATION](commands/volume-inspect-replication.md)               | Inspect replication state for a standby    |
| [VOLUME.INSPECT CURSOR](commands/volume-inspect-cursor.md)                         | Show the write cursor for a volume         |

For step-by-step maintenance procedures, see the [Operations Guide](operations-guide.md).
