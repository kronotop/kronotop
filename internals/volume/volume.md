# Volume

Volume is Kronotop's storage engine. It stores document content on the local disk in pre-allocated segment files while
managing all metadata transactionally in FoundationDB.

* [General Design](#general-design)
* [Segments](#segments)
* [Metadata Management](#metadata-management)

---

## General Design

### Dual-Storage Architecture

Volume splits storage into two layers:

| Layer        | System       | Stores                                                             |
|--------------|--------------|--------------------------------------------------------------------|
| **Metadata** | FoundationDB | Entry locations, versioning, segment accounting, replication state |
| **Content**  | Local disk   | Raw entry bytes in pre-allocated segment files                     |

FoundationDB gives ACID transactional guarantees for metadata; local disk gives high-throughput sequential I/O for
bulk data.

### Prefix-Based Namespace Isolation

Every entry belongs to a prefix: a namespace that isolates entries from different logical owners (for example,
different buckets). A prefix is an 8-byte key derived from a string name via SipHash24 hashing. All metadata keys in
FoundationDB include the prefix, so operations on one namespace never interfere with another.

### VolumeSession

A `VolumeSession` holds the context for a set of volume operations: a prefix, an optional FoundationDB transaction,
and a user version counter. There are two kinds:

- **Read-only**: constructed with just a prefix. Reads bypass FDB and use the `EntryMetadataCache`.
- **Transactional**: constructed with a prefix and an FDB transaction. Reads and writes go through FoundationDB.

### UserVersion

FoundationDB assigns a single transaction version to all mutations in a committed transaction. When a transaction
appends multiple entries, they would all share the same versionstamp. **UserVersion** is a 2-byte counter (0 to
65,535) appended to the 10-byte transaction version to form a 12-byte versionstamp. This guarantees a unique, ordered
key for every entry within a single transaction.

If a transaction exceeds 65,535 entries, a `TooManyEntriesException` is thrown.

### Write Path

Appending an entry follows a two-phase durability protocol:

1. **Segment write**: the entry bytes are appended to the current writable segment. If the segment is full, a new
   segment is created.
2. **Flush**: all mutated segments are synced to disk (`fd.sync()`), making the content durable.
3. **FDB metadata commit**: only after the flush succeeds, metadata (entry location, segment accounting, changelog)
   is committed in a single FDB transaction using `SET_VERSIONSTAMPED_KEY` mutations.

This ordering guarantees that FDB metadata never references data that has not been persisted to disk.

### Read Path

1. **Resolve metadata**: in transactional mode, read the entry's `EntryMetadata` from FDB. In read-only mode, look it
   up in the `EntryMetadataCache`.
2. **Read from segment**: use the `(position, length)` from the metadata to perform a random-access read on the
   segment file.

If a cached metadata entry points to a segment that no longer contains valid data, the cache entry is invalidated and
the read retries from FoundationDB.

### Delete Semantics

Deleting an entry removes its metadata from FoundationDB (both the forward and reverse indexes) and decrements the
segment's cardinality and used-bytes counters. **The actual bytes on disk are not touched.**

### Update Semantics

An update appends the new entry data to a segment (possibly a different one), then atomically updates the metadata
pointers to reference the new location. The old segment data becomes garbage.

### Volume Status

A volume operates in one of three states:

| Status       | Behavior                                 |
|--------------|------------------------------------------|
| `READWRITE`  | Default. Reads and writes are permitted. |
| `READONLY`   | Reads succeed; writes are rejected.      |
| `INOPERABLE` | The volume is unusable.                  |

Status is persisted in FDB and protected by a `ReadWriteLock` at runtime.

---

## Segments

### Overview

A segment is a fixed-size, pre-allocated, append-only file on disk. It stores raw entry bytes sequentially from
position 0. Segments are the unit of disk I/O; entries are never split across segments.

### File Layout

Each segment is a single file in the volume's data directory. Filenames are zero-padded 19-character decimal segment
IDs (e.g., `0000000000000000042`). Content is laid out as a contiguous byte stream:

```
| entry₀ (pos=0, len=N₀) | entry₁ (pos=N₀, len=N₁) | ... | free space ... |
```

Individual entries have a maximum size of 16 MiB (`1 << 24` bytes).

### Implementation

The segment layer defines a base contract for file-backed storage segments, then specializes into two sub-interfaces:

- **Readable segments** support position-based random reads using memory-mapped I/O.
- **Writable segments** support append, positional insert, and flush operations using file-channel I/O.

The read implementation maps the entire segment file into memory at open time and serves reads as zero-copy slices
of the mapped region. The write implementation uses a `RandomAccessFile` with a file channel.

### Write Operations

There are two write modes:

- **`append(ByteBuffer)`** is the normal write path. It atomically reserves space using a compare-and-swap on the
  internal position counter, then writes the entry at the reserved offset. It returns a `SegmentAppendResult` with the
  position and length. Thread-safe for concurrent appenders.
- **`insert(ByteBuffer, long position)`** writes at an explicit position without advancing the write pointer. Used
  exclusively by the replication subsystem to replay entries at their original offsets.

### Read Operation

Reading an entry at a given `(position, length)` returns a zero-copy slice of the memory-mapped segment. The entire
segment is mapped into the process address space when it is opened, so reads involve no system calls or data copying;
they expose a view of the mapped region.

### Space Management

Free bytes = `size − currentPosition`. When an append would exceed the remaining space, the volume creates a new
segment. Old segments are not sealed or made read-only in any special way; they just have no free space left.

### Thread Safety

- **Write position**: `AtomicLong` with CAS ensures concurrent appends never overlap.
- **Flush**: a counter-based dirty-tracking mechanism (`AtomicInteger flushCounter`) avoids redundant `fd.sync()`
  calls. Flushing acquires a dedicated `flushLock` to serialize sync operations.
- **Reads**: the memory-mapped segment uses a shared memory scope, so any thread can read concurrently without
  synchronization.

### Flush Mechanism

Each `append` increments the flush counter. When `flush()` is called, it checks whether the counter has changed since
the last sync. If no new writes occurred, the flush is a no-op. Otherwise, it calls `fd.sync()` under the flush lock
to force all buffered writes to stable storage.

### Segment Lifecycle

```
creation → writing → full → cleanup/deletion
```

Segments are created on demand when existing segments cannot accommodate a new entry.

### SegmentContainer

At runtime, segments are held in two containers:

- **Writable segment container** pairs the active writable segment with its segment metadata. Held in an atomic
  reference so that callers always see the latest writable segment without locking on the read path.
- **Readable segment containers** pair each read-only, memory-mapped segment with its segment metadata. Stored in
  a concurrent hash map keyed by segment ID.

A read-write lock protects segment lifecycle operations such as creating a new segment or closing the volume. Normal
reads do not acquire this lock.

---

## Metadata Management

### VolumeSubspace

Each volume owns a FoundationDB directory subspace. All metadata keys are packed relative to this subspace. The
`VolumeSubspace` class provides typed pack/unpack methods for every key format.

### Subspace Layout

Volume metadata is organized into nine subspaces, identified by single-byte prefixes:

#### `ENTRY_SUBSPACE` (0x01): Primary Entry Index

```
Key:   (0x01, prefix_bytes, versionstamp)
Value: EntryMetadata (fixed-size 40-byte encoding)
```

The forward index. Maps each entry's prefix + versionstamp to its entry metadata, which contains the physical location
of the entry on disk. This is the primary lookup path for reads.

#### `ENTRY_METADATA_SUBSPACE` (0x02): Reverse Index

```
Key:   (0x02, entry_metadata_bytes)
Value: versionstamp
```

The reverse index. Given an entry metadata (a known physical location), retrieve the corresponding versionstamp key.
Used by delete and update operations to locate the forward index entry.

#### `SEGMENT_POSITION_SUBSPACE` (0x03): Segment Position Tracking

```
Key:   (0x03, segment_id, position, length)
Value: versionstamp
```

Records every allocated position within a segment. Delete and update operations do not remove entries from it.
Stale entries (pointing to deleted logical entries) are harmless.

#### `SEGMENT_STATS_SUBSPACE` (0x04): Segment Statistics

```
Key:   (0x04, segment_id, prefix_bytes, stat_type)
Value: depends on stat_type
```

A unified subspace for per-segment, per-prefix accounting. The `stat_type` suffix distinguishes two metrics:

- **Cardinality** (stat_type = 0x01): number of live entries. Value: 4-byte little-endian integer. Updated via
  atomic ADD (+1 on append, −1 on delete).
- **Used bytes** (stat_type = 0x02): total bytes occupied by live entries. Value: 8-byte little-endian long. Updated
  via atomic ADD.

Because both counters use FoundationDB atomic mutations, concurrent writers never conflict.

#### `CHANGELOG_SUBSPACE` (0x05): Replication Changelog

Used by the replication subsystem to log operations for standby nodes. Documented separately.

#### `MUTATION_TRIGGER` (0x06): Replication Watch Key

A trigger key that standbys watch via FoundationDB's watch mechanism. Updated on each mutation to notify standbys of
new data. Documented separately.

#### `CHANGELOG_BACK_POINTER_SUBSPACE` (0x07): Changelog Back-References

Back-pointer tracking for the replication changelog. Documented separately.

#### `VACUUM_METADATA_SUBSPACE` (0x08): Vacuum State

Stores metadata for the vacuum process, organized into two sub-scopes:

- **Segment scope** (sub-prefix 0x01): per-segment vacuum state: processing status, start time, and per-prefix
  cardinality snapshots at the time the vacuum began.
- **Volume scope** (sub-prefix 0x02): volume-wide vacuum statistics: start time, completion time, result, and total
  segments processed.

Entries in this subspace are transient. They are created when a vacuum starts and cleared when the vacuum metadata
is dropped.

#### `VOLUME_METADATA_SUBSPACE` (0x09): Volume Identity and Configuration

Stores the volume's identity and top-level configuration as individual key-value pairs:

- **ID** (field 0x01): the volume's unique 8-byte identifier (a SipHash of a random UUID, assigned at creation).
- **Status** (field 0x02): the current operational state (READWRITE, READONLY, or INOPERABLE), stored as an ASCII
  string.
- **Segment IDs** (field 0x03, one key per segment): each registered segment ID is stored as a separate key under
  this field prefix, enabling range scans to enumerate all segments.

### EntryMetadata

`EntryMetadata` is a record that describes the physical location of an entry:

| Field       | Type     | Description                                                    |
|-------------|----------|----------------------------------------------------------------|
| `segmentId` | `long`   | The segment containing this entry                              |
| `prefix`    | `byte[]` | 8-byte namespace prefix                                        |
| `position`  | `long`   | Byte offset within the segment                                 |
| `length`    | `long`   | Entry size in bytes                                            |
| `handle`    | `long`   | Unique entry handle (derived from volume + segment + position) |

Encoded as a fixed-size 40-byte binary payload (five 8-byte fields written sequentially into a byte buffer) for
storage in the entry subspaces.

### SegmentMetadata

`SegmentMetadata` manages per-segment, per-prefix accounting (cardinality and used bytes). It uses FoundationDB atomic
`ADD` mutations, which are conflict-free: multiple concurrent transactions can increment or decrement counters without
causing transaction conflicts.

Segment metadata caches the packed FoundationDB keys for its counters (10-minute expiry after last access) to avoid
re-packing keys on every operation.

### VolumeMetadata

`VolumeMetadata` is the top-level metadata record for a volume. Each field is stored as a separate key-value pair
under the `VOLUME_METADATA_SUBSPACE` (see above):

| Field        | Type           | Description                |
|--------------|----------------|----------------------------|
| `id`         | `long`         | Unique volume identifier   |
| `status`     | `VolumeStatus` | Current operational state  |
| `segmentIds` | `List<Long>`   | Sorted list of segment IDs |

On volume startup, the metadata is loaded by reading the individual keys. If the volume has not been initialized,
the fields are created with a new random ID, `READWRITE` status, and a single initial segment.

### EntryMetadataCache

A per-prefix `LoadingCache<Versionstamp, EntryMetadata>` that speeds up read-only lookups by avoiding FoundationDB
round-trips. Configuration:

- **Maximum size**: 10,000 entries per prefix
- **Expiry**: 15 minutes after last access
- **Invalidation**: explicit invalidation on stale reads

Caches are lazily created per prefix in a `ConcurrentHashMap` and are fully thread-safe.

### Versionstamp-Based Keys

All entry keys use FoundationDB versionstamps (12 bytes):

```
| 10 bytes: transaction version (assigned by FDB at commit) | 2 bytes: user version |
```

The transaction version provides global ordering across all transactions. The user version provides ordering within
a single transaction. Together, they guarantee a unique, monotonically increasing key for every entry in the system.

On writes, `SET_VERSIONSTAMPED_KEY` mutations let the key include a placeholder that FDB fills in at commit time with
the actual transaction version, avoiding a read-then-write pattern.
