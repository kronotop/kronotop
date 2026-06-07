---
title: "VOLUME.ADMIN VACUUM"
description: "Manages garbage collection of stale segment data on a volume."
---

Manages garbage collection of stale segment data on a volume.

## Overview

Delete and update operations leave behind unreachable data in segments. Vacuum reclaims this space by evacuating
live entries from high-garbage segments into the current writable segment, then destroying the emptied segment files.

`START` returns immediately and processing continues in the background. Only one vacuum can
be active on a given volume at a time.

A typical lifecycle:

1. `START`: begin vacuum with a garbage threshold
2. `STATUS`: monitor progress
3. `STOP`: (optional) cancel early if needed
4. `DROP`: clear metadata after the run finishes or is stopped

Metadata from a completed or stopped run persists until explicitly dropped. A new vacuum cannot start while stale
metadata exists; run `DROP` first.

All subcommands are available on the management port (default 3320).

An invalid subcommand (not START, STOP, DROP, or STATUS) returns `ERR unknown subcommand: '<operation>'`.

## Subcommands

### START

Initiates a vacuum run on the specified volume.

**Syntax**

```kronotop
VOLUME.ADMIN VACUUM START <volume-name> <garbage-threshold>
```

**Parameters**

| Parameter           | Type   | Description                                                                                                 |
|---------------------|--------|-------------------------------------------------------------------------------------------------------------|
| `volume-name`       | string | Volume identifier (e.g. `bucket-shard-0`). Use `VOLUME.ADMIN LIST` to discover volume names                 |
| `garbage-threshold` | float  | Minimum garbage percentage required to vacuum a segment. Must be between 0 and 100 (exclusive on both ends) |

**Return Value**

Simple string `OK`.

**Behavior**

Continuously analyzes all segments on the volume. For each segment whose garbage percentage exceeds the threshold,
a worker is spawned to evacuate its live entries into the current writable segment.

The maximum number of concurrent workers is controlled by the `volume.vacuum.max_workers` configuration key. When set
to `0` (the default is 1), it falls back to the number of available CPU cores.

The writable (current active) segment is always skipped. Segments already being processed by a worker are also skipped.

When a worker finishes evacuating all entries from a segment, the segment file is destroyed. If the vacuum is stopped
before a worker completes, that segment's status is marked `STOPPED` and the file is not destroyed.

**Errors**

| Condition                                          | Message                                                             |
|----------------------------------------------------|---------------------------------------------------------------------|
| A vacuum is already running on this volume         | `ERR Vacuum is already running on volume <name>`                    |
| Metadata from a previous run exists                | `ERR Stale vacuum metadata exists on volume <name>, run DROP first` |
| Threshold is not a valid number                    | `ERR garbage-threshold must be a number`                            |
| Threshold is out of range                          | `ERR garbage-threshold must be between 0 and 100 (exclusive)`       |
| No volume with that name is managed by this member | `ERR Volume: '<name>' is not open`                                  |

**Examples**

**Start vacuum on bucket-shard-0 with a 30% threshold:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM START bucket-shard-0 30
OK
```

**Attempt to start while already running:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM START bucket-shard-0 30
(error) ERR Vacuum is already running on volume bucket-shard-0
```

**Stale metadata from a previous run:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM START bucket-shard-0 30
(error) ERR Stale vacuum metadata exists on volume bucket-shard-0, run DROP first
```

---

### STOP

Gracefully stops an active vacuum run.

**Syntax**

```kronotop
VOLUME.ADMIN VACUUM STOP <volume-name>
```

**Parameters**

| Parameter     | Type   | Description                                                                          |
|---------------|--------|--------------------------------------------------------------------------------------|
| `volume-name` | string | Volume identifier (e.g. `bucket-shard-0`). Use `VOLUME.ADMIN LIST` to discover names |

**Return Value**

Simple string `OK`.

**Behavior**

Signals the vacuum to stop. Active workers finish their current batch and then exit. The command blocks until all
workers have completed. Final statistics are saved to metadata with result `STOPPED`.

After stopping, the metadata remains in place. Use `DROP` to clear it before starting a new vacuum.

**Errors**

| Condition                                          | Message                                 |
|----------------------------------------------------|-----------------------------------------|
| No vacuum is running on this volume                | `ERR No active vacuum on volume <name>` |
| No volume with that name is managed by this member | `ERR Volume: '<name>' is not open`      |

**Examples**

**Stop an active vacuum:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM STOP bucket-shard-0
OK
```

**No vacuum to stop:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM STOP bucket-shard-0
(error) ERR No active vacuum on volume bucket-shard-0
```

---

### DROP

Removes all vacuum metadata from the volume.

**Syntax**

```kronotop
VOLUME.ADMIN VACUUM DROP <volume-name>
```

**Parameters**

| Parameter     | Type   | Description                                                                          |
|---------------|--------|--------------------------------------------------------------------------------------|
| `volume-name` | string | Volume identifier (e.g. `bucket-shard-0`). Use `VOLUME.ADMIN LIST` to discover names |

**Return Value**

Simple string `OK`.

**Behavior**

Clears all vacuum metadata (volume-level statistics and per-segment records). The vacuum must be stopped
before dropping. Drop metadata before starting a new vacuum run on the same volume.

**Errors**

| Condition                                          | Message                                                        |
|----------------------------------------------------|----------------------------------------------------------------|
| Vacuum is still running                            | `ERR Vacuum is still running on volume <name>, run STOP first` |
| No metadata exists                                 | `ERR No active vacuum on volume <name>`                        |
| No volume with that name is managed by this member | `ERR Volume: '<name>' is not open`                             |

**Examples**

**Drop metadata after a completed or stopped vacuum:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM DROP bucket-shard-0
OK
```

**Attempt to drop while still running:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM DROP bucket-shard-0
(error) ERR Vacuum is still running on volume bucket-shard-0, run STOP first
```

---

### STATUS

Returns the current state and progress of a vacuum run.

**Syntax**

```kronotop
VOLUME.ADMIN VACUUM STATUS <volume-name>
```

**Parameters**

| Parameter     | Type   | Description                                                                          |
|---------------|--------|--------------------------------------------------------------------------------------|
| `volume-name` | string | Volume identifier (e.g. `bucket-shard-0`). Use `VOLUME.ADMIN LIST` to discover names |

**Return Value**

RESP3 map with the following top-level fields:

| Field      | Type    | Description                                                                    |
|------------|---------|--------------------------------------------------------------------------------|
| `active`   | boolean | Whether a vacuum is currently running                                          |
| `metadata` | map     | Volume-level run statistics. Present only after the vacuum run has saved stats |
| `segments` | array   | Per-segment progress. Each element is a map                                    |

The `metadata` map contains:

| Field                | Type    | Description                                       |
|----------------------|---------|---------------------------------------------------|
| `started_at`         | integer | Epoch milliseconds when the vacuum run began      |
| `completed_at`       | integer | Epoch milliseconds when the vacuum run ended      |
| `result`             | string  | Run outcome: `NO_WORK`, `COMPLETED`, or `STOPPED` |
| `segments_processed` | integer | Number of segments that were vacuumed             |

Each element in the `segments` array is a map:

| Field        | Type    | Description                                                       |
|--------------|---------|-------------------------------------------------------------------|
| `segment_id` | integer | Segment identifier                                                |
| `status`     | string  | Current phase: `ANALYZE`, `EVACUATING`, `COMPLETED`, or `STOPPED` |
| `started_at` | integer | Epoch milliseconds when analysis of this segment began            |

**Behavior**

Checks whether a vacuum is active, then loads persisted metadata and per-segment records. If no metadata exists
(vacuum has never run or was already dropped), an error is returned.

During an active vacuum, `metadata` may not yet be present (it is written when the run completes or is stopped),
while `segments` reflects real-time per-segment progress.

**Errors**

| Condition                                          | Message                                 |
|----------------------------------------------------|-----------------------------------------|
| No metadata exists (never run or already dropped)  | `ERR No active vacuum on volume <name>` |
| No volume with that name is managed by this member | `ERR Volume: '<name>' is not open`      |

**Examples**

**Status of a completed vacuum:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM STATUS bucket-shard-0
1# "active" => (false)
2# "metadata" =>
   1# "started_at" => (integer) 1715100000000
   2# "completed_at" => (integer) 1715100060000
   3# "result" => "COMPLETED"
   4# "segments_processed" => (integer) 3
3# "segments" =>
   1) 1# "segment_id" => (integer) 0
      2# "status" => "COMPLETED"
      3# "started_at" => (integer) 1715100001000
   2) 1# "segment_id" => (integer) 1
      2# "status" => "COMPLETED"
      3# "started_at" => (integer) 1715100002000
   3) 1# "segment_id" => (integer) 2
      2# "status" => "COMPLETED"
      3# "started_at" => (integer) 1715100003000
```

**Status of an active vacuum with workers in progress:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM STATUS bucket-shard-0
1# "active" => (true)
2# "segments" =>
   1) 1# "segment_id" => (integer) 0
      2# "status" => "EVACUATING"
      3# "started_at" => (integer) 1715100001000
   2) 1# "segment_id" => (integer) 1
      2# "status" => "ANALYZE"
      3# "started_at" => (integer) 1715100005000
```

**No vacuum has run:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN VACUUM STATUS bucket-shard-0
(error) ERR No active vacuum on volume bucket-shard-0
```

## Concepts

**Garbage Percentage**

Each segment tracks its total size, free bytes (unallocated space), and used bytes (space occupied by live entries).
Garbage percentage is calculated as:

```
garbage_percentage = (size - free_bytes - used_bytes) / size * 100
```

Use `VOLUME.ADMIN DESCRIBE` to inspect per-segment garbage percentages before deciding on a threshold.

**Segment Status Values**

| Status       | Description                                                                       |
|--------------|-----------------------------------------------------------------------------------|
| `ANALYZE`    | Segment is being analyzed for prefix cardinalities before evacuation begins       |
| `EVACUATING` | Live entries are being read from this segment and written to the writable segment |
| `COMPLETED`  | All entries have been evacuated and the segment file has been destroyed           |
| `STOPPED`    | Evacuation was interrupted by a `STOP` command before completion                  |

**Vacuum Result Values**

| Result      | Description                                                        |
|-------------|--------------------------------------------------------------------|
| `NO_WORK`   | No segments exceeded the garbage threshold; nothing was vacuumed   |
| `COMPLETED` | All eligible segments were successfully vacuumed                   |
| `STOPPED`   | The vacuum was stopped before all eligible segments were processed |
