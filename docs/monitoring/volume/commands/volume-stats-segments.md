---
title: "VOLUME.STATS SEGMENTS"
description: "Returns per-segment size, usage, and garbage statistics for a named volume."
---

Returns per-segment size, usage, and garbage statistics for a named volume.

## Syntax

```kronotop
VOLUME.STATS <volume-name> SEGMENTS
```

## Parameters

| Parameter     | Type   | Description                                                               |
|---------------|--------|---------------------------------------------------------------------------|
| `volume-name` | string | Name of the volume, in `<kind>-shard-<id>` format (e.g. `bucket-shard-0`) |

## Return Value

RESP3 array of maps. Each map represents one segment with the following fields:

| Field                | Type    | Description                                                         |
|----------------------|---------|---------------------------------------------------------------------|
| `segment_id`         | integer | Numeric segment identifier                                          |
| `size_bytes`         | integer | Total segment file size in bytes                                    |
| `used_bytes`         | integer | Bytes occupied by live entries                                      |
| `free_bytes`         | integer | Unallocated space remaining in the segment                          |
| `garbage_bytes`      | integer | Reclaimable bytes: `(size_bytes - free_bytes) - used_bytes`         |
| `garbage_percentage` | double  | Percentage of reclaimable space: `garbage_bytes / size_bytes * 100` |
| `cardinality`        | integer | Number of live entries in the segment                               |

Returns an empty array if the volume has no segments.

## Behavior

Analyzes all segments in the volume and returns individual statistics for each one.

The following per-segment invariant holds:

```
size_bytes - free_bytes == used_bytes + garbage_bytes
```

This command is available on the management port (default 3320).

## Errors

| Condition                                          | Message                                                    |
|----------------------------------------------------|------------------------------------------------------------|
| Volume name parameter is missing                   | `ERR wrong number of arguments for 'VOLUME.STATS' command` |
| No volume with that name is managed by this member | `ERR Volume: '<name>' is not open`                         |

## Examples

**Volume with one segment after appends and deletes:**

```kronotop
127.0.0.1:3320> VOLUME.STATS bucket-shard-0 SEGMENTS
1) 1# "segment_id" => (integer) 0
   2# "size_bytes" => (integer) 1048576
   3# "used_bytes" => (integer) 80
   4# "free_bytes" => (integer) 1048476
   5# "garbage_bytes" => (integer) 20
   6# "garbage_percentage" => (double) 0.0019073486328125
   7# "cardinality" => (integer) 8
```

**Volume with multiple segments:**

```kronotop
127.0.0.1:3320> VOLUME.STATS bucket-shard-0 SEGMENTS
1) 1# "segment_id" => (integer) 0
   2# "size_bytes" => (integer) 1048576
   3# "used_bytes" => (integer) 1048576
   4# "free_bytes" => (integer) 0
   5# "garbage_bytes" => (integer) 0
   6# "garbage_percentage" => (double) 0.0
   7# "cardinality" => (integer) 104857
2) 1# "segment_id" => (integer) 1
   2# "size_bytes" => (integer) 1048576
   3# "used_bytes" => (integer) 100
   4# "free_bytes" => (integer) 1048476
   5# "garbage_bytes" => (integer) 0
   6# "garbage_percentage" => (double) 0.0
   7# "cardinality" => (integer) 10
```

**Empty volume (no segments):**

```kronotop
127.0.0.1:3320> VOLUME.STATS bucket-shard-0 SEGMENTS
(empty array)
```
