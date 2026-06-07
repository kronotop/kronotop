---
title: "VOLUME.STATS"
description: "Returns a volume-wide overview including status, capacity, garbage percentage, and fill ratio."
---

Returns a volume-wide overview including status, capacity, garbage percentage, and fill ratio.

## Syntax

```kronotop
VOLUME.STATS <volume-name>
```

## Parameters

| Parameter     | Type   | Description                                                               |
|---------------|--------|---------------------------------------------------------------------------|
| `volume-name` | string | Name of the volume, in `<kind>-shard-<id>` format (e.g. `bucket-shard-0`) |

## Return Value

RESP3 map with the following fields:

| Field                | Type    | Description                                                                                         |
|----------------------|---------|-----------------------------------------------------------------------------------------------------|
| `status`             | string  | Operational status: `READWRITE`, `READONLY`, or `INOPERABLE`                                        |
| `segment_count`      | integer | Number of segments in the volume                                                                    |
| `total_size_bytes`   | integer | Sum of all segment sizes in bytes                                                                   |
| `used_bytes`         | integer | Total bytes occupied by live entries across all segments                                            |
| `free_bytes`         | integer | Total unallocated bytes across all segments                                                         |
| `garbage_bytes`      | integer | Total reclaimable bytes across all segments                                                         |
| `garbage_percentage` | double  | Fraction of total capacity occupied by garbage: `garbage_bytes / total_size_bytes`                  |
| `fill_ratio`         | double  | Fraction of total capacity that has been written: `(used_bytes + garbage_bytes) / total_size_bytes` |
| `total_cardinality`  | integer | Total number of live entries across all segments                                                    |

## Behavior

Looks up the volume by name and analyzes all segments. Aggregates per-segment metrics into volume-wide totals.

The following invariant holds:

```
total_size_bytes - free_bytes == used_bytes + garbage_bytes
```

When `total_size_bytes` is zero (no segments), both `garbage_percentage` and `fill_ratio` return `0.0`.

When no subcommand is specified, `VOLUME.STATS` defaults to this overview. The available subcommands are:

| Subcommand    | Description                              |
|---------------|------------------------------------------|
| `OPCOUNTERS`  | In-memory operation counters             |
| `SEGMENTS`    | Per-segment statistics                   |
| `REPLICATION` | Replication state for a specific standby |
| `RESET`       | Reset counters                           |

This command is available on the management port (default 3320).

## Errors

| Condition                                          | Message                                                    |
|----------------------------------------------------|------------------------------------------------------------|
| Volume name parameter is missing                   | `ERR wrong number of arguments for 'VOLUME.STATS' command` |
| No volume with that name is managed by this member | `ERR Volume: '<name>' is not open`                         |
| Unknown subcommand                                 | `ERR unknown subcommand '<subcommand>'`                    |

## Examples

**Overview of a volume with one segment:**

```kronotop
127.0.0.1:3320> VOLUME.STATS bucket-shard-0
1# "status" => "READWRITE"
2# "segment_count" => (integer) 1
3# "total_size_bytes" => (integer) 1048576
4# "used_bytes" => (integer) 100
5# "free_bytes" => (integer) 1048476
6# "garbage_bytes" => (integer) 0
7# "garbage_percentage" => (double) 0.0
8# "fill_ratio" => (double) 9.5367431640625e-05
9# "total_cardinality" => (integer) 10
```

**Empty volume (no segments):**

```kronotop
127.0.0.1:3320> VOLUME.STATS bucket-shard-0
1# "status" => "READWRITE"
2# "segment_count" => (integer) 0
3# "total_size_bytes" => (integer) 0
4# "used_bytes" => (integer) 0
5# "free_bytes" => (integer) 0
6# "garbage_bytes" => (integer) 0
7# "garbage_percentage" => (double) 0.0
8# "fill_ratio" => (double) 0.0
9# "total_cardinality" => (integer) 0
```

**Volume not found:**

```kronotop
127.0.0.1:3320> VOLUME.STATS non-existent-volume
(error) ERR Volume: 'non-existent-volume' is not open
```
