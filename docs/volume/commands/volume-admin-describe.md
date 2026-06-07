---
title: "VOLUME.ADMIN DESCRIBE"
description: "Returns metadata and segment-level statistics for a named volume."
---

Returns metadata and segment-level statistics for a named volume.

## Syntax

```kronotop
VOLUME.ADMIN DESCRIBE <volume-name>
```

## Parameters

| Parameter     | Type   | Description                                                                           |
|---------------|--------|---------------------------------------------------------------------------------------|
| `volume-name` | string | Name of the volume to describe, in `<kind>-shard-<id>` format (e.g. `bucket-shard-0`) |

## Return Value

RESP3 map with the following top-level fields:

| Field          | Type    | Description                                                  |
|----------------|---------|--------------------------------------------------------------|
| `name`         | string  | Volume name                                                  |
| `status`       | string  | Operational status: `READWRITE`, `READONLY`, or `INOPERABLE` |
| `data_dir`     | string  | Filesystem path where segment files are stored               |
| `segment_size` | integer | Maximum size of each segment file in bytes                   |
| `segments`     | map     | Per-segment statistics keyed by integer segment ID           |

Each value in the `segments` map is a nested map:

| Field                | Type    | Description                                                                      |
|----------------------|---------|----------------------------------------------------------------------------------|
| `size`               | integer | Total segment file size in bytes                                                 |
| `free_bytes`         | integer | Unallocated space remaining in the segment                                       |
| `used_bytes`         | integer | Space occupied by live entries                                                   |
| `garbage_percentage` | double  | Percentage of reclaimable space: `(size - free_bytes - used_bytes) / size * 100` |
| `cardinality`        | integer | Number of live entries in the segment                                            |

## Behavior

Reads the volume's configuration and computes per-segment statistics.

It is available on the management port (default 3320).

## Errors

| Condition                                          | Message                            |
|----------------------------------------------------|------------------------------------|
| Volume name parameter is missing                   | `ERR invalid number of parameters` |
| No volume with that name is managed by this member | `ERR Volume: '<name>' is not open` |

## Examples

**Describe a volume with one segment:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN DESCRIBE bucket-shard-0
1# "name" => "bucket-shard-0"
2# "status" => "READWRITE"
3# "data_dir" => "/tmp/kronotop/data/bucket-shard-0"
4# "segment_size" => (integer) 268435456
5# "segments" =>
   1# (integer) 0 =>
      1# "size" => (integer) 268435456
      2# "free_bytes" => (integer) 268435200
      3# "used_bytes" => (integer) 256
      4# "garbage_percentage" => (double) 0.0
      5# "cardinality" => (integer) 1
```

**Volume not found:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN DESCRIBE non-existent-volume
(error) ERR Volume: 'non-existent-volume' is not open
```
