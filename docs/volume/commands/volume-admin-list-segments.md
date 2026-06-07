---
title: "VOLUME.ADMIN LIST-SEGMENTS"
description: "Returns the list of segment IDs for a given volume."
---

Returns the list of segment IDs for a given volume. Segments are the underlying storage units within a volume. Each
segment holds a range of appended data.

## Syntax

```kronotop
VOLUME.ADMIN LIST-SEGMENTS <volume-name>
```

## Parameters

| Parameter     | Type   | Description                                                               |
|---------------|--------|---------------------------------------------------------------------------|
| `volume-name` | string | Name of the volume, in `<kind>-shard-<id>` format (e.g. `bucket-shard-0`) |

## Return Value

RESP3 array of integers. Each element is a segment ID (`long`). Returns an empty array if the volume has no segments.

## Behavior

Loads volume metadata and returns the list of segment IDs as an integer array.

It is available on the management port (default 3320).

## Errors

| Condition                                          | Message                            |
|----------------------------------------------------|------------------------------------|
| Missing volume name parameter                      | `ERR invalid number of parameters` |
| No volume with that name is managed by this member | `ERR Volume: '<name>' is not open` |

## Examples

**List segments for an empty volume:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN LIST-SEGMENTS bucket-shard-1
(empty array)
```

**List segments for a volume with data:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN LIST-SEGMENTS bucket-shard-0
1) (integer) 0
```

**Volume not found:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN LIST-SEGMENTS non-existent-volume
(error) ERR Volume: 'non-existent-volume' is not open
```
