---
title: "VOLUME.INSPECT CURSOR"
description: "Returns the current write cursor for a volume's active segment, including its byte position, versionstamp, and changelog sequence number."
---

Returns the current write cursor for a volume's active segment, including its byte position, versionstamp, and changelog
sequence number.

## Syntax

```kronotop
VOLUME.INSPECT CURSOR <volume-name>
```

## Parameters

| Parameter     | Type   | Description                                                                          |
|---------------|--------|--------------------------------------------------------------------------------------|
| `volume-name` | string | Name of the volume to inspect, in `<kind>-shard-<id>` format (e.g. `bucket-shard-0`) |

## Return Value

RESP3 map with the following fields:

| Field               | Type    | Description                                                                                                      |
|---------------------|---------|------------------------------------------------------------------------------------------------------------------|
| `active_segment_id` | integer | ID of the segment currently accepting writes                                                                     |
| `versionstamp`      | string  | Base32-hex encoded versionstamp of the last write, or empty string if no writes have occurred                    |
| `next_position`     | integer | Byte offset where the next write will be appended in the active segment                                          |
| `sequence_number`   | integer | Changelog sequence number of the last mutation, or `-1` if the volume is empty or the changelog entry was pruned |

## Behavior

Finds the active segment ID and resolves the sequence number of the last changelog entry. Returns `-1` for
`sequence_number` and an empty string for `versionstamp` when the volume has no data.

It is available on the management port (default 3320).

## Errors

| Condition                                             | Message                            |
|-------------------------------------------------------|------------------------------------|
| Missing volume-name parameter                         | `ERR invalid number of parameters` |
| Volume name does not match `<kind>-shard-<id>` format | `ERR invalid volume name: <name>`  |

## Examples

**Volume with data:**

```kronotop
127.0.0.1:3320> VOLUME.INSPECT CURSOR bucket-shard-0
1# "active_segment_id" => (integer) 0
2# "versionstamp" => "A1B2C3D4E5F6G7H8I9J0"
3# "next_position" => (integer) 10240
4# "sequence_number" => (integer) 9
```

**Empty volume:**

```kronotop
127.0.0.1:3320> VOLUME.INSPECT CURSOR bucket-shard-0
1# "active_segment_id" => (integer) 0
2# "versionstamp" => ""
3# "next_position" => (integer) 0
4# "sequence_number" => (integer) -1
```
