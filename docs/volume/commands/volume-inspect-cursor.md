---
title: "VOLUME.INSPECT CURSOR"
description: "Returns the write cursor of a volume: the active segment, the last segment that holds committed data, and the byte position, versionstamp, and changelog sequence number of the last write."
---

Returns the write cursor of a volume: the active segment, the last segment that holds committed data, and the byte
position, versionstamp, and changelog sequence number of the last write.

## Syntax

```kronotop
VOLUME.INSPECT CURSOR <volume-name>
```

## Arguments

| Argument      | Type   | Description                                                            |
|---------------|--------|------------------------------------------------------------------------|
| `volume-name` | string | Name of the volume to inspect, in `shard-<id>` format (e.g. `shard-0`) |

## Return Value

RESP3 map with the following fields:

| Field               | Type    | Description                                                                                                      |
|---------------------|---------|------------------------------------------------------------------------------------------------------------------|
| `active_segment_id` | integer | ID of the segment currently accepting writes                                                                     |
| `tail_segment_id`   | integer | ID of the last segment that holds a committed entry, or `-1` if the volume is empty                              |
| `versionstamp`      | string  | Base32-hex encoded versionstamp of the last write, or empty string if no writes have occurred                    |
| `next_position`     | integer | Byte offset in the tail segment right after the last committed entry, or `0` if the volume is empty              |
| `sequence_number`   | integer | Changelog sequence number of the last mutation, or `-1` if the volume is empty or the changelog entry was pruned |

## Behavior

`versionstamp`, `next_position` and `sequence_number` describe the tail segment, which is the last segment that holds a
committed entry. `tail_segment_id` equals `active_segment_id` unless a new segment has been created and no entry in it
has been committed yet. In that case `tail_segment_id` is lower.

When the volume has no data, `tail_segment_id` and `sequence_number` are `-1`, `next_position` is `0` and
`versionstamp` is an empty string.

It is available on the management port (default 3320).

## Errors

Argument errors:

| Error Code | Error message                                                   | Cause                                            |
|------------|-----------------------------------------------------------------|--------------------------------------------------|
| `ERR`      | `Wrong number of arguments for 'VOLUME.INSPECT CURSOR' command` | -                                                |
| `ERR`      | `Invalid volume name: '<name>'`                                 | The name does not match the `shard-<id>` format. |

## Examples

**Volume with data:**

```kronotop
127.0.0.1:3320> VOLUME.INSPECT CURSOR shard-0
1# "active_segment_id" => (integer) 0
2# "tail_segment_id" => (integer) 0
3# "versionstamp" => "A1B2C3D4E5F6G7H8I9J0"
4# "next_position" => (integer) 10240
5# "sequence_number" => (integer) 9
```

**Empty volume:**

```kronotop
127.0.0.1:3320> VOLUME.INSPECT CURSOR shard-0
1# "active_segment_id" => (integer) 0
2# "tail_segment_id" => (integer) -1
3# "versionstamp" => ""
4# "next_position" => (integer) 0
5# "sequence_number" => (integer) -1
```

**New segment without a committed entry:**

```kronotop
127.0.0.1:3320> VOLUME.INSPECT CURSOR shard-0
1# "active_segment_id" => (integer) 1
2# "tail_segment_id" => (integer) 0
3# "versionstamp" => "A1B2C3D4E5F6G7H8I9J0"
4# "next_position" => (integer) 4294966272
5# "sequence_number" => (integer) 9
```
