---
title: "VOLUME.INSPECT REPLICATION"
description: "Returns the replication status for a specific standby member on a given shard, including the current stage, cursor position, per-stage progress, and any error message."
---

Returns the replication status for a specific standby member on a given shard, including the current stage, cursor
position, per-stage progress, and any error message.

## Syntax

```kronotop
VOLUME.INSPECT REPLICATION <volume-name> <standby-id>
```

## Parameters

| Parameter     | Type   | Description                                                                          |
|---------------|--------|--------------------------------------------------------------------------------------|
| `volume-name` | string | Volume identifier (e.g. `bucket-shard-0`). Use `VOLUME.ADMIN LIST` to discover names |
| `standby-id`  | string | Full member ID of the standby, or a unique 4-character prefix                        |

## Return Value

RESP3 map with the following fields:

| Field                       | Type   | Description                                                                                                 |
|-----------------------------|--------|-------------------------------------------------------------------------------------------------------------|
| `stage`                     | string | Current replication stage: `SEGMENT_REPLICATION`, `CHANGE_DATA_CAPTURE`, or empty string if not yet started |
| `cursor`                    | map    | Current replication cursor (see **cursor** below)                                                           |
| `status`                    | string | Replication status: `WAITING`, `RUNNING`, `DONE`, `STOPPED`, `FAILED`, or empty string if not yet started   |
| `error_message`             | string | Error description if replication has failed, or empty string                                                |
| `cdc_stage`                 | map    | CDC stage progress (see **cdc_stage** below)                                                                |
| `segment_replication_stage` | map    | Segment replication progress (see **segment_replication_stage** below)                                      |

### `cursor`

| Field        | Type    | Description                                  |
|--------------|---------|----------------------------------------------|
| `segment_id` | integer | ID of the segment currently being replicated |
| `position`   | integer | Current byte position within that segment    |

### `cdc_stage`

| Field             | Type    | Description                                   |
|-------------------|---------|-----------------------------------------------|
| `sequence_number` | integer | Changelog sequence number of the CDC consumer |
| `position`        | integer | Byte position within the current CDC segment  |

### `segment_replication_stage`

| Field                  | Type    | Description                                            |
|------------------------|---------|--------------------------------------------------------|
| `tail_sequence_number` | integer | Tail changelog sequence number for segment replication |
| `tail_next_position`   | integer | Next byte position after the segment tail              |

## Behavior

Reads the full replication status for the given standby and volume. Fields default to empty strings and zeroes when
replication has not yet started.

It is available on the management port (default 3320).

## Errors

| Condition                                         | Message                                                |
|---------------------------------------------------|--------------------------------------------------------|
| Missing or extra parameters                       | `ERR invalid number of parameters`                     |
| Invalid volume name format                        | `ERR invalid volume name: <name>`                      |
| Invalid member ID format                          | `ERR Invalid memberId: <id>`                           |
| No member found with the given 4-character prefix | `ERR no member found with prefix: <prefix>`            |
| More than one member matches the prefix           | `ERR more than one member found with prefix: <prefix>` |

## Examples

**Active replication (running):**

```kronotop
127.0.0.1:3320> VOLUME.INSPECT REPLICATION bucket-shard-0 ab12
1# "stage" => "CHANGE_DATA_CAPTURE"
2# "cursor" => 1# "segment_id" => (integer) 2
                2# "position" => (integer) 8192
3# "status" => "RUNNING"
4# "error_message" => ""
5# "cdc_stage" => 1# "sequence_number" => (integer) 15
                  2# "position" => (integer) 4096
6# "segment_replication_stage" => 1# "tail_sequence_number" => (integer) 10
                                  2# "tail_next_position" => (integer) 6144
```

**Replication not yet started:**

```kronotop
127.0.0.1:3320> VOLUME.INSPECT REPLICATION bucket-shard-0 ab12
1# "stage" => ""
2# "cursor" => 1# "segment_id" => (integer) 0
                2# "position" => (integer) 0
3# "status" => ""
4# "error_message" => ""
5# "cdc_stage" => 1# "sequence_number" => (integer) 0
                  2# "position" => (integer) 0
6# "segment_replication_stage" => 1# "tail_sequence_number" => (integer) 0
                                  2# "tail_next_position" => (integer) 0
```
