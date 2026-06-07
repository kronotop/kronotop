---
title: "VOLUME.STATS REPLICATION"
description: "Returns replication state for a specific standby member on a named volume."
---

Returns replication state for a specific standby member on a named volume.

## Syntax

```kronotop
VOLUME.STATS <volume-name> REPLICATION <shard-kind> <shard-id> <standby-id>
```

## Parameters

| Parameter     | Type    | Description                                                               |
|---------------|---------|---------------------------------------------------------------------------|
| `volume-name` | string  | Name of the volume, in `<kind>-shard-<id>` format (e.g. `bucket-shard-0`) |
| `shard-kind`  | string  | Shard kind (e.g. `BUCKET`, `STASH`)                                       |
| `shard-id`    | integer | Numeric shard identifier                                                  |
| `standby-id`  | string  | Member ID of the standby to inspect                                       |

## Return Value

RESP3 map with the following fields:

| Field                 | Type    | Description                                                                                                   |
|-----------------------|---------|---------------------------------------------------------------------------------------------------------------|
| `stage`               | string  | Current replication stage (e.g. `SEGMENT_REPLICATION`, `CHANGE_DATA_CAPTURE`), or empty string if not started |
| `status`              | string  | Replication status (e.g. `WAITING`, `RUNNING`, `FAILED`), or empty string if unknown                          |
| `error_message`       | string  | Error description if replication has failed, or empty string                                                  |
| `cursor_segment_id`   | integer | Segment ID the replication cursor is currently on (0 if not started)                                          |
| `cursor_position`     | integer | Byte position within the cursor segment (0 if not started)                                                    |
| `cdc_sequence_number` | integer | Change data capture sequence number (0 if not in CDC stage)                                                   |
| `cdc_position`        | integer | Position within the current CDC sequence (0 if not in CDC stage)                                              |

## Behavior

Reads the standby's replication status from FoundationDB. The volume name is derived from shard-kind and shard-id using
the standard naming convention.

Fields default to empty strings (for `stage`, `status`, `error_message`) or zero (for numeric fields) when replication
has not started or the standby subspace has not been initialized.

This command requires exactly 5 parameters total (volume name, subcommand, shard kind, shard ID, standby ID).

This command is available on the management port (default 3320).

## Errors

| Condition                      | Message                              |
|--------------------------------|--------------------------------------|
| Incorrect number of parameters | `ERR invalid number of parameters`   |
| Invalid shard kind             | `ERR Unknown ShardKind '<kind>'`     |
| Invalid shard ID               | Error from shard registry validation |
| Invalid member ID              | Error from member ID validation      |

## Examples

**Replication in segment replication stage:**

```kronotop
127.0.0.1:3320> VOLUME.STATS bucket-shard-0 REPLICATION BUCKET 0 standby-member-1
1# "stage" => "SEGMENT_REPLICATION"
2# "status" => "RUNNING"
3# "error_message" => ""
4# "cursor_segment_id" => (integer) 2
5# "cursor_position" => (integer) 51200
6# "cdc_sequence_number" => (integer) 0
7# "cdc_position" => (integer) 0
```

**Replication in change data capture stage:**

```kronotop
127.0.0.1:3320> VOLUME.STATS bucket-shard-0 REPLICATION BUCKET 0 standby-member-1
1# "stage" => "CHANGE_DATA_CAPTURE"
2# "status" => "RUNNING"
3# "error_message" => ""
4# "cursor_segment_id" => (integer) 3
5# "cursor_position" => (integer) 0
6# "cdc_sequence_number" => (integer) 42
7# "cdc_position" => (integer) 1024
```

**Replication not yet started:**

```kronotop
127.0.0.1:3320> VOLUME.STATS bucket-shard-0 REPLICATION BUCKET 0 standby-member-1
1# "stage" => ""
2# "status" => "WAITING"
3# "error_message" => ""
4# "cursor_segment_id" => (integer) 0
5# "cursor_position" => (integer) 0
6# "cdc_sequence_number" => (integer) 0
7# "cdc_position" => (integer) 0
```

**Failed replication:**

```kronotop
127.0.0.1:3320> VOLUME.STATS bucket-shard-0 REPLICATION BUCKET 0 standby-member-1
1# "stage" => "SEGMENT_REPLICATION"
2# "status" => "FAILED"
3# "error_message" => "Connection refused"
4# "cursor_segment_id" => (integer) 1
5# "cursor_position" => (integer) 4096
6# "cdc_sequence_number" => (integer) 0
7# "cdc_position" => (integer) 0
```
