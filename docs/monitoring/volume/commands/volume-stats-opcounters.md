---
title: "VOLUME.STATS OPCOUNTERS"
description: "Returns in-memory operation counters for a named volume."
---

Returns in-memory operation counters for a named volume.

## Syntax

```kronotop
VOLUME.STATS <volume-name> OPCOUNTERS
```

## Parameters

| Parameter     | Type   | Description                                                               |
|---------------|--------|---------------------------------------------------------------------------|
| `volume-name` | string | Name of the volume, in `<kind>-shard-<id>` format (e.g. `bucket-shard-0`) |

## Return Value

RESP3 map with the following fields:

| Field              | Type    | Description                                 |
|--------------------|---------|---------------------------------------------|
| `appends`          | integer | Number of append operations                 |
| `deletes`          | integer | Number of delete operations                 |
| `updates`          | integer | Number of update operations                 |
| `gets`             | integer | Number of get operations                    |
| `bytes_appended`   | integer | Total bytes written via appends and updates |
| `bytes_read`       | integer | Total bytes read via get operations         |
| `segments_created` | integer | Number of new segments allocated            |

## Behavior

Returns the current values of the volume's in-memory atomic counters. These counters accumulate from the moment the
volume is opened and reset to zero on server restart or when `VOLUME.STATS RESET` is called.

`bytes_appended` includes bytes written by both append and update operations, since updates write new data to the
current writable segment.

This command is available on the management port (default 3320).

## Errors

| Condition                                          | Message                                                    |
|----------------------------------------------------|------------------------------------------------------------|
| Volume name parameter is missing                   | `ERR wrong number of arguments for 'VOLUME.STATS' command` |
| No volume with that name is managed by this member | `ERR Volume: '<name>' is not open`                         |

## Examples

**Counters after some operations:**

```kronotop
127.0.0.1:3320> VOLUME.STATS bucket-shard-0 OPCOUNTERS
1# "appends" => (integer) 50
2# "deletes" => (integer) 5
3# "updates" => (integer) 10
4# "gets" => (integer) 30
5# "bytes_appended" => (integer) 6000
6# "bytes_read" => (integer) 3000
7# "segments_created" => (integer) 1
```

**Fresh volume (no operations yet):**

```kronotop
127.0.0.1:3320> VOLUME.STATS bucket-shard-0 OPCOUNTERS
1# "appends" => (integer) 0
2# "deletes" => (integer) 0
3# "updates" => (integer) 0
4# "gets" => (integer) 0
5# "bytes_appended" => (integer) 0
6# "bytes_read" => (integer) 0
7# "segments_created" => (integer) 0
```
