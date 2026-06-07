---
title: "VOLUME.STATS RESET"
description: "Resets operation counters to zero for a named volume."
---

Resets operation counters to zero for a named volume.

## Syntax

```kronotop
VOLUME.STATS <volume-name> RESET
```

## Parameters

| Parameter     | Type   | Description                                                               |
|---------------|--------|---------------------------------------------------------------------------|
| `volume-name` | string | Name of the volume, in `<kind>-shard-<id>` format (e.g. `bucket-shard-0`) |

## Return Value

Simple string `OK`.

## Behavior

Resets the volume's operation counters (appends, deletes, updates, gets, bytes_appended, bytes_read, segments_created)
to their default values (zero).

This affects only in-memory counters. Segment-level statistics (size, used_bytes, free_bytes, cardinality) are derived
from FoundationDB metadata and are not affected by reset.

This command is available on the management port (default 3320).

## Errors

| Condition                                          | Message                                                    |
|----------------------------------------------------|------------------------------------------------------------|
| Volume name parameter is missing                   | `ERR wrong number of arguments for 'VOLUME.STATS' command` |
| No volume with that name is managed by this member | `ERR Volume: '<name>' is not open`                         |

## Examples

**Reset stats:**

```kronotop
127.0.0.1:3320> VOLUME.STATS bucket-shard-0 RESET
OK
```

**Verify counters are zeroed:**

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
