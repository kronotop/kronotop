---
title: "KR.ADMIN COLLECT-STALE-PREFIXES"
description: "Starts, stops, removes, or locates the stale prefix collection task."
---

Starts, stops, removes, or locates the stale prefix collection task. A stale prefix is one whose pointer no longer
references valid data. Stale prefixes are cleared and published to a prefix-invalidations journal for downstream
cleanup.

## Syntax

```kronotop
KR.ADMIN COLLECT-STALE-PREFIXES <operation>
```

## Arguments

| Argument    | Type   | Description                                                      |
|-------------|--------|------------------------------------------------------------------|
| `operation` | string | One of `START`, `STOP`, `REMOVE`, or `LOCATE` (case-insensitive) |

**Operations:**

| Operation | Description                                                                                 |
|-----------|---------------------------------------------------------------------------------------------|
| `START`   | Starts the background task. Fails if the task is already running.                           |
| `STOP`    | Gracefully stops the running task and removes it from the task registry.                    |
| `REMOVE`  | Stops the task and also removes its persisted metadata.                                     |
| `LOCATE`  | Returns the member ID and process ID of the member that owns the task's persisted metadata. |

## Return Value

- **START, STOP, REMOVE:** Simple string `OK` on success. `OK` means the operation was accepted, not that scanning has
  finished.
- **LOCATE:** A map that identifies the task owner. It contains `member_id` (string), `process_id` (Base32Hex-encoded
  string), `external_advertise` and `internal_advertise`. The two advertise fields are arrays of `host:port` strings,
  and they are empty when the member is no longer registered.

## Behavior

Prefix pointers in the global prefix registry can become stale, the pointer target may have been deleted,
or the data it references may no longer match. This task provides a safe, batched way to identify and remove these stale
entries, keeping the prefix registry clean. The task scans the global prefix registry in batches. For each prefix, it
checks whether the pointer still references valid data. Stale prefixes are cleared and published to a
prefix-invalidations
journal for downstream cleanup.

Progress is tracked via a progress marker stored persistently, allowing the task to resume from where it left off if
restarted. The task auto-completes when a batch returns zero entries, and removes its own metadata upon natural
completion. The task also leaves the task registry at that point, so a later `STOP` or `REMOVE` returns `No such task`.

Task ownership is validated using both member ID and process ID. If the original member has been removed from the
cluster
or restarted (different process ID), another member can take over the task and resume from the last progress marker.
If the original member is still registered in the cluster with the same process ID, the command fails.

When a member starts up, it checks for persisted task metadata that matches its own member ID. If found, the task is
automatically resumed from the last progress marker without requiring a manual `START` command. This means that if a
member
is restarted while the task is in progress, the task picks up where it left off.

It is available on the management port (default 3320).

## Errors

Argument errors:

| Error Code | Error message                                                             | Cause |
|------------|---------------------------------------------------------------------------|-------|
| `ERR`      | `Wrong number of arguments for 'KR.ADMIN COLLECT-STALE-PREFIXES' command` | -     |
| `ERR`      | `Unknown operation: '<value>'`                                            | -     |

Task errors:

| Error Code | Error message                                                                          | Cause                                    |
|------------|----------------------------------------------------------------------------------------|------------------------------------------|
| `ERR`      | `Task already exists: 'cluster:collect-stale-prefixes-task'`                           | -                                        |
| `ERR`      | `No such task: 'cluster:collect-stale-prefixes-task'`                                  | -                                        |
| `ERR`      | `Run by another cluster member`                                                        | -                                        |
| `ERR`      | `Another member is starting task cluster:collect-stale-prefixes-task at the same time` | Two members ran `START` at the same time |
| `ERR`      | `No metadata found`                                                                    | -                                        |

## Examples

**Start the task:**

```kronotop
127.0.0.1:3320> KR.ADMIN COLLECT-STALE-PREFIXES START
OK
```

**Stop a running task:**

```kronotop
127.0.0.1:3320> KR.ADMIN COLLECT-STALE-PREFIXES STOP
OK
```

**Remove a task and its metadata:**

```kronotop
127.0.0.1:3320> KR.ADMIN COLLECT-STALE-PREFIXES REMOVE
OK
```

**Start when the task is already running:**

```kronotop
127.0.0.1:3320> KR.ADMIN COLLECT-STALE-PREFIXES START
(error) ERR Task already exists: 'cluster:collect-stale-prefixes-task'
```

**Locate the task owner:**

```kronotop
127.0.0.1:3320> KR.ADMIN COLLECT-STALE-PREFIXES LOCATE
1# "member_id" => "cdef7490344552d50e60f42b04e1febcaeafd4b4"
2# "process_id" => "000016M5BPGLO0000000xxxx"
3# "external_advertise" =>
   1) "192.168.1.10:5484"
4# "internal_advertise" =>
   1) "192.168.1.10:3320"
```

**Stop when no task is running:**

```kronotop
127.0.0.1:3320> KR.ADMIN COLLECT-STALE-PREFIXES STOP
(error) ERR No such task: 'cluster:collect-stale-prefixes-task'
```
