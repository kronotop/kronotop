---
title: "VOLUME.ADMIN MARK-STALE-PREFIXES"
description: "Starts, stops, removes, or locates the stale-prefix scanning task."
---

Starts, stops, removes, or locates the stale-prefix scanning task. A stale prefix is one whose pointer no longer
references valid data. Stale prefixes are cleared and published to a disused-prefixes journal for downstream cleanup.

## Syntax

```kronotop
VOLUME.ADMIN MARK-STALE-PREFIXES <operation>
```

## Parameters

| Parameter   | Type   | Description                                                      |
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
- **LOCATE:** A map with `member_id` (string), `process_id` (Base32Hex-encoded string), `external_address` (string,
  host:port or null), and `internal_address` (string, host:port or null) identifying the task owner.

## Behavior

Prefix pointers in the global prefix registry can become stale, the pointer target may have been deleted,
or the data it references may no longer match. This task provides a safe, batched way to identify and remove these stale
entries, keeping the prefix registry clean. The task scans the global prefix registry in batches. For each prefix, it
checks whether the pointer still references valid data. Stale prefixes are cleared and published to a disused-prefixes
journal for downstream cleanup.

Progress is tracked via a progress marker stored persistently, allowing the task to resume from where it left off if
restarted. The task auto-completes when a batch returns zero entries, and removes its own metadata upon natural
completion.

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

| Condition                          | Message                                                             |
|------------------------------------|---------------------------------------------------------------------|
| Missing or extra parameters        | `ERR invalid number of parameters`                                  |
| Invalid operation value            | `ERR invalid operation: <value>`                                    |
| Task already running               | `ERR Task volume:mark-stale-prefixes-task already exists`           |
| STOP when no task is running       | `ERR Task with name volume:mark-stale-prefixes-task does not exist` |
| REMOVE when no task is running     | `ERR Task with name volume:mark-stale-prefixes-task does not exist` |
| Task owned by another alive member | `ERR Run by another cluster member`                                 |
| LOCATE when no metadata exists     | `ERR no metadata found`                                             |

## Examples

**Start the task:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN MARK-STALE-PREFIXES START
OK
```

**Stop a running task:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN MARK-STALE-PREFIXES STOP
OK
```

**Remove a task and its metadata:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN MARK-STALE-PREFIXES REMOVE
OK
```

**Start when the task is already running:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN MARK-STALE-PREFIXES START
(error) ERR Task volume:mark-stale-prefixes-task already exists
```

**Locate the task owner:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN MARK-STALE-PREFIXES LOCATE
1# "member_id" => "cdef7490344552d50e60f42b04e1febcaeafd4b4"
2# "process_id" => "000016M5BPGLO0000000xxxx"
3# "external_address" => "192.168.1.10:5484"
4# "internal_address" => "192.168.1.10:3320"
```

**Stop when no task is running:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN MARK-STALE-PREFIXES STOP
(error) ERR Task with name volume:mark-stale-prefixes-task does not exist
```
