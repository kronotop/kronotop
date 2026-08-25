---
title: "Task Management"
description: "Kronotop runs various internal maintenance and housekeeping operations as background tasks, such as segment cleanup, index rebuilding, metadata verification, and replication checks."
---

Kronotop runs various internal maintenance and housekeeping operations as background tasks, such as segment cleanup,
index rebuilding, metadata verification, and replication checks. The Task Management interface provides visibility
into these asynchronous operations, allowing operators to monitor their status, execution history, and runtime behavior.

This chapter documents the available task-related admin commands and explains how to inspect, track, and manage internal
tasks running on each node.

## Commands

Task management commands are exposed through the **management interface**, which listens on a dedicated TCP port.
By default, this port is `3320`.

To interact with the cluster using admin commands, you can use the standard `valkey-cli` tool:

```bash
valkey-cli -3 -p 3320
```

### TASK.ADMIN LIST

`TASK.ADMIN LIST` command returns the current state of all registered background tasks on the local node.
This includes scheduled system tasks such as cleanup routines, index rebuilders, and metadata maintenance jobs.

This command is useful for monitoring the lifecycle of asynchronous tasks and diagnosing issues related to background
processing.

**Syntax**

```kronotop
TASK.ADMIN LIST
```

**Example**

```kronotop
127.0.0.1:3320> TASK.ADMIN LIST
1# journal:cleanup-task =>
   1# running => (false)
   2# finished => (false)
   3# started_at => (integer) 1752582119
   4# last_run => (integer) 0
```

**Output**

Each entry represents a background task, identified by its name (e.g., journal:cleanup-task). The following fields are
available for each task:

* `running`: Indicates whether the task is currently executing (`true` or `false`).
* `finished`: Whether the task has stopped running.
* `started_at`: The UNIX timestamp (in seconds) of when the task was registered.
* `last_run`: The UNIX timestamp of the last successful execution. 0 means it has not run yet.

**RESP2 clients**

RESP2 has no map and no boolean type. On a RESP2 connection the reply is a flat array where each task name is followed
by its field list, and `running` and `finished` are sent as `1` or `0`:

```kronotop
127.0.0.1:3320> TASK.ADMIN LIST
1) "journal:cleanup-task"
2) 1) "running"
   2) (integer) 0
   3) "finished"
   4) (integer) 0
   5) "started_at"
   6) (integer) 1752582119
   7) "last_run"
   8) (integer) 0
```

**Notes**

* This command reflects the **local node**’s task state only. To inspect tasks on other nodes, you must connect to them
  directly.
* Useful for verifying that maintenance jobs are running as expected.
* Task names are system-defined and may vary depending on features in use (e.g., replication, compaction).
