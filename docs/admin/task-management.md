# Task Management

Kronotop runs various internal maintenance and housekeeping operations as background tasks—such as segment cleanup, 
index rebuilding, metadata verification, and replication checks. The Task Management interface provides visibility 
into these asynchronous operations, allowing operators to monitor their status, execution history, and runtime behavior.

This chapter documents the available task-related admin commands and explains how to inspect, track, and manage internal 
tasks running on each node.

* [Commands](#commands)
  * [TASK.ADMIN LIST](#taskadmin-list)

## Commands

Task management commands are exposed through the **management interface**, which listens on a dedicated TCP port.
By default, this port is `3320`.

To interact with the cluster using admin commands, you can use the standard `redis-cli` tool:

```
redis-cli -3 -p 3320
```

### TASK.ADMIN LIST

`TASK.ADMIN LIST` command returns the current state of all registered background tasks on the local node. 
This includes scheduled system tasks such as cleanup routines, index rebuilders, and metadata maintenance jobs.

This command is useful for monitoring the lifecycle of asynchronous tasks and diagnosing issues related to background processing.

**Syntax**

```
TASK.ADMIN LIST
```

**Example**

```
127.0.0.1:3320> TASK.ADMIN LIST
1# journal:cleanup-task =>
   1# running => (false)
   2# completed => (false)
   3# started_at => (integer) 1752582119
   4# last_run => (integer) 0
```

**Output**

Each entry represents a background task, identified by its name (e.g., journal:cleanup-task). The following fields are available for each task:

* `running`: Indicates whether the task is currently executing (`true` or `false`).
* `completed`: Whether the task has successfully completed at least once.
* `started_at`: The UNIX timestamp (in seconds) of the most recent start time.
* `last_run`" The UNIX timestamp of the last successful execution. 0 means it has not completed yet.

**Notes**

* This command reflects the **local node**’s task state only. To inspect tasks on other nodes, you must connect to them directly.
* Useful for verifying that maintenance jobs are running as expected.
* Task names are system-defined and may vary depending on features in use (e.g., replication, vacuuming, compaction).