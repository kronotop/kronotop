---
title: "VOLUME.ADMIN REPLICATION"
description: "Starts or stops volume replication on a standby node."
---

Starts or stops volume replication on a standby node.

## Syntax

```kronotop
VOLUME.ADMIN REPLICATION <operation> <volume-name>
```

## Parameters

| Parameter     | Type   | Description                                                                          |
|---------------|--------|--------------------------------------------------------------------------------------|
| `operation`   | string | One of `START` or `STOP` (case-insensitive)                                          |
| `volume-name` | string | Volume identifier (e.g. `bucket-shard-0`). Use `VOLUME.ADMIN LIST` to discover names |

**Operations:**

| Operation | Description                                                                      |
|-----------|----------------------------------------------------------------------------------|
| `START`   | Starts replication. Resumes processing if it was previously stopped.             |
| `STOP`    | Stops replication and prevents automatic restart until explicitly started again. |

## Return Value

Simple string `OK` on success. `OK` means the request was accepted.

## Behavior

Both operations verify that the current node is listed as a standby in the route for the given volume.

**START** resumes replication processing. Replication normally starts automatically when a standby is assigned via
routing, but if it was previously stopped (e.g. for maintenance), this command re-initiates it manually.

**STOP** gracefully shuts down replication and prevents automatic restart until explicitly started again.

This command must be run on the standby node. It is available on the management port (default 3320).

## Errors

Argument errors:

| Error Code | Error message                       | Cause                                                   |
|------------|-------------------------------------|---------------------------------------------------------|
| `ERR`      | `invalid number of parameters`      | -                                                       |
| `ERR`      | `invalid volume name: <name>`       | The name does not match the `<kind>-shard-<id>` format. |
| `ERR`      | `unknown subcommand: '<operation>'` | -                                                       |

Volume errors:

| Error Code | Error message                                  | Cause |
|------------|------------------------------------------------|-------|
| `ERR`      | `No route found for <volume-name>`             | -     |
| `ERR`      | `This node is not a standby for <volume-name>` | -     |

## Examples

**Start replication on a standby node:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN REPLICATION START bucket-shard-1
OK
```

**Stop replication on a standby node:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN REPLICATION STOP bucket-shard-1
OK
```

**Node is not a standby:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN REPLICATION START bucket-shard-1
(error) ERR This node is not a standby for bucket-shard-1
```

**Missing parameters:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN REPLICATION
(error) ERR invalid number of parameters
```
