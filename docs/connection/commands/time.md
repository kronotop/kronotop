---
title: "TIME"
description: "Returns the current server time."
---

Returns the current wall-clock time of the server with microsecond resolution, read from the operating
system's realtime clock.

## Syntax

```kronotop
TIME
```

Takes no arguments.

## Return Value

Array of two bulk strings:

1. Unix timestamp in seconds
2. Microseconds already elapsed in the current second

## Behavior

The value comes from the system clock of the node that handles the connection. Different nodes can report
different times.

Wall-clock time is not monotonic. It can move backwards on clock adjustments. Callers that need a monotonic
value should use [TICK](../../transactions/commands/tick.md) instead.

This command does not require the cluster to be initialized. It does not require an active transaction.

## Errors

No command-specific errors.

## Examples

```kronotop
> TIME
1) "1781119691"
2) "14102"
```
