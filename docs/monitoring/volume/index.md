---
title: "VOLUME.STATS Commands"
description: "Monitoring commands for inspecting volume health, performance counters, and replication state."
sidebar:
  label: Overview
---

Monitoring commands for inspecting volume health, performance counters, and replication state.

All `VOLUME.STATS` commands are available on the management port (default 3320).

## Commands

| Command                                                          | Description                                                 |
|------------------------------------------------------------------|-------------------------------------------------------------|
| [VOLUME.STATS](commands/volume-stats.md)                         | Volume-wide overview: status, capacity, garbage percentage  |
| [VOLUME.STATS OPCOUNTERS](commands/volume-stats-opcounters.md)   | In-memory operation counters (appends, deletes, gets, etc.) |
| [VOLUME.STATS SEGMENTS](commands/volume-stats-segments.md)       | Per-segment size, usage, and garbage breakdown              |
| [VOLUME.STATS REPLICATION](commands/volume-stats-replication.md) | Replication state for a specific standby member             |
| [VOLUME.STATS RESET](commands/volume-stats-reset.md)             | Reset operation counters to zero                            |
