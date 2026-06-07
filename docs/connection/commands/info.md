---
title: "INFO"
description: "Returns server information and statistics."
---

Returns server information and statistics.

## Syntax

```kronotop
INFO [section ...]
```

## Parameters

| Parameter | Type   | Required | Description                                               |
|-----------|--------|----------|-----------------------------------------------------------|
| `section` | string | No       | One or more section names to filter (not yet implemented) |

## Return Value

Bulk string containing server information formatted as `key:value` pairs grouped under `# Section` headers.

Currently returned sections:

| Section   | Fields                                 |
|-----------|----------------------------------------|
| `Server`  | `kronotop_version`, `redis_mode`, `os` |
| `Cluster` | `cluster_enabled`                      |

## Behavior

Builds and returns a bulk string with server metadata. Each section is prefixed with a `# SectionName` header, followed
by `key:value` lines.

Section filtering is not yet implemented. All sections are returned regardless of arguments.

This command does not require the cluster to be initialized.

## Errors

No command-specific errors.

## Examples

```kronotop
127.0.0.1:5484> INFO
# Server
kronotop_version:0.13
redis_mode:cluster
os:Linux 5.15.0 amd64

# Cluster
cluster_enabled:1
```
