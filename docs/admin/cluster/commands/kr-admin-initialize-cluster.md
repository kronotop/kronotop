---
title: "KR.ADMIN INITIALIZE-CLUSTER"
description: "Initializes a new Kronotop cluster."
---

Initializes a new Kronotop cluster.

## Syntax

```kronotop
KR.ADMIN INITIALIZE-CLUSTER [SHARDS <shard-count>]
```

## Arguments

| Argument      | Type    | Description                                                     |
|---------------|---------|-----------------------------------------------------------------|
| `shard-count` | integer | Number of shards to create. Must be positive. The default is 1. |

## Return Value

Simple string. Returns `OK` on success.

## Behavior

This command creates the cluster metadata that Kronotop needs before it can serve traffic. Run it once, on the
management interface (port 3320 by default).

`INITIALIZE-CLUSTER` is the only `KR.ADMIN` subcommand that works on a cluster that is not initialized yet.

The command also creates the shards. `SHARDS` sets the count, and the default is one shard. Shard IDs start at 0, and
every shard starts as `INOPERABLE`. Before the cluster can accept data, operators must set the shard status and assign
routes.

## Errors

Argument errors:

| Error Code | Error message                                                         | Cause                                             |
|------------|-----------------------------------------------------------------------|---------------------------------------------------|
| `ERR`      | `Wrong number of arguments for 'KR.ADMIN INITIALIZE-CLUSTER' command` | -                                                 |
| `ERR`      | `Unknown argument: '<argument>'`                                      | -                                                 |
| `ERR`      | `SHARDS argument must be followed by a positive integer`              | `SHARDS` has no value, or the value is below 1.   |
| `ERR`      | `Value is not an integer or out of range`                             | The value of `SHARDS` is not a valid integer.     |

Cluster errors:

| Error Code | Error message                          | Cause                                                                                                         |
|------------|----------------------------------------|---------------------------------------------------------------------------------------------------------------|
| `ERR`      | `Cluster has already been initialized` | -                                                                                                             |
| `ERR`      | `Directory already exists: '<path>'`   | A directory that initialization creates exists already. An earlier initialization was partial or conflicting. |

## Examples

**First-time initialization:**

```kronotop
127.0.0.1:3320> KR.ADMIN INITIALIZE-CLUSTER
OK
```

**Initialization with three shards:**

```kronotop
127.0.0.1:3320> KR.ADMIN INITIALIZE-CLUSTER SHARDS 3
OK
```

**Duplicate initialization:**

```kronotop
127.0.0.1:3320> KR.ADMIN INITIALIZE-CLUSTER
(error) ERR Cluster has already been initialized
```
