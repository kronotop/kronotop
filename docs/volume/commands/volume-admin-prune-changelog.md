---
title: "VOLUME.ADMIN PRUNE-CHANGELOG"
description: "Removes changelog entries older than a given retention period to reclaim storage."
---

Removes changelog entries older than a given retention period to reclaim storage.

## Syntax

```kronotop
VOLUME.ADMIN PRUNE-CHANGELOG <volume-name> <retention-period>
```

## Parameters

| Parameter          | Type    | Description                                                                |
|--------------------|---------|----------------------------------------------------------------------------|
| `volume-name`      | string  | Name of the volume, in `<kind>-shard-<id>` format (e.g. `bucket-shard-0`)  |
| `retention-period` | integer | Number of hours of changelog history to retain. Must be greater than zero. |

## Return Value

Simple string `OK` on success.

## Behavior

Calculates a cutoff timestamp as `now() - retention-period` hours. Clears changelog entries older than the cutoff
in a single atomic operation.

It is available on the management port (default 3320).

## Errors

Argument errors:

| Error Code | Error message                                | Cause                                                   |
|------------|----------------------------------------------|---------------------------------------------------------|
| `ERR`      | `invalid number of parameters`               | -                                                       |
| `ERR`      | `retention period must be greater than zero` | -                                                       |
| `ERR`      | `invalid volume name: <name>`                | The name does not match the `<kind>-shard-<id>` format. |

## Examples

**Prune changelog entries older than 24 hours:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN PRUNE-CHANGELOG bucket-shard-0 24
OK
```

**Retention period is zero:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN PRUNE-CHANGELOG bucket-shard-0 0
(error) ERR retention period must be greater than zero
```

**Invalid volume name:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN PRUNE-CHANGELOG non-existent-volume 24
(error) ERR invalid volume name: non-existent-volume
```

**Missing parameters:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN PRUNE-CHANGELOG
(error) ERR invalid number of parameters
```
