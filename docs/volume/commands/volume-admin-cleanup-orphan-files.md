---
title: "VOLUME.ADMIN CLEANUP-ORPHAN-FILES"
description: "Identifies and removes orphaned segment files from a volume's data directory that are no longer tracked in metadata."
---

Identifies and removes orphaned segment files from a volume's data directory that are no longer tracked in metadata.

## Syntax

```kronotop
VOLUME.ADMIN CLEANUP-ORPHAN-FILES <volume-name>
```

## Parameters

| Parameter     | Type   | Description                                            |
|---------------|--------|--------------------------------------------------------|
| `volume-name` | string | Name of the volume to clean up (e.g. `bucket-shard-0`) |

## Return Value

Array of bulk strings, each containing the absolute path of a deleted orphan file. Returns an empty array if no orphan
files were found.

## Behavior

Loads volume metadata and builds a set of expected segment file names, then lists the actual files present in the
volume's `segments/` directory on disk. Any file on disk that is not in the expected set is considered an orphan and is
deleted. The absolute paths of successfully deleted files are returned.

Orphan segment files remain on disk after crashes where the metadata entry was removed, but the file was not deleted.

It is available on the management port (default 3320).

## Errors

Argument errors:

| Error Code | Error message                  | Cause |
|------------|--------------------------------|-------|
| `ERR`      | `invalid number of parameters` | -     |

Volume errors:

| Error Code | Error message                  | Cause                                               |
|------------|--------------------------------|-----------------------------------------------------|
| `ERR`      | `Volume: '<name>' is not open` | No volume with that name is managed by this member. |
| `ERR`      | `Volume <name> is closed.`     | -                                                   |
| `ERR`      | `File not found: <path>`       | Segments directory not found.                       |

## Examples

**Orphan files found and deleted:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN CLEANUP-ORPHAN-FILES bucket-shard-0
1) "/var/kronotop/data/bucket-shard-0/segments/00000a.seg"
2) "/var/kronotop/data/bucket-shard-0/segments/00000b.seg"
```

**No orphan files:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN CLEANUP-ORPHAN-FILES bucket-shard-0
(empty array)
```

**Volume not found:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN CLEANUP-ORPHAN-FILES non-existent-volume
(error) ERR Volume: 'non-existent-volume' is not open
```
