---
title: "VOLUME.ADMIN SET-STATUS"
description: "Changes the operational status of a named volume."
---

Changes the operational status of a named volume.

## Syntax

```kronotop
VOLUME.ADMIN SET-STATUS <volume-name> <status>
```

## Parameters

| Parameter     | Type   | Description                                                                         |
|---------------|--------|-------------------------------------------------------------------------------------|
| `volume-name` | string | Name of the volume to update, in `<kind>-shard-<id>` format (e.g. `bucket-shard-0`) |
| `status`      | string | New operational status: `READWRITE`, `READONLY`, or `INOPERABLE` (case-insensitive) |

## Return Value

Simple string `OK` on success.

## Behavior

Persists the new status. The status input is converted to uppercase before validation.

It does not require cluster initialization. It is available on the management port (default 3320).

## Errors

| Condition                                          | Message                                       |
|----------------------------------------------------|-----------------------------------------------|
| Missing volume name or status parameter            | `ERR invalid number of parameters`            |
| No volume with that name is managed by this member | `ERR Volume: '<name>' is not open`            |
| Status value is not a valid volume status          | `ERR Invalid volume status: <provided-value>` |

## Examples

**Set a volume to read-only:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN SET-STATUS bucket-shard-0 READONLY
OK
```

**Verify via DESCRIBE:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN DESCRIBE bucket-shard-0
1# "name" => "bucket-shard-0"
2# "status" => "READONLY"
3# "data_dir" => "/tmp/kronotop/data/bucket-shard-0"
4# "segment_size" => (integer) 268435456
5# "segments" =>
   1# (integer) 0 =>
      1# "size" => (integer) 268435456
      2# "free_bytes" => (integer) 268435200
      3# "used_bytes" => (integer) 256
      4# "garbage_percentage" => (double) 0.0
      5# "cardinality" => (integer) 1
```

**Invalid status value:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN SET-STATUS bucket-shard-0 INVALID
(error) ERR Invalid volume status: INVALID
```

**Volume not found:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN SET-STATUS non-existent-volume READONLY
(error) ERR Volume: 'non-existent-volume' is not open
```
