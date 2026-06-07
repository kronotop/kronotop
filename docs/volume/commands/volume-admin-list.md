---
title: "VOLUME.ADMIN LIST"
description: "Lists all volumes opened by the connected member."
---

Lists all volumes opened by the connected member.

## Syntax

```kronotop
VOLUME.ADMIN LIST
```

## Parameters

None.

## Return Value

RESP array of bulk strings. Each element is a volume name in `<kind>-shard-<id>` format (e.g. `bucket-shard-0`). Returns
an empty array if no volumes are open.

## Behavior

Returns all volume names managed by the connected member. This command does not require cluster initialization.
It is available on the management port (default 3320).

## Errors

No command-specific errors. The command takes no parameters beyond the subcommand itself.

## Examples

**Member managing several shards:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN LIST
1) "bucket-shard-0"
2) "bucket-shard-1"
3) "bucket-shard-2"
```

**No volumes open:**

```kronotop
127.0.0.1:3320> VOLUME.ADMIN LIST
(empty array)
```
