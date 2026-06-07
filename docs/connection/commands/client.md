---
title: "CLIENT"
description: "Manages client connection properties."
---

Manages client connection properties.

## Syntax

```kronotop
CLIENT <subcommand> [arguments]
```

## Subcommands

### CLIENT SETINFO

Sets client library metadata on the current connection.

```kronotop
CLIENT SETINFO <attribute> <value>
```

| Parameter   | Type   | Required | Description                             |
|-------------|--------|----------|-----------------------------------------|
| `attribute` | string | Yes      | Attribute name: `lib-name` or `lib-ver` |
| `value`     | string | Yes      | Attribute value                         |

### CLIENT SETNAME

Sets a human-readable name for the current connection.

```kronotop
CLIENT SETNAME <name>
```

| Parameter | Type   | Required | Description     |
|-----------|--------|----------|-----------------|
| `name`    | string | Yes      | Connection name |

## Return Value

All subcommands return simple string `OK` on success.

## Behavior

`CLIENT SETINFO` stores library metadata (`lib-name` or `lib-ver`) on the session. `CLIENT SETNAME` sets the connection
name on the session.

This command does not require the cluster to be initialized.

## Errors

**`ERR`** is returned when:

- Wrong number of arguments for the subcommand.
- Unrecognized attribute for `SETINFO` (not `lib-name` or `lib-ver`).
- Unknown subcommand.

## Examples

**Set library name:**

```kronotop
127.0.0.1:5484> CLIENT SETINFO lib-name jedis
OK
```

**Set library version:**

```kronotop
127.0.0.1:5484> CLIENT SETINFO lib-ver 4.3.1
OK
```

**Set connection name:**

```kronotop
127.0.0.1:5484> CLIENT SETNAME my-app-connection
OK
```
