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

| Argument    | Type   | Required | Description                             |
|-------------|--------|----------|-----------------------------------------|
| `attribute` | string | Yes      | Attribute name: `lib-name` or `lib-ver` |
| `value`     | string | Yes      | Attribute value                         |

### CLIENT SETNAME

Sets a human-readable name for the current connection.

```kronotop
CLIENT SETNAME <name>
```

| Argument | Type   | Required | Description     |
|----------|--------|----------|-----------------|
| `name`   | string | Yes      | Connection name |

## Return Value

All subcommands return simple string `OK` on success.

## Behavior

`CLIENT SETINFO` stores library metadata (`lib-name` or `lib-ver`) on the connection. `CLIENT SETNAME` sets the
connection name. Both values live as long as the connection does, so `SESSION.CLOSE` keeps them.

This command does not require the cluster to be initialized.

## Errors

Argument errors:

| Error Code | Error message                                                         | Cause                                                                       |
|------------|-----------------------------------------------------------------------|-----------------------------------------------------------------------------|
| `ERR`      | `Wrong number of arguments for 'CLIENT\|<subcommand>' command`        | -                                                                           |
| `ERR`      | `Unrecognized option '<attribute>'`                                   | The `SETINFO` attribute is not `lib-name` or `lib-ver`.                     |
| `ERR`      | `<attribute> cannot contain spaces, newlines or special characters.`  | The `SETINFO` value has a space or a character that is not printable ASCII. |
| `ERR`      | `Client names cannot contain spaces, newlines or special characters.` | The `SETNAME` name has a space or a character that is not printable ASCII.  |
| `ERR`      | `unknown subcommand '<value>'. Try CLIENT HELP.`                      | -                                                                           |

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
