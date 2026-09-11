---
title: "COMMAND"
description: "Returns information about registered server commands."
---

Returns information about registered server commands.

## Syntax

```kronotop
COMMAND [subcommand [arguments]]
```

## Subcommands

### COMMAND

Returns information about all commands that have a definition.

```kronotop
COMMAND
```

### COMMAND INFO

Returns information about one or more commands. Without names, it returns all commands. Names are matched without
regard to case. A subcommand is named `container|subcommand`, for example `kr.admin|describe-cluster`. An unknown
name gives a null entry in its position.

```kronotop
COMMAND INFO [command ...]
```

| Parameter | Type   | Required | Description                        |
|-----------|--------|----------|------------------------------------|
| `command` | string | No       | One or more command names to query |

### COMMAND COUNT

Returns the number of commands that have a definition.

```kronotop
COMMAND COUNT
```

### COMMAND DOCS

Returns documentation for commands. Without names, it returns all documented commands. With names, it returns only
those. Names are matched without regard to case. A subcommand is named `container|subcommand`. Unknown names are
skipped.

```kronotop
COMMAND DOCS [command ...]
```

| Parameter | Type   | Required | Description                        |
|-----------|--------|----------|------------------------------------|
| `command` | string | No       | One or more command names to query |

### COMMAND LIST

Returns the names of all commands, subcommands included. A filter keeps only part of the list.

```kronotop
COMMAND LIST [FILTERBY MODULE name | ACLCAT category | PATTERN pattern]
```

| Filter    | Description                                                                                    |
|-----------|------------------------------------------------------------------------------------------------|
| `MODULE`  | Commands of a module. The result is always empty, see below                                    |
| `ACLCAT`  | Commands in an ACL category, for example `read` or `bucket`. An unknown category gives nothing |
| `PATTERN` | Commands whose name matches a glob pattern, without regard to case                             |

Kronotop has no module system. The `MODULE` filter is accepted so that clients written for the standard command
set keep working. Any module name gives an empty array, the same reply a server without that module would give.

### COMMAND GETKEYS

Returns the key arguments of a full command. The command is given as it would be sent to the server.

```kronotop
COMMAND GETKEYS command [arg ...]
```

### COMMAND GETKEYSANDFLAGS

Same as `COMMAND GETKEYS`, but each key comes with the access flags of its key specification.

```kronotop
COMMAND GETKEYSANDFLAGS command [arg ...]
```

### COMMAND HELP

Returns a short description of every subcommand.

```kronotop
COMMAND HELP
```

## Return Value

- **COMMAND / COMMAND INFO:** Array of arrays, one per command, each containing:

| Position | Field              | Type    | Description                                                                 |
|----------|--------------------|---------|-----------------------------------------------------------------------------|
| 1        | name               | string  | Command name, lowercase. Subcommands are named `container\|subcommand`      |
| 2        | arity              | integer | Number of arguments. Negative means "at least"                              |
| 3        | flags              | set     | Lowercase command flags such as `readonly`, `write`, `fast`                 |
| 4        | first key          | integer | Position of the first key argument, 0 when the command has no keys          |
| 5        | last key           | integer | Position of the last key argument, negative counts from the end             |
| 6        | step               | integer | Distance between key arguments                                              |
| 7        | acl categories     | set     | Categories with a `@` prefix, such as `@read`, `@fast`, `@connection`       |
| 8        | tips               | set     | Command tips                                                                |
| 9        | key specifications | set     | One map per key spec with `flags`, `begin_search` and `find_keys`           |
| 10       | subcommands        | array   | Entries of the same shape for each subcommand, an empty set when none exist |

A key specification map has an optional `notes` string, a `flags` set (`RO`, `RW`, `OW`, `RM`, `access`, `update`,
`insert`, `delete`, `not_key`, `incomplete`, `variable_flags`), a `begin_search` map and a `find_keys` map. Both
maps hold a `type` and a `spec` map. `begin_search` types are `index` (`index`), `keyword` (`keyword`,
`startfrom`) and `unknown`. `find_keys` types are `range` (`lastkey`, `keystep`, `limit`), `keynum`
(`keynumidx`, `firstkey`, `keystep`) and `unknown`.

- **COMMAND COUNT:** Integer: number of commands that have a definition.
- **COMMAND DOCS:** Map keyed by lowercase command name. Each value is a map with these keys. Empty fields are left
  out, `group` is always present.

| Key            | Type   | Description                                                       |
|----------------|--------|-------------------------------------------------------------------|
| `summary`      | string | One-line description                                              |
| `since`        | string | Version the command was added in                                  |
| `group`        | string | Command group, for example `bucket` or `connection`               |
| `complexity`   | string | Time complexity                                                   |
| `doc_flags`    | set    | `deprecated` or `syscmd`                                          |
| `history`      | set    | Pairs of version and change description                           |
| `reply_schema` | map    | JSON Schema of the reply, see below                               |
| `arguments`    | array  | Argument descriptions, see below                                  |
| `subcommands`  | map    | Subcommands keyed as `container\|subcommand`, same shape as above |

Each argument is a map with `name`, `type`, `display_text`, `token`, `flags` (`optional`, `multiple`,
`multiple_token`) and, for `oneof` and `block` types, a nested `arguments` array.

The reply schema is a JSON Schema document. JSON objects arrive as maps, JSON arrays as arrays, and strings,
integers and booleans keep their type. On RESP2 every map becomes a flat array of alternating keys and values.

- **COMMAND LIST:** Array of command names, lowercase. Subcommands are named `container|subcommand`.
- **COMMAND GETKEYS:** Array of the key arguments, as sent.
- **COMMAND GETKEYSANDFLAGS:** Array of pairs. Each pair is the key and a set of its key spec flags.
- **COMMAND HELP:** Array of simple strings.

## Behavior

Replies come from the command definitions loaded at startup. A command without a definition is not listed and gives a
null entry in `COMMAND INFO`. Flags and categories are written in a fixed order, not in definition order. On RESP2,
sets and maps arrive as flat arrays.

`COMMAND GETKEYS` finds keys with the key specifications of the command. It fails when the command has no key
specifications, when the argument count does not fit the command arity, or when a specification cannot be applied
to the given arguments. When no key is found the result is the same. A command that has no mandatory keys gives an
empty array in these two cases.

This command does not require the cluster to be initialized.

## Errors

Argument errors:

| Error Code | Error message                                                   | Cause                                              |
|------------|-----------------------------------------------------------------|----------------------------------------------------|
| `ERR`      | `unknown subcommand '<value>'. Try COMMAND HELP.`               | -                                                  |
| `ERR`      | `wrong number of arguments for 'command\|<subcommand>' command` | Subcommand called with the wrong argument count    |
| `ERR`      | `syntax error`                                                  | Bad `COMMAND LIST` filter                          |
| `ERR`      | `Invalid command specified`                                     | `GETKEYS` with an unknown command                  |
| `ERR`      | `The command has no key arguments`                              | `GETKEYS` with a command without key specs         |
| `ERR`      | `Invalid number of arguments specified for command`             | `GETKEYS` argument count does not fit the arity    |
| `ERR`      | `Invalid arguments specified for command`                       | `GETKEYS` key specs do not apply to the arguments  |

## Examples

**Get documentation for PING (RESP3):**

```kronotop
127.0.0.1:5484> COMMAND DOCS ping
1# "ping" =>
   1# "summary" => "Ping the server"
   2# "since" => "1.0.0"
   3# "group" => "connection"
   4# "complexity" => "O(1)"
   5# "arguments" =>
      1) 1# "name" => "message"
         2# "type" => "string"
         3# "display_text" => "message"
         4# "flags" => ~1 "optional"
```

**Get command count:**

```kronotop
127.0.0.1:5484> COMMAND COUNT
(integer) 42
```

**Get info for PING (RESP3):**

```kronotop
127.0.0.1:5484> COMMAND INFO ping
1) 1) "ping"
   2) (integer) -1
   3) 1~ fast
   4) (integer) 0
   5) (integer) 0
   6) (integer) 0
   7) 1~ @fast
      2~ @connection
   8) 1~ "REQUEST_POLICY:ALL_SHARDS"
      2~ "RESPONSE_POLICY:ALL_SUCCEEDED"
   9) (empty set)
  10) (empty set)
```

**List commands by pattern:**

```kronotop
127.0.0.1:5484> COMMAND LIST FILTERBY PATTERN bucket.*
1) "bucket.query"
```

**Get the keys of a command:**

```kronotop
127.0.0.1:5484> COMMAND GETKEYS set foo bar
1) "foo"
```

**Get the keys with their flags (RESP3):**

```kronotop
127.0.0.1:5484> COMMAND GETKEYSANDFLAGS get foo
1) 1) "foo"
   2) 1~ RO
      2~ access
```
