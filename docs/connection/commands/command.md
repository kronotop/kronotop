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

Returns information about all registered commands.

```kronotop
COMMAND
```

### COMMAND INFO

Returns information about one or more specific commands.

```kronotop
COMMAND INFO [command ...]
```

| Parameter | Type   | Required | Description                                                 |
|-----------|--------|----------|-------------------------------------------------------------|
| `command` | string | No       | One or more command names to query (returns all if omitted) |

### COMMAND COUNT

Returns the total number of registered commands.

```kronotop
COMMAND COUNT
```

### COMMAND DOCS

Returns command documentation (not yet implemented).

```kronotop
COMMAND DOCS
```

## Return Value

- **COMMAND / COMMAND INFO:** Array of arrays, one per command, each containing:

| Position | Field              | Type    | Description                             |
|----------|--------------------|---------|-----------------------------------------|
| 1        | name               | string  | Command name (lowercase)                |
| 2        | arity              | integer | Number of arguments                     |
| 3        | flags              | array   | Command flags (e.g. `readonly`, `fast`) |
| 4        | first key          | integer | Position of the first key argument      |
| 5        | last key           | integer | Position of the last key argument       |
| 6        | step               | integer | Key step interval                       |
| 7        | acl categories     | array   | ACL categories (prefixed with `@`)      |
| 8        | tips               | array   | Command tips                            |
| 9        | key specifications | array   | Key specification details               |
| 10       | subcommands        | array   | Subcommand info (empty)                 |

- **COMMAND COUNT:** Integer: total number of commands.
- **COMMAND DOCS:** Empty array (not yet implemented).

## Behavior

Reads command metadata from the server's command registry. For `COMMAND` and `COMMAND INFO`, builds a detailed array
structure per command. `COMMAND COUNT` returns the total count of all registered commands.

This command does not require the cluster to be initialized.

## Errors

| Error | Condition          |
|-------|--------------------|
| `ERR` | Unknown subcommand |

## Examples

**Get command count:**

```kronotop
127.0.0.1:5484> COMMAND COUNT
(integer) 42
```

**Get info for PING:**

```kronotop
127.0.0.1:5484> COMMAND INFO ping
1) 1) "ping"
   2) (integer) -1
   3) 1) "fast"
   4) (integer) 0
   5) (integer) 0
   6) (integer) 0
   7) 1) "@connection"
   8) (empty array)
   9) (empty array)
  10) (empty array)
```
