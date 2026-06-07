---
title: "HELLO"
description: "Negotiates the protocol version and optionally authenticates the connection."
---

Negotiates the protocol version and optionally authenticates the connection.

## Syntax

```kronotop
HELLO protover [AUTH username password] [SETNAME clientname]
```

## Parameters

| Parameter                | Type           | Required | Description                                            |
|--------------------------|----------------|----------|--------------------------------------------------------|
| `protover`               | integer        | Yes      | Protocol version to use (`2` for RESP2, `3` for RESP3) |
| `AUTH username password` | string, string | No       | Optional inline authentication                         |
| `SETNAME clientname`     | string         | No       | Optional client connection name                        |

## Return Value

Returns connection metadata in a format determined by the negotiated protocol version:

- **RESP2:** Array of alternating key-value pairs.
- **RESP3:** Map.

| Field     | Type    | Description                      |
|-----------|---------|----------------------------------|
| `server`  | string  | Server product name (`Kronotop`) |
| `version` | string  | Server version                   |
| `proto`   | integer | Negotiated protocol version      |
| `id`      | integer | Client connection ID             |
| `mode`    | string  | Server mode (`cluster`)          |
| `role`    | string  | Server role (`master`)           |
| `modules` | array   | Loaded modules (empty array)     |

## Behavior

Sets the RESP protocol version for the current session. If `AUTH` is provided, authenticates the connection using the
same logic as the `AUTH` command. If `SETNAME` is provided, sets the client connection name.

The protocol version is applied after the response is generated, so the response itself is encoded using the newly
negotiated version.

This command does not require the cluster to be initialized.

## Errors

| Error       | Condition                                       |
|-------------|-------------------------------------------------|
| `NOPROTO`   | Unsupported protocol version (not 2 or 3)       |
| `WRONGPASS` | Invalid username or password in the AUTH clause |

## Examples

**Switch to RESP3:**

```kronotop
127.0.0.1:5484> HELLO 3
1# "server" => "Kronotop"
2# "version" => "0.13"
3# "proto" => (integer) 3
4# "id" => (integer) 1
5# "mode" => "cluster"
6# "role" => "master"
7# "modules" => (empty array)
```

**Switch to RESP2 with authentication:**

```kronotop
127.0.0.1:5484> HELLO 2 AUTH admin mysecretpassword
 1) "server"
 2) "Kronotop"
 3) "version"
 4) "0.13"
 5) "proto"
 6) (integer) 2
 7) "id"
 8) (integer) 1
 9) "mode"
10) "cluster"
11) "role"
12) "master"
13) "modules"
14) (empty array)
```

**Set client name:**

```kronotop
127.0.0.1:5484> HELLO 3 SETNAME my-app
1# "server" => "Kronotop"
2# "version" => "0.13"
3# "proto" => (integer) 3
4# "id" => (integer) 1
5# "mode" => "cluster"
6# "role" => "master"
7# "modules" => (empty array)
```
