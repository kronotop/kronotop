---
title: "Connection"
sidebar:
  label: "Overview"
description: "Kronotop speaks RESP2 and RESP3. Connection commands handle protocol negotiation, authentication, and server introspection."
---

## Overview

Kronotop speaks RESP2 and RESP3, so existing RESP-compatible clients and tools connect without a special driver.
Every instance listens on two ports: the client port (default 5484) serves regular workloads, and the internal port
(default 3320) serves cluster administration.

Opening a TCP connection creates exactly one [session](../sessions/index.md) that holds all per-client state:
configuration attributes, query cursors, and the active transaction. The session lives as long as the connection.

## Protocol Negotiation

New connections start in RESP2. `HELLO` switches the protocol version for the rest of the connection and returns
connection metadata. The response itself is already encoded with the newly negotiated version:

```kronotop
> HELLO 3
1# "server" => "Kronotop"
2# "version" => "0.13"
3# "proto" => (integer) 3
4# "id" => (integer) 1
5# "mode" => "cluster"
6# "role" => "master"
7# "modules" => (empty array)
```

RESP3 is the better choice for new applications. Structured replies arrive as native maps instead of flat arrays,
and all examples in this documentation use RESP3 output.

## Authentication

Authentication is disabled by default. When an `auth` block is present in the configuration, the connection must
authenticate before doing anything else. Until then, every command except `AUTH` and `HELLO` is rejected:

```kronotop
> BUCKET.LIST
(error) NOAUTH Authentication required.

> AUTH devpass
OK
```

Two modes are supported: the default user authenticates with `auth.requirepass`, and named users authenticate with
the accounts defined in `auth.users`. `HELLO` also accepts an inline `AUTH username password` clause, so protocol
negotiation and authentication can happen in a single round trip. See [Configuration](../config.md) for the
`auth` block parameters.

## Before Cluster Initialization

No connection command requires the cluster to be initialized. `PING`, `HELLO`, and `AUTH` work on a freshly started
instance, which makes them suitable for health checks and bootstrap scripts.

## Commands

| Command                        | Description                                                  |
|--------------------------------|--------------------------------------------------------------|
| [AUTH](commands/auth.md)       | Authenticates the current connection                         |
| [CLIENT](commands/client.md)   | Manages client connection properties                         |
| [COMMAND](commands/command.md) | Returns information about registered server commands         |
| [ECHO](commands/echo.md)       | Echoes back the given message                                |
| [HELLO](commands/hello.md)     | Negotiates the protocol version and optionally authenticates |
| [INFO](commands/info.md)       | Returns server information and statistics                    |
| [PING](commands/ping.md)       | Returns PONG or echoes back the given message                |
