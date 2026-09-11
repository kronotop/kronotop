---
title: "INFO"
description: "Returns server information and statistics."
---

Returns server information and statistics.

## Syntax

```kronotop
INFO [section ...]
```

## Parameters

| Parameter | Type   | Required | Description                                                    |
|-----------|--------|----------|----------------------------------------------------------------|
| `section` | string | No       | One or more section names to return. Case does not matter.     |

## Return Value

Bulk string containing server information formatted as `key:value` pairs grouped under `# Section` headers. Sections
are separated by an empty line.

| Section    | Fields                                                                                                                                                                                                                               |
|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Server`   | `server_name`, `kronotop_version`, `kronotop_git_sha1`, `kronotop_build_time`, `server_mode`, `os`, `arch_bits`, `java_version`, `process_id`, `run_id`, `tcp_port`, `server_time_usec`, `fdb_api_version`, `listener0`, `listener1` |
| `Cluster`  | `cluster_enabled`                                                                                                                                                                                                                    |
| `Kronotop` | `cluster_name`, `member_id`, `member_status`, `bucket_shards`, `stash_shards`, `known_members`, `alive_members`, `primary_shards`, `standby_shards`                                                                                  |

Field notes:

- `kronotop_version`, `kronotop_git_sha1` and `kronotop_build_time` are set at build time. A value that is not
  available is reported as `unknown`.
- `run_id` changes on every restart of the process. `process_id` is the operating system process id.
- `tcp_port` is the port the client listener is bound to. When the config sets port `0`, this is the port picked at
  startup.
- `listener0` is the client listener and `listener1` the internal cluster listener. Each line has the bind host, the
  bound port and one `advertise=host:port` entry per advertised address. When no advertise address is configured, the
  entry is derived from the bind address.
- `server_time_usec` is the server clock in microseconds since the Unix epoch.
- `member_status` is one of `RUNNING`, `UNAVAILABLE`, `STOPPED`, `UNKNOWN`.
- `known_members` counts every member this node has seen. `alive_members` counts the ones with a recent heartbeat.
- `primary_shards` and `standby_shards` count the shards where this member is the primary or a standby.
- `stash_shards` is present only when `stash.enabled` is true.

## Behavior

Without arguments, every section is returned. With one or more section names, only the named sections are returned,
in their normal order. Unknown names are ignored. If no name matches, the reply is an empty bulk string.

The names `all`, `default` and `everything` return every section.

The command reads only in-memory state. It does not touch FoundationDB and does not require the cluster to be
initialized.

`server_mode` is always `standalone` and `cluster_enabled` is always `0`. Kronotop does not use slot-based routing at the
protocol level, so clients must connect in standalone mode.

## Errors

No command-specific errors.

## Examples

```kronotop
127.0.0.1:5484> INFO
# Server
server_name:kronotop
kronotop_version:2026.08-1
kronotop_git_sha1:dd188c4
kronotop_build_time:2026-09-11T22:23:20+03:00
server_mode:standalone
os:Mac OS X 26.6.2 aarch64
arch_bits:64
java_version:26.0.2
process_id:94884
run_id:00006JKCN4LOA0000000xxxx
tcp_port:5484
server_time_usec:1789154622358000
fdb_api_version:630
listener0:name=external,bind=127.0.0.1,port=5484,advertise=localhost:5484
listener1:name=internal,bind=127.0.0.1,port=3320,advertise=localhost:3320

# Cluster
cluster_enabled:0

# Kronotop
cluster_name:development
member_id:1ceb8d2debb1caa2e6acfbd1052afe9e76079b2f
member_status:RUNNING
bucket_shards:1
known_members:1
alive_members:1
primary_shards:1
standby_shards:0
```

```kronotop
127.0.0.1:5484> INFO kronotop
# Kronotop
cluster_name:development
member_id:1ceb8d2debb1caa2e6acfbd1052afe9e76079b2f
member_status:RUNNING
bucket_shards:1
known_members:1
alive_members:1
primary_shards:1
standby_shards:0
```
