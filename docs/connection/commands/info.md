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
| `Clients`  | `connected_clients`, `clients_in_transaction`, `clients_in_multi`, `snapshot_read_clients`, `resp2_clients`, `resp3_clients`                                                                                                         |
| `Memory`   | `used_memory`, `used_memory_human`, `committed_memory`, `max_memory`, `max_memory_human`, `gc_count`, `gc_time_msec`                                                                                                                 |

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
- `connected_clients` counts every open connection on both listeners, client and internal.
- `clients_in_transaction` counts sessions between `BEGIN` and `COMMIT` or `ROLLBACK`. `snapshot_read_clients` counts
  sessions with `SNAPSHOTREAD ON`.
- `clients_in_multi` counts sessions between `MULTI` and `EXEC` or `DISCARD`. Present only when `stash.enabled` is
  true.
- `resp2_clients` and `resp3_clients` split `connected_clients` by the negotiated protocol version.
- `used_memory` is the JVM heap in use, `committed_memory` the heap reserved from the operating system and
  `max_memory` the heap limit. All three are in bytes. `gc_count` and `gc_time_msec` are totals since startup.

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

# Clients
connected_clients:3
clients_in_transaction:1
clients_in_multi:0
snapshot_read_clients:0
resp2_clients:2
resp3_clients:1

# Memory
used_memory:17106512
used_memory_human:16 MB
committed_memory:125829120
max_memory:12884901888
max_memory_human:12.0 GB
gc_count:9
gc_time_msec:9
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
