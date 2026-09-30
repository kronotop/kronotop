---
title: "INFO"
description: "Returns server information and statistics."
---

Returns server information and statistics.

## Syntax

```kronotop
INFO [section ...]
```

## Arguments

| Argument  | Type   | Required | Description                                                    |
|-----------|--------|----------|----------------------------------------------------------------|
| `section` | string | No       | One or more section names to return. Case does not matter.     |

## Return Value

Bulk string containing server information formatted as `key:value` pairs grouped under `# Section` headers. Sections
are separated by an empty line.

| Section    | Fields                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Server`   | `server_name`, `kronotop_version`, `kronotop_git_sha1`, `kronotop_build_time`, `server_mode`, `os`, `arch_bits`, `java_version`, `process_id`, `run_id`, `tcp_port`, `server_time_usec`, `fdb_api_version`, `listener0`, `listener1`                                                                                                                                                                                                                                                                                                                                                                                         |
| `Cluster`  | `cluster_enabled`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `Kronotop` | `cluster_name`, `member_id`, `member_status`, `bucket_shards`, `stash_shards`, `known_members`, `alive_members`, `primary_shards`, `standby_shards`                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `Clients`  | `connected_clients`, `clients_in_transaction`, `clients_in_multi`, `snapshot_read_clients`, `resp2_clients`, `resp3_clients`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `Memory`   | `heap_used_memory`, `heap_committed_memory`, `heap_max_memory`, `non_heap_used_memory`, `non_heap_committed_memory`, `non_heap_max_memory`, `direct_buffer_count`, `direct_buffer_used_memory`, `direct_buffer_total_capacity`, `mapped_buffer_count`, `mapped_buffer_used_memory`, `mapped_buffer_total_capacity`, `netty_used_direct_memory`, `netty_used_heap_memory`, `total_allocated_memory`, `gc_freed_memory`, each byte field with a `_human` pair, `gc_count`, `gc_time_msec`                                                                                                                                      |
| `Traffic`  | `external_total_commands_processed`, `external_read_bytes`, `external_written_bytes`, `internal_total_commands_processed`, `internal_read_bytes`, `internal_written_bytes`, each byte field with a `_human` pair                                                                                                                                                                                                                                                                                                                                                                                                             |
| `Tasks`    | `task_count`, `running_tasks`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `Volume`   | `volume_count`, `volume0`, `volume1`, ...                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `Bucket`   | `plan_cache_size`, `index_maintenance_workers`, `index_maintenance_processed_entries`, `index_maintenance_retried_conflicts`, `index_maintenance_last_run`                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `Vector`   | `vector_indexes`, `vector_bytes_used`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |

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
- `heap_*` fields cover the JVM heap. `non_heap_*` fields cover JVM memory outside the heap, such as class metadata
  and compiled code. Direct buffers are not included.
- `*_used_memory` is the memory in use, `*_committed_memory` the memory reserved from the operating system and
  `*_max_memory` the limit. All are in bytes. A value of `-1` means the JVM does not define it.
  `non_heap_max_memory` is usually `-1`.
- `direct_buffer_*` fields cover direct buffers. `mapped_buffer_*` fields cover memory-mapped files, such as volume
  segments. `*_count` is the number of buffers, `*_total_capacity` their total size and `*_used_memory` the memory
  the JVM uses for them, in bytes. For mapped files, the size is the mapped range, not the part loaded in RAM.
- `netty_used_direct_memory` and `netty_used_heap_memory` are the direct and heap memory held by the network buffer
  pool, in bytes. This is the size of the pooled memory blocks, not only the bytes in use by open requests.
  Depending on the JVM options, `netty_used_direct_memory` is also counted in `direct_buffer_*`.
- `total_allocated_memory` is the total heap memory allocated since startup, in bytes. It only grows. Read it twice
  and divide the difference by the time between the reads to get the allocation rate. It is `-1` when the JVM does not
  measure it. The field is missing when the JVM does not support this measurement.
- Each `_human` field repeats the value before it in a readable unit.
- `gc_count` and `gc_time_msec` are totals since startup.
- `gc_freed_memory` is the total memory freed by garbage collection, in bytes. Counting begins when the member
  starts. Collections that ran before that moment are counted in `gc_count` and `gc_time_msec`, but not here. It
  covers the heap, the class metadata areas and the compiled code area. Objects moved from one memory area to
  another are not counted as freed. It can be slightly higher than `total_allocated_memory`, because it also counts
  unused space the JVM leaves in memory blocks, freed class metadata and freed compiled code.
- `*_total_commands_processed` is the number of commands received since the member started, including the ones
  answered with an error. Fields with the `external_` prefix cover the client port, fields with the `internal_`
  prefix cover the cluster port.
- `*_read_bytes` and `*_written_bytes` are the total bytes read from and written to the connections of that port.
  With TLS enabled, the encrypted bytes are counted. The counters start at zero on restart.
- `task_count` counts the background tasks registered on this member. `running_tasks` counts the ones executing at
  the moment of the call.
- `volume_count` counts the open volumes on this member. Each `volumeN` line has the volume name, its status
  (`READWRITE`, `READONLY` or `INOPERABLE`), `vacuum_active` (`1` while a vacuum runs on that volume) and the
  operation counters `appends`, `deletes`, `updates`, `gets`, `bytes_appended`, `bytes_read` and `segments_created`.
  The counters start at zero on restart and `VOLUME.STATS RESET` clears them. Volumes are listed in name order.
- `plan_cache_size` is the number of cached query plans.
- `index_maintenance_workers` counts the index maintenance workers running on this member. The
  `index_maintenance_processed_entries` and `index_maintenance_retried_conflicts` totals cover those workers.
  `index_maintenance_last_run` is the most recent run time in milliseconds since the Unix epoch, or `0` when no worker
  is running.
- `vector_indexes` counts the vector graph indexes held in memory. `vector_bytes_used` is their total heap usage in
  bytes.

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
kronotop_version:2026.09-1
kronotop_git_sha1:dd188c4
kronotop_build_time:2026-09-11T22:23:20+03:00
server_mode:standalone
os:Mac OS X 26.6.2 aarch64
arch_bits:64
java_version:26.0.2
process_id:94884
run_id:00006JKCN4LOA0000000xxxx
tcp_port:5484
server_time_usec:1789154622358417
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
heap_used_memory:17106512
heap_used_memory_human:16 MB
heap_committed_memory:125829120
heap_committed_memory_human:120 MB
heap_max_memory:12884901888
heap_max_memory_human:12.0 GB
non_heap_used_memory:45088768
non_heap_used_memory_human:43 MB
non_heap_committed_memory:55574528
non_heap_committed_memory_human:53 MB
non_heap_max_memory:-1
non_heap_max_memory_human:-1 B
direct_buffer_count:32
direct_buffer_used_memory:4194304
direct_buffer_used_memory_human:4 MB
direct_buffer_total_capacity:4194304
direct_buffer_total_capacity_human:4 MB
mapped_buffer_count:1
mapped_buffer_used_memory:67108864
mapped_buffer_used_memory_human:64 MB
mapped_buffer_total_capacity:67108864
mapped_buffer_total_capacity_human:64 MB
netty_used_direct_memory:4194304
netty_used_direct_memory_human:4 MB
netty_used_heap_memory:0
netty_used_heap_memory_human:0 B
total_allocated_memory:2147483648
total_allocated_memory_human:2.0 GB
gc_count:9
gc_time_msec:9
gc_freed_memory:1887436800
gc_freed_memory_human:1.8 GB

# Traffic
external_total_commands_processed:1284
external_read_bytes:196608
external_read_bytes_human:192 KB
external_written_bytes:917504
external_written_bytes_human:896 KB
internal_total_commands_processed:42
internal_read_bytes:8192
internal_read_bytes_human:8 KB
internal_written_bytes:12288
internal_written_bytes_human:12 KB

# Tasks
task_count:4
running_tasks:0

# Volume
volume_count:1
volume0:name=bucket-shard-0,status=READWRITE,vacuum_active=0,appends=120,deletes=3,updates=8,gets=540,bytes_appended=65536,bytes_read=294912,segments_created=1

# Bucket
plan_cache_size:6
index_maintenance_workers:0
index_maintenance_processed_entries:0
index_maintenance_retried_conflicts:0
index_maintenance_last_run:0

# Vector
vector_indexes:1
vector_bytes_used:12288
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
