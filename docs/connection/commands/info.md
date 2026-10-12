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

| Argument  | Type   | Required | Description                                                                                                                                                                                                                             |
|-----------|--------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `section` | string | No       | One or more of `server`, `cluster`, `kronotop`, `clients`, `memory`, `threads`, `traffic`, `transactions`, `tasks`, `volume`, `bucket`, `vector`, `zmap`. `all`, `default` and `everything` select every section. Case does not matter. |

## Return Value

A verbatim string (`txt`) in RESP3, a bulk string in RESP2. It holds `key:value` lines grouped under `# Section`
headers. Sections are separated by an empty line.

These rules apply to all sections:

- Each byte field is followed by a `_human` field that repeats the value in a readable unit.
- Counters are totals since the member started. They begin at zero on restart.
- A value of `-1` means the JVM does not define it. The `_human` pair then says `unlimited`.

### Server

| Field                   | Description                                                                                                                                                                                                  |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `server_name`           | Always `kronotop`.                                                                                                                                                                                           |
| `kronotop_version`      | Version set at build time. `unknown` when not available.                                                                                                                                                     |
| `kronotop_git_sha1`     | Git commit set at build time. `unknown` when not available.                                                                                                                                                  |
| `kronotop_build_time`   | When the binary was built. `unknown` when not available.                                                                                                                                                     |
| `server_mode`           | Always `standalone`.                                                                                                                                                                                         |
| `os`                    | Operating system name, version and architecture.                                                                                                                                                             |
| `arch_bits`             | Pointer size of the JVM, `64` or `32`.                                                                                                                                                                       |
| `java_version`          | Version of the running JVM.                                                                                                                                                                                  |
| `process_id`            | Operating system process id.                                                                                                                                                                                 |
| `run_id`                | Changes on every restart of the process.                                                                                                                                                                     |
| `tcp_port`              | Port the client listener is bound to. When the config sets port `0`, this is the port picked at startup.                                                                                                     |
| `server_time_usec`      | Server clock in microseconds since the Unix epoch.                                                                                                                                                           |
| `uptime_in_seconds`     | Seconds since the JVM started.                                                                                                                                                                               |
| `process_cpu_time_msec` | CPU time used by the process, in milliseconds.                                                                                                                                                               |
| `fdb_api_version`       | FoundationDB API version from the config.                                                                                                                                                                    |
| `listener0`             | Client listener. The line has the bind host, the bound port and one `advertise=host:port` entry per advertised address. When no advertise address is configured, the entry is derived from the bind address. |
| `listener1`             | Internal cluster listener. Same format as `listener0`.                                                                                                                                                       |

### Cluster

| Field             | Description                                                                                                                                 |
|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `cluster_enabled` | Always `0`.                                                                                                                                 |

### Kronotop

| Field            | Description                                                                 |
|------------------|-----------------------------------------------------------------------------|
| `cluster_name`   | Name of the cluster this member belongs to.                                 |
| `member_id`      | Id of this member.                                                          |
| `member_status`  | One of `RUNNING`, `UNAVAILABLE`, `STOPPED`, `UNKNOWN`.                      |
| `bucket_shards`  | Number of Bucket shards in the cluster.                                     |
| `stash_shards`   | Number of Stash shards. Present only when `stash.enabled` is true.          |
| `known_members`  | Members this member has ever seen.                                          |
| `alive_members`  | Members with a recent heartbeat.                                            |
| `primary_shards` | Shards where this member is the primary.                                    |
| `standby_shards` | Shards where this member is a standby.                                      |

### Clients

`resp2_clients` and `resp3_clients` together equal `connected_clients`.

| Field                    | Description                                                                                  |
|--------------------------|----------------------------------------------------------------------------------------------|
| `connected_clients`      | All open connections on both listeners, client and internal.                                 |
| `clients_in_transaction` | Sessions between `BEGIN` and `COMMIT` or `ROLLBACK`.                                         |
| `clients_in_multi`       | Sessions between `MULTI` and `EXEC` or `DISCARD`. Present only when `stash.enabled` is true. |
| `snapshot_read_clients`  | Sessions with `SNAPSHOTREAD ON`.                                                             |
| `resp2_clients`          | Connections that negotiated RESP2.                                                           |
| `resp3_clients`          | Connections that negotiated RESP3.                                                           |

### Memory

`heap_*` fields cover the JVM heap. `non_heap_*` fields cover JVM memory outside the heap, such as class metadata and
compiled code. Direct buffers are not included in either. All byte fields are in bytes.

| Field                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|--------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `heap_used_memory`             | Heap memory in use.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `heap_committed_memory`        | Heap memory reserved from the operating system.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `heap_max_memory`              | Heap limit.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `non_heap_used_memory`         | Non-heap memory in use.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `non_heap_committed_memory`    | Non-heap memory reserved from the operating system.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `non_heap_max_memory`          | Non-heap limit. Usually `-1`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `direct_buffer_count`          | Number of direct buffers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `direct_buffer_used_memory`    | Memory the JVM uses for direct buffers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `direct_buffer_total_capacity` | Total size of the direct buffers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `mapped_buffer_count`          | Number of mapped volume segments.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `mapped_buffer_total_capacity` | Mapped range of the memory-mapped files, not the part loaded in RAM.                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `netty_used_direct_memory`     | Direct memory held by the network buffer pool. This is the size of the pooled memory blocks, not only the bytes in use by open requests. Depending on the JVM options, it is also counted in `direct_buffer_*`.                                                                                                                                                                                                                                                                                                    |
| `netty_used_heap_memory`       | Heap memory held by the network buffer pool. Same rule as `netty_used_direct_memory`.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `total_allocated_memory`       | Total heap memory allocated since startup. It only grows. Read it twice and divide the difference by the time between the reads to get the allocation rate. `-1` when the JVM does not measure it, missing when the JVM cannot.                                                                                                                                                                                                                                                                                    |
| `gc_count`                     | Number of garbage collections since startup.                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `gc_time_msec`                 | Time spent in garbage collection since startup, in milliseconds.                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `gc_freed_memory`              | Total memory freed by garbage collection. Counting begins when the member starts. Collections that ran before that moment are counted in `gc_count` and `gc_time_msec`, but not here. It covers the heap, the class metadata areas and the compiled code area. Objects moved from one memory area to another are not counted as freed. It can be slightly higher than `total_allocated_memory`, because it also counts unused space the JVM leaves in memory blocks, freed class metadata and freed compiled code. |

### Threads

Only platform threads are counted. Virtual threads are not included.

| Field                   | Description                                           |
|-------------------------|-------------------------------------------------------|
| `thread_count`          | Live threads, daemon and non-daemon.                  |
| `daemon_thread_count`   | Live daemon threads.                                  |
| `peak_thread_count`     | Highest `thread_count` seen since the JVM started.    |
| `total_started_threads` | Threads started since the JVM started. It only grows. |

### Traffic

Each field appears twice: once with the `external_` prefix for the client port, once with the `internal_` prefix for
the cluster port.

| Field                        | Description                                                                                       |
|------------------------------|---------------------------------------------------------------------------------------------------|
| `*_total_commands_processed` | Commands received on that port.                                                                   |
| `*_commands_succeeded`       | Commands answered without an error.                                                               |
| `*_commands_failed`          | Commands answered with an error.                                                                  |
| `*_read_bytes`               | Bytes read from the connections of that port. With TLS enabled, the encrypted bytes are counted.  |
| `*_written_bytes`            | Bytes written to the connections of that port. With TLS enabled, the encrypted bytes are counted. |

### Transactions

| Field                                | Description                                                                                                                   |
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `transactions_created`               | Transactions created since the member started.                                                                                |
| `transactions_active`                | Open transactions a client or a task is working in. Watch transactions are not included.                                      |
| `transactions_watching`              | Open transactions that hold a watch and stay open until an event arrives.                                                     |
| `oldest_active_transaction_usec`     | Age of the oldest active transaction in microseconds. `0` when no transaction is active. Watch transactions are not included. |
| `transactions_committed`             | Commits that succeeded.                                                                                                       |
| `transactions_conflicted`            | Commits rejected by FoundationDB because of a conflict.                                                                       |
| `transactions_commit_failed`         | Commits that failed for another reason.                                                                                       |
| `transactions_rolled_back`           | Transactions closed with `ROLLBACK`.                                                                                          |
| `transactions_closed_without_commit` | Transactions closed without a commit attempt or a rollback, for example read-only sessions or a closed connection.            |
| `transaction_reads`                  | Single key reads.                                                                                                             |
| `transaction_range_reads`            | Range reads.                                                                                                                  |
| `transaction_writes`                 | Single key writes.                                                                                                            |
| `transaction_deletes`                | Single key deletes.                                                                                                           |
| `transaction_range_deletes`          | Range deletes.                                                                                                                |
| `transaction_mutations`              | Atomic mutations.                                                                                                             |
| `transaction_bytes_read`             | Bytes read from FoundationDB.                                                                                                 |
| `transaction_bytes_written`          | Bytes written to FoundationDB.                                                                                                |
| `avg_commit_latency_usec`            | Commit latency averaged over all commit attempts, in microseconds.                                                            |
| `max_commit_latency_usec`            | Highest commit latency seen, in microseconds.                                                                                 |
| `avg_transaction_duration_usec`      | Average lifetime of a closed transaction, from create to close, in microseconds.                                              |

### Tasks

| Field           | Description                                                     |
|-----------------|-----------------------------------------------------------------|
| `task_count`    | Background tasks registered on this member.                     |
| `running_tasks` | Tasks that were running when `INFO` was called.                 |

### Volume

| Field          | Description                                                                                                                                                                                                                                                                                                                                                                       |
|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `volume_count` | Open volumes on this member.                                                                                                                                                                                                                                                                                                                                                      |
| `volumeN`      | One line per open volume, in name order, starting at `volume0`. The line has the volume name, its status (`READWRITE`, `READONLY` or `INOPERABLE`), `vacuum_active` (`1` while a vacuum runs on that volume) and the operation counters `appends`, `deletes`, `updates`, `gets`, `bytes_appended`, `bytes_read` and `segments_created`. `VOLUME.STATS RESET` clears the counters. |

### Bucket

| Field                                 | Description                                                                                              |
|---------------------------------------|----------------------------------------------------------------------------------------------------------|
| `plan_cache_size`                     | Number of cached query plans.                                                                            |
| `plan_cache_hits`                     | Query plan lookups served from the cache. `EXPLAIN` lookups are not counted.                             |
| `plan_cache_misses`                   | Query plan lookups that missed the cache. An expired entry is a miss. `EXPLAIN` lookups are not counted. |
| `open_cursors`                        | Open `BUCKET.QUERY`, `BUCKET.DELETE` and `BUCKET.UPDATE` cursors across all sessions.                    |
| `bucket_inserts`                      | `BUCKET.INSERT` calls. One call is one count, no matter how many documents it touched.                   |
| `bucket_queries`                      | `BUCKET.QUERY` calls.                                                                                    |
| `bucket_updates`                      | `BUCKET.UPDATE` calls.                                                                                   |
| `bucket_deletes`                      | `BUCKET.DELETE` calls.                                                                                   |
| `bucket_advances`                     | `BUCKET.ADVANCE` calls.                                                                                  |
| `bucket_vector_searches`              | `BUCKET.VECTOR` calls.                                                                                   |
| `bucket_purges`                       | `BUCKET.PURGE` calls.                                                                                    |
| `bucket_removes`                      | `BUCKET.REMOVE` calls.                                                                                   |
| `index_maintenance_workers`           | Index maintenance workers running on this member.                                                        |
| `index_maintenance_processed_entries` | Entries processed by index maintenance workers on this member since start.                               |
| `index_maintenance_retried_conflicts` | Commit conflicts index maintenance workers retried since start.                                          |
| `index_maintenance_last_run`          | Most recent run time in milliseconds since the Unix epoch, or `0` when no worker is running.             |

### Vector

| Field                    | Description                                                                                  |
|--------------------------|----------------------------------------------------------------------------------------------|
| `vector_indexes`         | Vector graph indexes held in memory.                                                         |
| `vector_bytes_used`      | Total heap usage of those indexes, in bytes.                                                 |
| `vector_pending_retries` | Failed vector node adds and deletes waiting for the background retry.                        |
| `vector_retried_ops`     | Operations the retry replayed successfully.                                                  |

### ZMap

| Field                | Description                                                                             |
|----------------------|-----------------------------------------------------------------------------------------|
| `zmap_sets`          | `ZSET` and the typed `ZSET*` calls. One call is one count.                              |
| `zmap_gets`          | `ZGET`, `ZGETKEY` and the typed `ZGET*` calls.                                          |
| `zmap_range_reads`   | `ZGETRANGE` and `ZGETRANGESIZE` calls.                                                  |
| `zmap_deletes`       | `ZDEL` and `ZDELRANGE` calls.                                                           |
| `zmap_mutations`     | `ZMUTATE` and the `ZINC*` calls.                                                        |
| `zmap_watches`       | `ZWATCH` calls.                                                                         |
| `zmap_watched_keys`  | Distinct keys with an active `ZWATCH`, which equals the number of FoundationDB watches. |
| `zmap_watch_waiters` | Clients blocked on those watches.                                                       |
| `zmap_watch_fires`   | Watches that fired. One fire wakes every waiter on the key and counts once.             |

## Behavior

Without arguments, all sections are returned. With one or more section names, only the named sections are returned,
in the order above. Unknown names are ignored. If no name matches, the reply is an empty string.

The command reads only in-memory state. It does not touch FoundationDB and does not require the cluster to be
initialized.

`server_mode` is always `standalone` and `cluster_enabled` is always `0`. Kronotop does not use slot-based routing at the
protocol level, so clients must connect in standalone mode.

## Errors

No command-specific errors.