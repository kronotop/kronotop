---
title: "Configuration Reference"
description: "Kronotop uses HOCON (Human-Optimized Config Object Notation) for its configuration files."
---

Kronotop uses [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md) (Human-Optimized Config Object Notation)
for its configuration files. HOCON is a superset of
JSON that supports comments, variable substitution, and a more readable syntax.

The built-in defaults ship in `reference.conf` inside the Kronotop JAR. To override any value, pass a custom
configuration file at startup:

```bash
java -Dconfig.file=/etc/kronotop/kronotop.conf -jar kronotop.jar
```

Only the values you want to change need to appear in your override file; everything else falls back to the built-in
defaults.

## Overriding Individual Parameters

Any configuration parameter can be overridden directly on the command line using Java system properties (`-D`). The
property
name matches the dotted config path:

```bash
java -Dnetwork.external.port=6000 -Dcluster.name=production -jar kronotop.jar
```

This is useful for per-member overrides in a multi-node cluster where each member needs a different bind address or port
without maintaining separate config files:

```bash
# Node 1
java -Dnetwork.external.host=10.0.0.1 -Dnetwork.external.port=5484 \
     -Dnetwork.internal.host=10.0.0.1 -Dnetwork.internal.port=3320 \
     -Ddata_dir=/data/node1 -jar kronotop.jar

# Node 2
java -Dnetwork.external.host=10.0.0.2 -Dnetwork.external.port=5484 \
     -Dnetwork.internal.host=10.0.0.2 -Dnetwork.internal.port=3320 \
     -Ddata_dir=/data/node2 -jar kronotop.jar
```

`-D` overrides take precedence over both `reference.conf` defaults and any config file supplied via `-Dconfig.file`. You
can combine both approaches: use a shared config file for cluster-wide settings and `-D` flags for member-specific
values.

---

## General

| Parameter           | Type   | Default           | Description                                                                                                                                     |
|---------------------|--------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| `default_namespace` | string | `"global"`        | Logical namespace used to isolate tenants or environments within a single cluster. All data structures are scoped under this namespace.         |
| `data_dir`          | string | `"kronotop-data"` | Directory where Kronotop stores local data (volumes, segments). Resolved relative to the working directory. Use an absolute path in production. |

---

## Cluster

Controls cluster membership, failure detection, and inter-node communication.

| Parameter                                 | Type          | Default         | Description                                                                                                                                                                           |
|-------------------------------------------|---------------|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cluster.name`                            | string        | `"development"` | Human-readable cluster identifier. All nodes that should form a cluster must share the same name.                                                                                     |
| `cluster.heartbeat.interval`              | int (seconds) | `5`             | How often each node sends a heartbeat to signal liveness.                                                                                                                             |
| `cluster.heartbeat.maximum_silent_period` | int (seconds) | `20`            | If no heartbeat is received from a node within this window, the node is considered unreachable. Should be at least 3-4x the heartbeat interval to tolerate transient network hiccups. |
| `cluster.client_pool.idle_timeout`        | int (minutes) | `10`            | How long an idle inter-node client connection is kept open before being closed.                                                                                                       |

---

## Session Defaults

Default values applied to every new client session. Clients can override these per-session.

| Parameter                       | Type   | Default  | Description                                                                                      |
|---------------------------------|--------|----------|--------------------------------------------------------------------------------------------------|
| `session_attributes.input_type` | string | `"bson"` | Default encoding for incoming documents. Accepted values: `"bson"`, `"json"`.                    |
| `session_attributes.reply_type` | string | `"bson"` | Default encoding for query results returned to clients. Accepted values: `"bson"`, `"json"`.     |
| `session_attributes.limit`      | int    | `100`    | Default maximum number of documents returned per query when the client does not specify a LIMIT. |

---

## Network

Kronotop exposes two network interfaces:

- **external**: Client-facing port (default `5484`). All application traffic uses this.
- **internal**: Inter-node and admin port (default `3320`). Used for replication, heartbeats, and cluster
  administration.

Both interfaces share the same configuration structure.

### External Interface

| Parameter                               | Type    | Default       | Description                                                                                                                 |
|-----------------------------------------|---------|---------------|-----------------------------------------------------------------------------------------------------------------------------|
| `network.external.host`                 | string  | `"127.0.0.1"` | Bind address for the client-facing interface. Set to `0.0.0.0` to accept connections on all interfaces.                     |
| `network.external.port`                 | int     | `5484`        | TCP port for client connections.                                                                                            |
| `network.external.netty.transport`      | string  | `"nio"`       | Netty transport type. `"nio"` works on all platforms; `"epoll"` uses Linux kernel-level I/O for lower latency (Linux only). |
| `network.external.netty.worker_threads` | int     | `0`           | Number of Netty worker threads. `0` uses the Netty default (2 x CPU cores).                                                 |
| `network.external.netty.so_backlog`     | int     | `4096`        | TCP listen backlog size. Higher values allow more pending connections during connection bursts.                             |
| `network.external.netty.so_reuseport`   | boolean | `true`        | Enable `SO_REUSEPORT` socket option. Only effective with `epoll` transport on Linux.                                        |

### Internal Interface

| Parameter                               | Type    | Default       | Description                                                   |
|-----------------------------------------|---------|---------------|---------------------------------------------------------------|
| `network.internal.host`                 | string  | `"127.0.0.1"` | Bind address for the internal interface.                      |
| `network.internal.port`                 | int     | `3320`        | TCP port for inter-node communication and admin commands.     |
| `network.internal.netty.transport`      | string  | `"nio"`       | Netty transport type. Same options as the external interface. |
| `network.internal.netty.worker_threads` | int     | `0`           | Number of Netty worker threads.                               |
| `network.internal.netty.so_backlog`     | int     | `4096`        | TCP listen backlog.                                           |
| `network.internal.netty.so_reuseport`   | boolean | `true`        | Enable `SO_REUSEPORT`.                                        |

### TLS (Optional)

TLS is disabled by default on both interfaces. To enable it, uncomment the `tls` block under the appropriate interface
section.

**External TLS:**

```hocon
network.external.tls {
  enabled = true
  cert_path = "/path/to/server-cert.pem"
  key_path = "/path/to/server-key.pem"
}
```

**Internal TLS:**

The internal interface additionally supports a `ca_path` for mutual TLS between cluster nodes:

```hocon
network.internal.tls {
  enabled = true
  cert_path = "/path/to/server-cert.pem"
  key_path = "/path/to/server-key.pem"
  ca_path = "/path/to/ca-cert.pem"
}
```

| Parameter       | Type    | Default | Description                                                                      |
|-----------------|---------|---------|----------------------------------------------------------------------------------|
| `tls.enabled`   | boolean | `false` | Enable TLS on this interface.                                                    |
| `tls.cert_path` | string  | -       | Path to the PEM-encoded server certificate.                                      |
| `tls.key_path`  | string  | -       | Path to the PEM-encoded private key.                                             |
| `tls.ca_path`   | string  | -       | (Internal only) Path to the CA certificate for verifying peer node certificates. |

---

## Authentication (Optional)

Authentication is disabled by default. To enable it, add an `auth` block at the top level:

```hocon
auth {
  requirepass = "your-password"
  users = {
    "alice": "alice-password"
    "bob": "bob-password"
  }
}
```

| Parameter          | Type   | Default | Description                                                                                                            |
|--------------------|--------|---------|------------------------------------------------------------------------------------------------------------------------|
| `auth.requirepass` | string | -       | When set, clients must authenticate with the `AUTH` command using this password before issuing any other commands.     |
| `auth.users`       | object | -       | Named user accounts. Keys are usernames, values are passwords. Clients authenticate with `AUTH <username> <password>`. |

---

## FoundationDB

Kronotop uses FoundationDB as its transactional metadata and index store.

| Parameter                  | Type   | Default | Description                                                                                                                                   |
|----------------------------|--------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `foundationdb.clusterfile` | string | -       | Path to the FoundationDB cluster file. When omitted, the FDB client uses its default search paths (`/etc/foundationdb/fdb.cluster` on Linux). |
| `foundationdb.fdbc`        | string | -       | Path to the native FDB C client library (`libfdb_c.so` / `libfdb_c.dylib`). Only needed if the library is not on the default library path.    |
| `foundationdb.fdbjava`     | string | -       | Path to the FDB Java JNI library (`libfdb_java.so` / `libfdb_java.jnilib`). Only needed if the library is not on the default library path.    |
| `foundationdb.apiversion`  | int    | `630`   | FoundationDB API version to use. Must match the FDB client library version installed on the system.                                           |

### FoundationDB Network Options (Optional)

These settings configure the FoundationDB client's network layer. All are optional; when omitted, the FDB client
defaults to apply.

**Trace logging:**

Enables FDB client-side trace logs, useful for debugging connectivity and performance issues.

```hocon
foundationdb.network_options.trace {
  enable = "/var/log/kronotop/fdb-trace"
  roll_size = 10485760
  max_logs_size = 104857600
  log_group = "default"
  format = "json"
  file_identifier = ""
}
```

| Parameter                                            | Type        | Default               | Description                                                                                     |
|------------------------------------------------------|-------------|-----------------------|-------------------------------------------------------------------------------------------------|
| `foundationdb.network_options.trace.enable`          | string      | -                     | Directory path for FDB trace log files. Setting this value enables trace logging.               |
| `foundationdb.network_options.trace.roll_size`       | int (bytes) | `10485760` (10 MiB)   | Maximum size of a single trace log file before rotation.                                        |
| `foundationdb.network_options.trace.max_logs_size`   | int (bytes) | `104857600` (100 MiB) | Maximum total size of all trace log files. Oldest files are deleted when this limit is reached. |
| `foundationdb.network_options.trace.log_group`       | string      | `"default"`           | Label applied to trace log entries to identify this cluster or service.                         |
| `foundationdb.network_options.trace.format`          | string      | `"json"`              | Trace log format. `"json"` or `"xml"`.                                                          |
| `foundationdb.network_options.trace.file_identifier` | string      | `""`                  | Optional string appended to trace file names for identification.                                |

**FDB Client TLS:**

Configures TLS for the connection between Kronotop and the FoundationDB cluster (separate from Kronotop's own network
TLS).

| Parameter                                       | Type   | Default           | Description                                                                                                                  |
|-------------------------------------------------|--------|-------------------|------------------------------------------------------------------------------------------------------------------------------|
| `foundationdb.network_options.tls.cert_path`    | string | -                 | Path to the TLS certificate for the FDB client connection.                                                                   |
| `foundationdb.network_options.tls.key_path`     | string | -                 | Path to the TLS private key.                                                                                                 |
| `foundationdb.network_options.tls.ca_path`      | string | -                 | Path to the CA bundle for verifying the FDB cluster's certificate.                                                           |
| `foundationdb.network_options.tls.password`     | string | `""`              | Password for the private key, if encrypted.                                                                                  |
| `foundationdb.network_options.tls.verify_peers` | string | `"Check.Valid=1"` | Peer verification rules. See the [FoundationDB TLS documentation](https://apple.github.io/foundationdb/tls.html) for syntax. |

**FDB Client miscellaneous:**

| Parameter                                                        | Type    | Default  | Description                                                         |
|------------------------------------------------------------------|---------|----------|---------------------------------------------------------------------|
| `foundationdb.network_options.client.tmp_dir`                    | string  | `"/tmp"` | Temporary directory used by the FDB client for internal operations. |
| `foundationdb.network_options.client.disable_statistics_logging` | boolean | `false`  | When `true`, disables the FDB client's periodic statistics logging. |
| `foundationdb.network_options.client.distributed_tracer`         | string  | `"none"` | Distributed tracing backend. `"none"` disables tracing.             |

---

## Volume

Global tuning parameters for the volume storage engine. These apply to all volume instances (bucket, stash, etc.).

### Vacuum

Controls the background vacuum process that reclaims space from stale volume segments.

| Parameter                   | Type | Default | Description                                                                                              |
|-----------------------------|------|---------|----------------------------------------------------------------------------------------------------------|
| `volume.vacuum.max_workers` | int  | `1`     | Maximum number of concurrent vacuum worker threads. Set to `0` to use the number of available CPU cores. |

### Replication

| Parameter                              | Type          | Default | Description                                                                                                                                                                                                                           |
|----------------------------------------|---------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `volume.replication.max_retries`       | int           | `10`    | Maximum number of retry attempts for a failed replication operation before giving up.                                                                                                                                                 |
| `volume.replication.retry_interval`    | int (seconds) | `10`    | Delay between replication retry attempts.                                                                                                                                                                                             |
| `volume.replication.reset_threshold`   | int (seconds) | `10`    | Time after which a stalled replication stream is reset and restarted from the last known good position.                                                                                                                               |
| `volume.replication.reconnect_backoff` | int (ms)      | `250`   | Delay a standby waits before retrying a replication stage that was interrupted because it had not connected to the primary yet, such as during a topology change. Prevents a tight retry loop while the connection is re-established. |

---

## Bucket

Configuration for the document database engine.

| Parameter                 | Type   | Default   | Description                                                                                                                                                 |
|---------------------------|--------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bucket.shards`           | int    | `1`       | Number of shards for bucket data. More shards allow parallel writes across cluster nodes but increase coordination overhead.                                |
| `bucket.object_id_format` | string | `"bytes"` | Controls how ObjectIds are encoded in DELETE and UPDATE responses. `"bytes"` returns raw 12-byte ObjectId values; `"hex"` returns 24-character hex strings. |

### Plan Cache

The query plan cache avoids re-planning identical queries.

| Parameter                   | Type               | Default          | Description                                                                                            |
|-----------------------------|--------------------|------------------|--------------------------------------------------------------------------------------------------------|
| `bucket.plan_cache.enabled` | boolean            | `true`           | Enable or disable the query plan cache. Disabling forces re-planning on every query.                   |
| `bucket.plan_cache.max_ttl` | int (milliseconds) | `300000` (5 min) | Time-to-live for cached query plans. Plans are evicted after this duration and re-planned on next use. |

### Bucket Volume

Controls the local storage segments used by bucket volumes.

| Parameter                                      | Type         | Default              | Description                                                                                                                              |
|------------------------------------------------|--------------|----------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| `bucket.volume.segment_size`                   | long (bytes) | `4294967296` (4 GiB) | Maximum size of a single volume segment file. When a segment reaches this size, a new segment is created.                                |
| `bucket.volume.segment_replication_chunk_size` | long (bytes) | `16777216` (16 MiB)  | Chunk size used when replicating segment data to standby nodes. Larger chunks reduce round trips; smaller chunks reduce memory pressure. |

### Index

Configuration for secondary indexes.

| Parameter                   | Type    | Default | Description                                                                                                                                                                                                                                             |
|-----------------------------|---------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bucket.index.strict_types` | boolean | `true`  | Enables strict type enforcement for secondary indexes. When enabled, queries with mismatched predicate types do not use the index, and index maintenance skips values with incompatible types. Prevents mixed-type values under the same indexed field. |

### Index Maintenance

Controls the background subsystem responsible for index build and cleanup tasks.

| Parameter                                              | Type          | Default | Description                                                                                                                       |
|--------------------------------------------------------|---------------|---------|-----------------------------------------------------------------------------------------------------------------------------------|
| `bucket.index.maintenance.worker_pool_size`            | int           | `0`     | Number of worker threads for index maintenance. `0` auto-selects based on available CPU cores.                                    |
| `bucket.index.maintenance.worker_maintenance_interval` | int (seconds) | `60`    | Interval at which the maintenance scheduler runs periodic checks such as cleaning up stale workers and dispatching pending tasks. |

### Vector

Configuration for the vector index subsystem.

| Parameter                             | Type         | Default               | Description                                                                                                                                                                                                                  |
|---------------------------------------|--------------|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bucket.vector.flush_threshold_bytes` | long (bytes) | `268435456` (256 MiB) | RAM threshold for flushing an on-heap vector index to disk. When an index exceeds this limit after an insert, it is rotated and flushed asynchronously.                                                                      |
| `bucket.vector.pq_training_threshold` | int          | `1000`                | Minimum number of vectors before Product Quantization training kicks in. Until this threshold is reached, exact scoring is used for graph construction.                                                                      |
| `bucket.vector.pq_subspace_divisor`   | int          | `6`                   | Controls the number of PQ subspaces: `subspaces = dimensions / pq_subspace_divisor`.                                                                                                                                         |
| `bucket.vector.max_scan_candidates`   | int          | `10000`               | Safety cap for filtered vector search. Limits the total number of unique candidates examined during post-filter graph traversal. Once reached, the search stops expanding even if fewer than TOP results matched the filter. |
| `bucket.vector.default_overquery`     | float        | `1.0`                 | Overquery multiplier for vector search. Controls how many extra candidates the graph traversal collects before reranking to the final top-K. Higher values improve recall for PQ-scored indexes at the cost of latency.      |

---

## Stash (Optional)

The stash is an experimental key-value store with String and Hash data types. It persists data by syncing it to the
volume storage engine. The stash is disabled by default; set `stash.enabled = true` to opt in.

| Parameter                                     | Type         | Default              | Description                                         |
|-----------------------------------------------|--------------|----------------------|-----------------------------------------------------|
| `stash.enabled`                               | boolean      | `false`              | Enable or disable the stash subsystem.              |
| `stash.shards`                                | int          | `1`                  | Number of shards for stash data.                    |
| `stash.volume.segment_size`                   | long (bytes) | `4294967296` (4 GiB) | Maximum size of a single stash volume segment file. |
| `stash.volume.segment_replication_chunk_size` | long (bytes) | `16777216` (16 MiB)  | Chunk size for stash segment replication.           |

### Stash Volume Syncer

The volume syncer runs background workers that keep stash volumes synchronized.

| Parameter                     | Type               | Default                 | Description                                                                            |
|-------------------------------|--------------------|-------------------------|----------------------------------------------------------------------------------------|
| `stash.volume_syncer.prefix`  | string             | `"stash-volume-syncer"` | Prefix used to name volume syncer threads for identification in logs and thread dumps. |
| `stash.volume_syncer.workers` | int                | `8`                     | Number of concurrent syncer worker threads.                                            |
| `stash.volume_syncer.period`  | int (milliseconds) | `1000`                  | How often each syncer worker checks for pending sync work.                             |

---

## Background Tasks

Configuration for system-wide background maintenance tasks.

### Journal Cleanup

The journal cleanup task removes old journal entries to reclaim FoundationDB storage.

| Parameter                                                | Type   | Default  | Description                                                                                                                    |
|----------------------------------------------------------|--------|----------|--------------------------------------------------------------------------------------------------------------------------------|
| `background_tasks.journal_cleanup_task.retention_period` | int    | `1`      | How long journal entries are retained before cleanup. Interpreted together with `timeunit`.                                    |
| `background_tasks.journal_cleanup_task.timeunit`         | string | `"days"` | Time unit for `retention_period`. Accepted values correspond to Java's `TimeUnit` enum: `"days"`, `"hours"`, `"minutes"`, etc. |
