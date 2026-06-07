# kronotop-ctl

Command-line control tool for managing Kronotop clusters.

## Build

```bash
# JAR
./mvnw clean package -pl kronotop-ctl

# Native binary (requires GraalVM)
./mvnw clean package -pl kronotop-ctl -Pnative
```

## Usage

```bash
# JAR
java -jar kronotop-ctl/target/kronotop-ctl-2026.06-1.jar <command> [options]

# Native binary
./kronotop-ctl <command> [options]
```

## Commands

### bootstrap

Bootstraps a new Kronotop cluster. Connects to the first primary node, initializes the cluster,
waits for shard discovery, resolves member IDs, sets routes, and enables shards.

```
kronotop-ctl bootstrap [options]
```

**Options:**

| Option                                         | Description                                                |
|------------------------------------------------|------------------------------------------------------------|
| `--primary <shard-kind> <host:port=shard-ids>` | Assign primary shards. Repeatable, accepts multiple pairs. |
| `--standby <shard-kind> <host:port=shard-ids>` | Assign standby shards. Repeatable, accepts multiple pairs. |
| `--shard-discovery-timeout <seconds>`          | Max seconds to wait for shard discovery. Default: 30       |

**Shard kinds:** `BUCKET`, `STASH`

**Examples:**

Single-node cluster with BUCKET and STASH shards:

```bash
kronotop-ctl bootstrap \
  --primary BUCKET localhost:3320=0,1,2 STASH localhost:3320=0,1,2
```

Multi-node cluster with primary and standby assignments:

```bash
kronotop-ctl bootstrap \
  --primary BUCKET node1:3320=0,1,2 STASH node1:3320=0,1,2 \
  --standby BUCKET node2:3320=0,1,2 STASH node2:3320=0,1,2
```

**Bootstrap steps:**

1. Initialize the cluster via `KR.ADMIN INITIALIZE-CLUSTER`
2. Wait for shard discovery (polls `KR.ADMIN DESCRIBE-CLUSTER`)
3. Resolve member IDs via `KR.ADMIN DESCRIBE-MEMBER` on each unique node
4. Set routes via `KR.ADMIN ROUTE SET PRIMARY|STANDBY`
5. Enable shards via `KR.ADMIN SET-SHARD-STATUS READWRITE`
