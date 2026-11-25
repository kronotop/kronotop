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
java -jar kronotop-ctl/target/kronotop-ctl-2026.11-2.jar <command> [options]

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

| Option                                | Description                                                 |
|---------------------------------------|-------------------------------------------------------------|
| `--primary <host:port=shard-ids>`     | Assign primary shards. Repeatable, accepts multiple values. |
| `--standby <host:port=shard-ids>`     | Assign standby shards. Repeatable, accepts multiple values. |
| `--shards <count>`                    | Number of shards to create. Default: 1                      |
| `--shard-discovery-timeout <seconds>` | Max seconds to wait for shard discovery. Default: 30        |

Shard ids start at 0 and must be lower than the shard count.

**Examples:**

Single-node cluster:

```bash
kronotop-ctl bootstrap \
  --shards 3 \
  --primary localhost:3320=0,1,2
```

Multi-node cluster with primary and standby assignments:

```bash
kronotop-ctl bootstrap \
  --shards 3 \
  --primary node1:3320=0,1,2 \
  --standby node2:3320=0,1,2
```

**Bootstrap steps:**

1. Initialize the cluster via `KR.ADMIN INITIALIZE-CLUSTER SHARDS <count>`
2. Wait for shard discovery (polls `KR.ADMIN DESCRIBE-CLUSTER`)
3. Resolve member IDs via `KR.ADMIN DESCRIBE-MEMBER` on each unique node
4. Set routes via `KR.ADMIN ROUTE SET PRIMARY|STANDBY`
5. Enable shards via `KR.ADMIN SET-SHARD-STATUS READWRITE`

### describe-cluster

Prints the shards and the members of a cluster. It reads `KR.ADMIN DESCRIBE-CLUSTER`, `KR.ADMIN LIST-MEMBERS` and
`KR.ADMIN LIST-SILENT-MEMBERS` from one member. The shard table names each owner by the first four characters of its
member ID. The member table lists every registered member with its status and addresses, and shows whether the
connected member sees it as silent.

```
kronotop-ctl describe-cluster [options]
```

**Options:**

| Option                  | Description                                                     |
|-------------------------|-----------------------------------------------------------------|
| `--address <host:port>` | Internal address of a cluster member. Default: `localhost:3320` |
| `--json`                | Print the output as JSON.                                       |

**Example:**

```bash
kronotop-ctl describe-cluster --address node1:3320
```

```
Cluster:           my-cluster
Metadata version:  1.0.0

SHARD  STATUS      VOLUME   PRIMARY  STANDBYS
0      READWRITE   shard-0  006c     a3f1
1      INOPERABLE  shard-1  -        -

MEMBER  STATUS   SILENT  INTERNAL       EXTERNAL
006c    RUNNING  no      10.0.0.1:3320  10.0.0.1:5484
a3f1    RUNNING  no      10.0.0.2:3320  10.0.0.2:5484
```

### list-volumes

Prints the volumes of a cluster, one row for each volume on each member that owns its shard (the primary and the
standbys). It reads the shard owners from one member, then connects to the internal address of every owner and runs
`VOLUME.ADMIN DESCRIBE` there. A row that cannot be read is printed with the reason in the `ERROR` column.

```
kronotop-ctl list-volumes [options]
```

**Options:**

| Option                  | Description                                                                      |
|-------------------------|----------------------------------------------------------------------------------|
| `--address <host:port>` | Internal address of a cluster member. Default: `localhost:3320`                  |
| `--shard <id>`          | Show only the volume of this shard.                                              |
| `--member <id>`         | Show only the volumes on this member, as a full ID or its first four characters. |
| `--timeout <seconds>`   | Max seconds to wait for each member to connect and reply. Default: 10            |
| `--json`                | Print the output as JSON.                                                        |

**Example:**

```bash
kronotop-ctl list-volumes --address node1:3320
```

```
VOLUME   MEMBER  ROLE     STATUS     SEGMENTS  SIZE     USED     FREE       GARBAGE  ENTRIES  ERROR
shard-0  006c    primary  READWRITE  2         2.0 GiB  1.0 GiB  512.0 MiB  25.0%    150      -
shard-0  a3f1    standby  READWRITE  2         2.0 GiB  1.0 GiB  512.0 MiB  25.0%    150      -
```
