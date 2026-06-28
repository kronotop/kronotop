# Building and Running Kronotop Locally

This guide covers building Kronotop from source and running a node locally for development. To try Kronotop without
building from source, use the [Quickstart](README.md#quickstart) in the README, which starts a cluster with Docker
Compose.

## Contents

- [Prerequisites](#prerequisites)
- [Installing FoundationDB](#installing-foundationdb)
- [Building from Source](#building-from-source)
- [Running a Local Node](#running-a-local-node)
- [Bootstrapping the Cluster](#bootstrapping-the-cluster)
- [Connecting](#connecting)
- [Running Multiple Nodes Locally](#running-multiple-nodes-locally)
- [Configuration Overrides](#configuration-overrides)
- [Docker Alternative](#docker-alternative)
- [Getting Help](#getting-help)

## Prerequisites

- **Java (JDK 25 or newer).** Kronotop uses virtual threads, the incubator vector module, and JVM flags that
  require a recent JDK. Install one yourself, for example, [Eclipse Temurin](https://adoptium.net/) or
  [Amazon Corretto](https://aws.amazon.com/corretto/). Check the version:

  ```
  java -version
  ```

  It must report 25 or higher.

- **FoundationDB 7.3.68.** You need both the client library and a running server. See
  [Installing FoundationDB](#installing-foundationdb) below.

- **Git** to clone the repository.

Maven does not need to be installed. The project ships a Maven wrapper (`./mvnw`) that downloads the required
version.

## Installing FoundationDB

Kronotop targets **FoundationDB 7.3.68**. Both the client library and a local server are required for development.

Release page: <https://github.com/apple/foundationdb/releases/tag/7.3.68>

All assets download from `https://github.com/apple/foundationdb/releases/download/7.3.68/<file>`.

### macOS

Download and install the package for your CPU. It installs both the client and the server.

- Apple Silicon: `FoundationDB-7.3.68_arm64.pkg`
- Intel: `FoundationDB-7.3.68_x86_64.pkg`

```
curl -LO https://github.com/apple/foundationdb/releases/download/7.3.68/FoundationDB-7.3.68_arm64.pkg
sudo installer -pkg FoundationDB-7.3.68_arm64.pkg -target /
```

The installer configures and starts a single-node database and writes the cluster file to
`/usr/local/etc/foundationdb/fdb.cluster`. No manual initialization is needed.

### Linux (Debian / Ubuntu)

Install the `foundationdb-clients` package first, then the server package. Use the `aarch64` variants on ARM machines.

```
curl -LO https://github.com/apple/foundationdb/releases/download/7.3.68/foundationdb-clients_7.3.68-1_amd64.deb
curl -LO https://github.com/apple/foundationdb/releases/download/7.3.68/foundationdb-server_7.3.68-1_amd64.deb
sudo dpkg -i foundationdb-clients_7.3.68-1_amd64.deb foundationdb-server_7.3.68-1_amd64.deb
```

ARM packages: `foundationdb-clients_7.3.68-1_aarch64.deb` and `foundationdb-server_7.3.68-1_aarch64.deb`.

The server package configures and starts a single-node database and writes the cluster file to
`/etc/foundationdb/fdb.cluster`. No manual initialization is needed.

### Linux (RHEL / CentOS)

```
curl -LO https://github.com/apple/foundationdb/releases/download/7.3.68/foundationdb-clients-7.3.68-1.el7.x86_64.rpm
curl -LO https://github.com/apple/foundationdb/releases/download/7.3.68/foundationdb-server-7.3.68-1.el7.x86_64.rpm
sudo rpm -i foundationdb-clients-7.3.68-1.el7.x86_64.rpm foundationdb-server-7.3.68-1.el7.x86_64.rpm
```

ARM packages on EL9: `foundationdb-clients-7.3.68-1.el9.aarch64.rpm` and `foundationdb-server-7.3.68-1.el9.aarch64.rpm`.

The server package configures and starts a single-node database. No manual initialization is needed.

### Windows

FoundationDB does not publish a native Windows installer for 7.3.68. Use one of these instead:

- **WSL2**: run a Linux distribution under WSL2 and install the Debian/Ubuntu packages there.
- **Docker**: run the official image `foundationdb/foundationdb:7.3.68` and point Kronotop at its cluster file.

### Initializing the database

The native packages (macOS, Linux) configure and start a single-node database during installation, so it is ready
once the installation finishes.

The Docker image is different: it starts a server process but does not configure a database. Configure it once after
the container starts:

```
fdbcli --exec "configure new single memory"
```

This is the same step the [Docker Compose](#docker-alternative) setup runs automatically on the first start.

### Verify the server is up

```
fdbcli --exec status
```

A running, configured cluster prints a status summary. A connection error means the FoundationDB server is not
running; a "database is unavailable" message means it is running but not yet configured.

## Building from Source

Clone the repository and build from the project root.

Compile all modules:

```
./mvnw clean compile
```

Build the runnable artifacts (shaded jars):

```
./mvnw clean package
```

Add `-DskipTests` to skip the test phase for a faster build:

```
./mvnw clean package -DskipTests
```

The build produces three runnable jars:

| Jar                                              | Purpose                            |
|--------------------------------------------------|------------------------------------|
| `kronotop/target/kronotop-2026.06-4.jar`         | The server                         |
| `kronotop-cli/target/kronotop-cli-2026.06-4.jar` | Interactive command-line client    |
| `kronotop-ctl/target/kronotop-ctl-2026.06-4.jar` | Cluster control and bootstrap tool |

### Running Tests

Run the tests for the core module:

```
./mvnw test -pl kronotop
```

Run a single test class or method:

```
./mvnw test -Dtest=PipelineIntegrationTest
./mvnw test -Dtest=PipelineIntegrationTest#shouldStreamResults
```

### Parallel Test Execution

The core module has around 6,000 tests. They run in parallel across multiple JVM forks, controlled by the
`forkCount` property. The default is `0.5C`, which uses half the available CPU cores.

Override it with `-DforkCount=<value>`:

```
./mvnw test -pl kronotop -DforkCount=1C      # one fork per CPU core
./mvnw test -pl kronotop -DforkCount=4       # exactly 4 forks
./mvnw test -pl kronotop -DforkCount=1       # single fork, sequential
```

A `C` suffix is a multiplier of the CPU core count (`0.5C` = half, `1C` = one per core). A plain number is an
absolute fork count. Use `1` or `0` to run sequentially. Higher fork counts finish faster but use more memory.

Avoid running the full test suite unless necessary; it is slow. Prefer targeted module or class-level runs.

## Running a Local Node

FoundationDB must be running first (`fdbcli --exec status`).

Use the launcher script to start a node. It locates the server jar under `kronotop/target` and applies the required
JVM flags:

```
./bin/kronotop
```

To run the jar directly, pass the JVM flags explicitly:

```
java --sun-misc-unsafe-memory-access=allow \
     --add-opens java.base/sun.misc=ALL-UNNAMED \
     --enable-native-access=ALL-UNNAMED \
     --add-modules jdk.incubator.vector \
     -jar kronotop/target/kronotop-2026.06-4.jar
```

On startup the node binds two ports: the client port `5484` and the internal/admin port `3320`.

By default, Kronotop connects to the local FoundationDB using its default cluster file (
`/usr/local/etc/foundationdb/fdb.cluster`
on macOS, `/etc/foundationdb/fdb.cluster` on Linux). To use a different cluster file, set `foundationdb.clusterfile`:

```
./bin/kronotop -Dfoundationdb.clusterfile=/path/to/fdb.cluster
```

Local data (volumes and segments) is written to a `kronotop-data` directory in the current working directory by
default. Override it with `-Ddata_dir=/path/to/data`.

## Bootstrapping the Cluster

A freshly started node serves no traffic until its shards are initialized and routed. `kronotop-ctl`, the cluster
control tool, bootstraps the cluster in one step.

Run it from the built jar:

```
java -jar kronotop-ctl/target/kronotop-ctl-2026.06-4.jar bootstrap --primary BUCKET 127.0.0.1:3320=0
```

This initializes the cluster, waits for shard discovery, assigns shard `0` to the node as primary, and opens it for
read/write traffic. Bootstrap is idempotent: if the cluster is already initialized, it skips and exits.

The assignment syntax is `host:port=shard-ids`, where the port is the internal/admin port (`3320`) and shard ids are
comma-separated. Valid shard kinds are `BUCKET` and `STASH`.

For a two-node primary/standby setup:

```
java -jar kronotop-ctl/target/kronotop-ctl-2026.06-4.jar bootstrap \
  --primary BUCKET 127.0.0.1:3320=0 \
  --standby BUCKET 127.0.0.1:3322=0
```

To drive the steps by hand with `KR.ADMIN` commands (initialize, list members, assign routes, set shard status),
follow the [Cluster Operations Guide](docs/admin/cluster/operations-guide.md).

## Connecting

Connect with `kronotop-cli` on the client port:

```
java -jar kronotop-cli/target/kronotop-cli-2026.06-4.jar -h 127.0.0.1 -p 5484
```

The CLI defaults to host `127.0.0.1` and port `5484`, so `-h` and `-p` can be omitted for a local node. Admin
commands run against the internal port `3320`.

`redis-cli` and `valkey-cli` also work. With those clients, set the session attributes manually for readable output:

```
SESSION.ATTRIBUTE SET input_type json
SESSION.ATTRIBUTE SET reply_type json
SESSION.ATTRIBUTE SET object_id_format hex
```

`kronotop-cli` sets these automatically.

Smoke test the node:

```
127.0.0.1:5484> PING
PONG
127.0.0.1:5484> BUCKET.CREATE orders
OK
127.0.0.1:5484> BUCKET.INSERT orders DOCS '{"item": "keyboard", "qty": 2, "price": 49.99}'
1) "6a133c8806bf494c9e7e00cb"
127.0.0.1:5484> BUCKET.QUERY orders '{"qty": {"$gte": 1}}'
1# "cursor_id" => (integer) 1
2# "entries" => 1) {"_id": "6a133c8806bf494c9e7e00cb", "item": "keyboard", "qty": 2, "price": 49.99}
```

## Running Multiple Nodes Locally

To run more than one node on the same machine against the same FoundationDB, give each node its own ports and data
directory with `-D` overrides. The bootstrap assignment refers to the internal port.

Node 1 (primary):

```
./bin/kronotop \
  -Dnetwork.external.port=5484 \
  -Dnetwork.internal.port=3320 \
  -Ddata_dir=/tmp/kronotop/node1
```

Node 2 (standby):

```
./bin/kronotop \
  -Dnetwork.external.port=5485 \
  -Dnetwork.internal.port=3322 \
  -Ddata_dir=/tmp/kronotop/node2
```

Then bootstrap with both nodes:

```
java -jar kronotop-ctl/target/kronotop-ctl-2026.06-4.jar bootstrap \
  --primary BUCKET 127.0.0.1:3320=0 \
  --standby BUCKET 127.0.0.1:3322=0
```

## Configuration Overrides

Kronotop reads its defaults from a built-in HOCON configuration. Two ways to override values at the startup:

- Supply a full config file: `-Dconfig.file=/path/to/kronotop.conf`. Only the values you change need to appear in it.
- Override individual parameters: `-Dnetwork.external.port=6000`, `-Dcluster.name=dev`, and so on. The property name
  matches the dotted config path.

`-D` overrides take precedence over a config file. See the [Configuration Reference](docs/config.md) for the full
list of parameters.

## Docker Alternative

To run Kronotop without building from source, the [README Quickstart](README.md#quickstart) starts a FoundationDB
instance, a Kronotop primary, and a standby with Docker Compose. The compose file and related assets live in the
[`docker/`](docker/) directory.

## Getting Help

For questions and problems:

- [GitHub Issues](https://github.com/kronotop/kronotop/issues)
- [Discord](https://discord.gg/VPRNvdh2C)
