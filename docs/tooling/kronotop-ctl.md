---
title: "kronotop-ctl"
description: "Control tool for Kronotop cluster operations."
---

`kronotop-ctl` is the control tool for Kronotop clusters. It automates administrative procedures that would
otherwise require running a series of `KR.ADMIN` commands by hand.

## Usage

Run the jar directly:

```bash
java -jar kronotop-ctl-2026.06-3.jar <command> [options]
```

Or use the jar bundled in the Docker image:

```bash
docker run --rm -it --network kronotop \
  ghcr.io/kronotop/kronotop:latest java -jar /opt/kronotop/kronotop-ctl.jar <command> [options]
```

## bootstrap

Bootstraps a new Kronotop cluster. It connects to the first primary node, initializes the cluster, waits for
shard discovery, assigns primary and standby shard ownership, and enables the shards for read-write traffic.

Shard assignments are given per shard kind. Valid shard kinds are `BUCKET` and `STASH`. Addresses use the
internal port (default 3320).

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

## Help

```
Usage: kronotop-ctl [-hV] [COMMAND]
Control tool for Kronotop clusters.
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  bootstrap  Bootstrap a Kronotop cluster.
```

```
Usage: kronotop-ctl bootstrap [-hV] [--shard-discovery-timeout <seconds>]
                              [--primary <shard-kind host:port=shard-ids>
                              <shard-kind host:port=shard-ids>...]...
                              [--standby <shard-kind host:port=shard-ids>
                              <shard-kind host:port=shard-ids>...]...
Bootstrap a Kronotop cluster.
  -h, --help      Show this help message and exit.
      --primary <shard-kind host:port=shard-ids> <shard-kind host:
        port=shard-ids>...
                  Assign primary shards (repeatable). Example: --primary BUCKET
                    node1:3320=0,1,2
      --shard-discovery-timeout <seconds>
                  Max seconds to wait for shard discovery after cluster
                    initialization. Default: 30
      --standby <shard-kind host:port=shard-ids> <shard-kind host:
        port=shard-ids>...
                  Assign standby shards (repeatable). Example: --standby BUCKET
                    node2:3320=0,1,2
  -V, --version   Print version information and exit.
```
