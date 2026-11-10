---
title: "kronotop-ctl"
description: "Control tool for Kronotop cluster operations."
---

`kronotop-ctl` is the control tool for Kronotop clusters. It automates administrative procedures that would
otherwise require running a series of `KR.ADMIN` commands by hand.

## Usage

Run the jar directly:

```bash
java -jar kronotop-ctl-2026.11-1.jar <command> [options]
```

Or use the jar bundled in the Docker image:

```bash
docker run --rm -it --network kronotop \
  ghcr.io/kronotop/kronotop:latest java -jar /opt/kronotop/kronotop-ctl.jar <command> [options]
```

## bootstrap

Bootstraps a new Kronotop cluster. It connects to the first primary node, initializes the cluster, waits for
shard discovery, assigns primary and standby shard ownership, and enables the shards for read-write traffic.

Addresses use the internal port (default 3320).

Single-node cluster:

```bash
kronotop-ctl bootstrap \
  --primary localhost:3320=0,1,2
```

Multi-node cluster with primary and standby assignments:

```bash
kronotop-ctl bootstrap \
  --primary node1:3320=0,1,2 \
  --standby node2:3320=0,1,2
```

## Help

```
Usage: kronotop-ctl [-hv] [COMMAND]
Control tool for Kronotop clusters.
  -h, --help      Show help.
  -v, --version   Print version information and quit.
Commands:
  bootstrap  Bootstrap a Kronotop cluster.
```

```
Usage: kronotop-ctl bootstrap [-h] [--shard-discovery-timeout <seconds>]
                              [--primary <host:port=shard-ids>...]...
                              [--standby <host:port=shard-ids>...]...
Bootstrap a Kronotop cluster.
  -h, --help   Show help.
      --primary <host:port=shard-ids>...
               Assign primary shards (repeatable). Example: --primary node1:
                 3320=0,1,2
      --shard-discovery-timeout <seconds>
               Max seconds to wait for shard discovery after cluster
                 initialization. Default: 30
      --standby <host:port=shard-ids>...
               Assign standby shards (repeatable). Example: --standby node2:
                 3320=0,1,2
```
