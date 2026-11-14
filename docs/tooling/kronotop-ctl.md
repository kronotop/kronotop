---
title: "kronotop-ctl"
description: "Control tool for Kronotop cluster operations."
---

`kronotop-ctl` is the control tool for Kronotop clusters. It runs administrative procedures that you would
otherwise do by hand with a series of `KR.ADMIN` commands.

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

`bootstrap` sets up a new Kronotop cluster. It connects to the first primary node and initializes the cluster. After the
shards are discovered, it assigns their primary and standby owners and opens them for read-write traffic.

Addresses use the internal port (default 3320).

`--shards` sets how many shards the cluster gets. The default is 1. Shard ids start at 0 and must be lower than
the shard count.

For a single-node cluster:

```bash
kronotop-ctl bootstrap \
  --shards 3 \
  --primary localhost:3320=0,1,2
```

For a multi-node cluster with primary and standby assignments:

```bash
kronotop-ctl bootstrap \
  --shards 3 \
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
                              [--shards <count>] [--primary <host:
                              port=shard-ids>...]... [--standby <host:
                              port=shard-ids>...]...
Bootstrap a Kronotop cluster.
  -h, --help             Show help.
      --primary <host:port=shard-ids>...
                         Assign primary shards (repeatable). Example: --primary
                           node1:3320=0,1,2
      --shard-discovery-timeout <seconds>
                         Max seconds to wait for shard discovery after cluster
                           initialization. Default: 30
      --shards <count>   Number of shards to create. Default: 1
      --standby <host:port=shard-ids>...
                         Assign standby shards (repeatable). Example: --standby
                           node2:3320=0,1,2
```
