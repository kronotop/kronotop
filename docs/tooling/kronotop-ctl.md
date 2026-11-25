---
title: "kronotop-ctl"
description: "Control tool for Kronotop cluster operations."
---

`kronotop-ctl` is the control tool for Kronotop clusters. It runs administrative procedures that you would
otherwise do by hand with a series of `KR.ADMIN` commands.

## Usage

Run the jar directly:

```bash
java -jar kronotop-ctl-2026.11-2.jar <command> [options]
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

## describe-cluster

`describe-cluster` prints the shards and the members of a cluster.

`--address` is the internal address of any cluster member. Without it, the tool connects to `localhost:3320`.

```bash
kronotop-ctl describe-cluster --address node1:3320
```

```
Cluster:           my-cluster
Metadata version:  1.0.0

SHARD  STATUS      VOLUME   PRIMARY  STANDBYS
0      READWRITE   shard-0  006c     a3f1
1      READWRITE   shard-1  006c     a3f1
2      INOPERABLE  shard-2  -        -

MEMBER  STATUS   SILENT  INTERNAL       EXTERNAL
006c    RUNNING  no      10.0.0.1:3320  10.0.0.1:5484
7be2    RUNNING  no      10.0.0.3:3320  10.0.0.3:5484
a3f1    RUNNING  no      10.0.0.2:3320  10.0.0.2:5484
```

In the shard table, `PRIMARY` and `STANDBYS` show the first four characters of each owner's member ID. `KR.ADMIN`
commands that take a member ID accept this prefix in place of the full ID. A `-` means that the shard has no primary
or no standby.

The member table lists every registered member, including members that own no shard. A member with several internal
or external addresses has them separated by commas.

`SILENT` is `yes` when a member has stopped sending heartbeats. This is the view of the member that you connect to,
so another member may report a different result.

`--json` prints the same shards and members as indented JSON, with full member IDs. A shard without a primary has a
`null` primary.

```bash
kronotop-ctl describe-cluster --json
```

```json
{
  "cluster_name": "my-cluster",
  "metadata_version": "1.0.0",
  "shards": [
    {
      "id": 0,
      "status": "READWRITE",
      "linked_volume": "shard-0",
      "primary": "006cdc459c59e600c76494e8388857fc3cba2fa8",
      "standbys": [
        "a3f18b2e74d9c5601f82e4a7b390d612c8f7e149"
      ]
    }
  ],
  "members": [
    {
      "member_id": "006cdc459c59e600c76494e8388857fc3cba2fa8",
      "status": "RUNNING",
      "silent": false,
      "internal_advertise": [
        "10.0.0.1:3320"
      ],
      "external_advertise": [
        "10.0.0.1:5484"
      ]
    },
    {
      "member_id": "a3f18b2e74d9c5601f82e4a7b390d612c8f7e149",
      "status": "RUNNING",
      "silent": false,
      "internal_advertise": [
        "10.0.0.2:3320"
      ],
      "external_advertise": [
        "10.0.0.2:5484"
      ]
    }
  ]
}
```

The command exits with 0 on success. On an error, it prints the message to stderr and exits with 1.

## Help

### kronotop-ctl

```
Usage: kronotop-ctl [-hv] [COMMAND]
Control tool for Kronotop clusters.
  -h, --help      Show help.
  -v, --version   Print version information and quit.
Commands:
  bootstrap         Bootstrap a Kronotop cluster.
  describe-cluster  Show the shards of a cluster with their status and owners.
```

### kronotop-ctl bootstrap

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

### kronotop-ctl describe-cluster

```
Usage: kronotop-ctl describe-cluster [-h] [--json] [--address <host:port>]
Show the shards of a cluster with their status and owners.
      --address <host:port>
               Internal address of a cluster member. Default: localhost:3320
  -h, --help   Show help.
      --json   Print the output as JSON.
```
