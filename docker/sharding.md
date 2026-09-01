# Running Kronotop with more than one shard

`multi-shard-cluster.yaml` in this folder starts a two node cluster with two bucket shards. This
file explains what that compose file does, so you can change it for your own setup.

`docker-compose.yaml` is the simple case: one shard, one primary, one standby. Read this file when
you need more than one shard.

## 1. Pick the shard count

Bucket data is split into shards. The number of shards comes from the `bucket.shards` setting. The
default is 1.

Give every node the same value. In Docker you pass it as a JVM property:

```yaml
environment:
  JAVA_TOOL_OPTIONS: "-Dbucket.shards=2"
```

The setting is read when the cluster is initialized, and the shards are created at that moment.
There is no command to add or remove shards later, so pick the count before the first start.

## 2. Spread the shards over the nodes

Every shard has one primary node. The primary takes the writes. A shard can also have standby nodes
that replicate the primary.

`multi-shard-cluster.yaml` uses a crossed layout:

| Shard | Primary | Standby |
|-------|---------|---------|
| 0     | node1   | node2   |
| 1     | node2   | node1   |

Both nodes are primary for one shard, so both nodes can take writes. Both nodes are also standby for
the other shard, so they still cover each other. If you put all primaries on node1, node2 only
replicates and does no write work.

A bucket is not spread over all shards on its own. `BUCKET.CREATE` picks one shard by round robin,
or you name the shards yourself with the `SHARDS` option. So with this layout the buckets you create
land on both nodes, not the documents of a single bucket. A write is served by a node that owns one
of the bucket's shards. If you send it to a node that owns none of them, the node redirects you to
the right one.

## 3. Give each node its addresses

The image binds both ports to `0.0.0.0`, so a node cannot know the address clients and the other
nodes use to reach it. Each node publishes its own addresses with two environment variables, read by
`start.sh`:

```yaml
KRONOTOP_EXTERNAL_ADVERTISE: "127.0.0.1:5485,kronotop-node2:5484"
KRONOTOP_INTERNAL_ADVERTISE: "kronotop-node2:3320"
```

Both take a comma-separated list. The first entry is the preferred one.

- `KRONOTOP_EXTERNAL_ADVERTISE` is the address clients get. The example is node2 from
  `multi-shard-cluster.yaml`. The first entry is the port published on your machine, `5485`. The
  second entry is for clients that run inside the Docker network.
- `KRONOTOP_INTERNAL_ADVERTISE` is the address other nodes get. It must be reachable inside the
  Docker network, so use the compose service name and the internal port.

Both variables are needed on every node. A node without them fails at startup.

## 4. Give the layout at bootstrap

The layout is passed to the cluster once, at bootstrap time:

```yaml
KRONOTOP_BOOTSTRAP: "--primary BUCKET kronotop-node1:3320=0 --primary BUCKET kronotop-node2:3320=1 --standby BUCKET kronotop-node2:3320=0 --standby BUCKET kronotop-node1:3320=1"
```

Each part is `<shard kind> <host>:<port>=<shard ids>`. The port is the internal port, 3320 by
default. For the full flag syntax, see `docs/tooling/kronotop-ctl.md`.

Two environment variables control this, both read by `start.sh`:

- `KRONOTOP_BOOTSTRAP` goes on one node only. When that node answers PING, the entrypoint runs
  `kronotop-ctl bootstrap` with this value in the background.
- `KRONOTOP_STANDBY_HOST` makes the entrypoint wait until the given host answers PING before it runs
  bootstrap. Bootstrap connects to every node listed in the assignments to read its member id, so
  all of them must be up. Without this wait, bootstrap can start too early and fail.

## Run it

```bash
docker compose -f multi-shard-cluster.yaml up
```
