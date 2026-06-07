---
title: "Quickstart"
description: "Start a minimal Kronotop cluster with Docker Compose, insert a document, and query it back."
---

The fastest way to try Kronotop is with Docker Compose. Prebuilt jars are also published on the
[Releases page](https://github.com/kronotop/kronotop/releases); download them and run with `java -jar`,
which requires Java 25 or newer. If you want to build Kronotop from source and run a node locally, see the
[Building and Running guide](https://github.com/kronotop/kronotop/blob/main/BUILDING.md).

## Start the Cluster

Download the Docker Compose file:

```bash
curl -O https://kronotop.com/kronotop-quickstart.yaml
```

Start the cluster:

```bash
docker compose -f kronotop-quickstart.yaml up
```

This starts a minimal cluster: one FoundationDB instance, one Kronotop primary node, and one standby node.
The primary owns bucket shard 0; the standby replicates it for failover.

## Connect

Once all containers are running, connect with `kronotop-cli` on port 3320:

```bash
docker run --rm -it --platform linux/amd64 --network kronotop \
  ghcr.io/kronotop/kronotop:latest kronotop-cli -h kronotop-primary -p 3320
```

Alternatively, you can use `kronotop-cli` or `valkey-cli` if you have them installed locally:

```bash
valkey-cli -p 3320
```

Verify the cluster is up:

```kronotop
kronotop-primary:3320> PING
PONG
kronotop-primary:3320> KR.ADMIN DESCRIBE-CLUSTER
1# "metadata_version" => "1.0.0"
2# "cluster_name" => "development"
3# "bucket" =>
   1# (integer) 0 =>
      1# "primary" => "f627bf46f8627333a064de5c388d0316cc223a54"
      2# "standbys" => 1) "e159f73fb21cd6ac80639b2cc1e087e8330cd947"
      3# "status" => "READWRITE"
      4# "linked_volumes" => 1) "bucket-shard-0"
```

## Session Setup

Port 3320 is the internal/admin port. Client traffic goes through port 5484. Connect to the client port:

```bash
docker run --rm -it --platform linux/amd64 --network kronotop \
  ghcr.io/kronotop/kronotop:latest kronotop-cli -h kronotop-primary -p 5484
```

`kronotop-cli` sets these automatically. If you are using `valkey-cli`, set the session attributes manually
for human-readable output:

```kronotop
SESSION.ATTRIBUTE SET input_type json
SESSION.ATTRIBUTE SET reply_type json
SESSION.ATTRIBUTE SET object_id_format hex
```

## Insert and Query a Document

Create a bucket:

```kronotop
kronotop-primary:5484> BUCKET.CREATE orders
OK
```

Insert a document:

```kronotop
kronotop-primary:5484> BUCKET.INSERT orders DOCS '{
  "item": "keyboard",
  "qty": 2,
  "price": 49.99
}'
1) "6a133c8806bf494c9e7e00cb"
```

Query it back with the Bucket Query Language (BQL):

```kronotop
kronotop-primary:5484> BUCKET.QUERY orders '{"qty": {"$gte": 1}}'
1# "cursor_id" => (integer) 1
2# "entries" => 1) {"_id": "6a133c8806bf494c9e7e00cb", "item": "keyboard", "qty": 2, "price": 49.99}
```

## One Transaction, Multiple Models

Kronotop allows different data models and namespaces to participate in the same strictly serializable transaction
boundary. A single transaction can atomically write a document and update a counter across isolated namespaces:

```kronotop
BEGIN

# In the sales namespace: record a new order.
NAMESPACE USE production.sales
BUCKET.INSERT orders DOCS '{"item": "keyboard", "qty": 2, "price": 49.99}'

# In the inventory namespace: decrement stock, conflict-free.
NAMESPACE USE production.inventory
ZINC.I64 keyboard -2

COMMIT
```

Namespaces must exist before use; create them with `NAMESPACE CREATE production.sales` and
`NAMESPACE CREATE production.inventory`.

## Next Steps

- [Bucket Tutorial](bucket/tutorial.md): a walkthrough of the Bucket API, from creating a bucket to removing it
- [BQL Reference](bucket/bql-reference.md): the query language in full
- [ZMap](zmap/index.md): the ordered key-value model
- [Transactions](transactions/index.md): explicit transactions and snapshot reads
- [Namespaces](namespaces/index.md): logical isolation and cross-namespace transactions
- [Configuration Reference](config.md): overriding the built-in defaults
