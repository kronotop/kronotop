# Proof: Millions of databases

This proof creates **1,000,001 namespaces** in a single Kronotop cluster to back the
claim *"Millions of databases."* Every namespace is an isolated database living on top
of one shared FoundationDB.

## Structure

The tree is nested under Kronotop's fixed default namespace `global`, which always
exists and cannot be removed. The script never creates or touches `global` itself.
The `global.millions` node is the single explicit "+1" root it creates.

```
global                                     (already exists, untouched)
global.millions                            1
global.millions.<parent-uuid>             1000   parents
global.millions.<parent-uuid>.<leaf-uuid> 999 leaves
                                          ---------
                                          1,000,001
```

`total = 1 + parents + (parents * leaves)`. With the defaults `parents=1000` and
`leaves=999` that is exactly 1,000,001. Each level is created explicitly, so the
counter matches the tree one-to-one.

## How it works

Each `NAMESPACE CREATE` is a single, isolated FoundationDB transaction. The script runs a fixed pool of `asyncio` workers 
over one multiplexed connection pool. Concurrency equals the number of workers; a background reporter prints throughput 
every two seconds.

## Run

The script declares its dependencies inline (PEP 723), so [uv](https://docs.astral.sh/uv/)
fetches them into an ephemeral environment on the fly. No virtualenv, no `pip install`.

Spin up a local Kronotop cluster with Docker Compose.

Download the Docker Compose file:

```bash
curl -O https://kronotop.com/kronotop-quickstart.yaml
```

Start the cluster:

```bash
docker compose -f kronotop-quickstart.yaml up
```

Finally, run the proof script, which creates **1,000,001 logical databases (namespaces)** on the same cluster.

```bash
uv run millions_of_databases.py
```

Common options:

```bash
uv run millions_of_databases.py \
  --host 127.0.0.1 --port 5484 \
  --parents 1000 --leaves 999 \
  --concurrency 200
```

Quick smoke test before the full million:

```bash
uv run millions_of_databases.py --parents 10 --leaves 9
```

| Flag            | Default           | Meaning                                    |
|-----------------|-------------------|--------------------------------------------|
| `--host`        | `127.0.0.1`       | Kronotop client host                       |
| `--port`        | `5484`            | Kronotop client port                       |
| `--root`        | `global.millions` | Sub-tree root under the `global` namespace |
| `--parents`     | `1000`            | Number of parent namespaces                |
| `--leaves`      | `999`             | Leaves per parent                          |
| `--concurrency` | `200`             | Number of concurrent workers               |
| `--queue-size`  | `10000`           | In-flight work queue bound (memory cap)    |

## Sample output

```
Creating 1,000,001 namespaces under 'global.millions' (1,000 parents x 999 leaves) with 200 workers -> 127.0.0.1:5484
     412,330/1,000,001  created=412,330 existed=0 failed=0     8,210/s now    8,055/s avg
...
Done in 2,693.0s (44.9 min)  created=1,000,001 existed=0 failed=0  avg=371/s
```

## Spot-check

You can confirm a slice of the tree with the client:

```
NAMESPACE LIST global.millions
```
