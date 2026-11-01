# Proof: Millions of databases

This script creates 1,000,001 namespaces in one Kronotop cluster to back the claim
"Millions of databases." Each namespace is an isolated logical database on one shared
FoundationDB.

## Structure

The tree sits under Kronotop's default namespace `global`, which always exists and
cannot be removed. The script does not create or touch `global`. It creates
`global.millions` as the root of its own subtree; that root is the "+1" in the total.

```
global                                     (already exists, untouched)
global.millions                            1
global.millions.<parent-uuid>             1000   parents
global.millions.<parent-uuid>.<leaf-uuid> 999 leaves
                                          ---------
                                          1,000,001
```

`total = 1 + parents + (parents * leaves)`. With the defaults `parents=1000` and
`leaves=999` that is exactly 1,000,001. The script creates every level with its own
`NAMESPACE CREATE` call; nothing is created implicitly.

## How it works

Each `NAMESPACE CREATE` runs in its own FoundationDB transaction. The script runs a fixed pool of `asyncio` workers
over one connection pool. Concurrency equals the number of workers. A background reporter prints throughput every
two seconds.

## Run

The script declares its dependencies inline (PEP 723), so [uv](https://docs.astral.sh/uv/)
fetches them into a temporary environment. You do not need a virtualenv or `pip install`.

Start a local Kronotop cluster with Docker Compose. Download the Compose file:

```bash
curl -O https://kronotop.com/kronotop-quickstart.yaml
```

Start the cluster:

```bash
docker compose -f kronotop-quickstart.yaml up
```

Run the script against that cluster:

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

A small run to check the setup before the full million:

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

List a slice of the tree with the client to confirm it exists:

```
NAMESPACE LIST global.millions
```
