# kronotop-benchmark

Vector search benchmarks for Kronotop.

## Build

```bash
mvn clean package -pl kronotop-benchmark
```

The shaded JAR is produced at `target/kronotop-benchmark-*.jar`.

## Benchmarks

### vector tweets — US Senator Tweets (384-dim)

**1. Download the dataset from HuggingFace**

Go to https://huggingface.co/datasets/m-newhauser/senator-tweets, download the Parquet files
under `data/`, and place them in a local directory, e.g.:

```
senator-tweets/
└── data/
    ├── train-00000-of-00001.parquet
    └── test-00000-of-00001.parquet
```

**2. Generate the JSONL file**

`uv` is the most practical way to run a Python script:

```bash
uv run --with pyarrow scripts/prepare_senator_tweets.py \
      --parquet <path-to>/senator-tweets/data/train-00000-of-00001.parquet \
      --output <path-to>/senator-tweets/train.jsonl
```

Expected output:

```
  10,000 rows written...
  20,000 rows written...
  30,000 rows written...
  40,000 rows written...
  50,000 rows written...
  60,000 rows written...
  70,000 rows written...
Done: 79,754 rows → senator-tweets/train.jsonl
```

Produces `train.jsonl` (~376 MB, 79,754 rows).

**3. Run the benchmark**

```bash
java -jar target/kronotop-benchmark-*.jar vector tweets \
    --data train.jsonl \
    --host localhost \
    --port 5484
```

```bash
java -jar kronotop-benchmark/target/kronotop-benchmark-2026.06-3.jar vector tweets 
    --threads 8 
    --search-rounds 100 
    --top-k 10 
    --data ~/kronotop-vector-demo/senator-tweets/train.jsonl
```

```bash
java -jar kronotop-benchmark/target/kronotop-benchmark-2026.06-3.jar vector tweets 
    --threads 8 
    --skip-load 
    --search-rounds 100 
    --top-k 10
```

Options:

| Flag              | Default          | Description                     |
|-------------------|------------------|---------------------------------|
| `--host`          | `localhost`      | Kronotop host                   |
| `--port`          | `5484`           | Kronotop port                   |
| `--data`          | *(required)*     | Path to JSONL file              |
| `--bucket`        | `senator-tweets` | Bucket name                     |
| `--batch-size`    | `100`            | Insert batch size               |
| `--max-docs`      | `0` (all)        | Limit documents loaded          |
| `--search-rounds` | `100`            | Number of search iterations     |
| `--top-k`         | `10`             | Results per query               |
| `--threads`       | `1`              | Concurrent search threads       |
| `--skip-load`     | —                | Skip data loading phase         |
| `--skip-filter`   | —                | Skip filtered search benchmarks |

Results on Apple Silicon M4 Pro with 48GB of RAM:

```bash
java -jar kronotop-benchmark/target/kronotop-benchmark-2026.06-3.jar vector tweets --threads 8 --skip-load --search-rounds 100 --top-k 10
=== Kronotop Vector Search Benchmark (Tweets) ===
Host: localhost:5484
Bucket: senator-tweets
Batch size: 100
Max docs: all
Search rounds: 100
Top-K: 10
Threads: 8

Skipping data load phase.
No query vectors available. Generating random vectors for search benchmark.

--- Vector Search Benchmark (unfiltered) ---
Queries: 100, Top-K: 10, Threads: 8

Unfiltered vector search results (100 queries, 8 threads):
  Throughput:  3548.1 queries/sec
  Avg:         1.22 ms
  P50:         0.99 ms
  P95:         3.63 ms
  P99:         4.44 ms
  Min:         0.65 ms
  Max:         4.44 ms

--- Vector Search Benchmark (filtered: username=SenatorShaheen, ~2.4% selectivity) ---
Queries: 100, Top-K: 10, Threads: 8

Filtered vector search (username=SenatorShaheen) results (100 queries, 8 threads):
  Throughput:  972.4 queries/sec
  Avg:         6.01 ms
  P50:         5.43 ms
  P95:         11.71 ms
  P99:         14.01 ms
  Min:         1.33 ms
  Max:         14.01 ms

--- Vector Search Benchmark (filtered: party=Democrat, ~50% selectivity) ---
Queries: 100, Top-K: 10, Threads: 8

Filtered vector search (party=Democrat) results (100 queries, 8 threads):
  Throughput:  5122.4 queries/sec
  Avg:         0.91 ms
  P50:         0.83 ms
  P95:         1.54 ms
  P99:         2.09 ms
  Min:         0.53 ms
  Max:         2.09 ms

Benchmark complete.
```

---

### vector hnm — H&M Fashion Products (2048-dim)

Source: https://github.com/qdrant/ann-filtering-benchmark-datasets

Download this file and unpack: https://storage.googleapis.com/ann-filtered-benchmark/datasets/hnm.tgz

Requires `payloads.jsonl`, `vectors.npy`, and optionally `tests.jsonl`. Pass the directory
containing these files via `--data-dir`.

```bash
java -jar target/kronotop-benchmark-*.jar vector hnm \
    --data-dir /path/to/hnm-data \
    --host localhost \
    --port 5484
```

```bash
java -jar kronotop-benchmark/target/kronotop-benchmark-2026.06-3.jar vector hnm --threads 8 --search-rounds 100 --top-k 10 --data-dir ~/Downloads/hnm
```

Results on Apple Silicon M4 Pro with 48GB of RAM:

```bash
java -jar kronotop-benchmark/target/kronotop-benchmark-2026.06-3.jar vector hnm --threads 8 --search-rounds 100 --top-k 10 --skip-load
=== Kronotop Vector Search Benchmark (H&M) ===
Host: localhost:5484
Bucket: hnm-products
Batch size: 50
Max docs: all
Search rounds: 100
Top-K: 10
Threads: 8

Skipping data load phase.
No data directory specified. Generating random query vectors.

--- Vector Search Benchmark (unfiltered) ---
Queries: 100, Top-K: 10, Threads: 8
00:03:32.179 [virtual-32] WARN  i.n.r.d.DnsServerAddressStreamProviders - Can not find io.netty.resolver.dns.macos.MacOSDnsServerAddressStreamProvider in the classpath, fallback to system defaults. This may result in incorrect DNS resolutions on MacOS. Check whether you have a dependency on 'io.netty:netty-resolver-dns-native-macos'

Unfiltered vector search results (100 queries, 8 threads):
  Throughput:  1380.2 queries/sec
  Avg:         4.55 ms
  P50:         4.27 ms
  P95:         6.39 ms
  P99:         8.54 ms
  Min:         3.26 ms
  Max:         8.54 ms

--- Vector Search Benchmark (filtered: product_group_name=Garment Upper body, ~40% selectivity) ---
Queries: 100, Top-K: 10, Threads: 8

Filtered vector search (product_group_name=Garment Upper body) results (100 queries, 8 threads):
  Throughput:  773.3 queries/sec
  Avg:         8.26 ms
  P50:         6.21 ms
  P95:         20.20 ms
  P99:         31.73 ms
  Min:         3.42 ms
  Max:         31.73 ms

--- Vector Search Benchmark (filtered: colour_group_name=Black, ~21% selectivity) ---
Queries: 100, Top-K: 10, Threads: 8

Filtered vector search (colour_group_name=Black) results (100 queries, 8 threads):
  Throughput:  1062.2 queries/sec
  Avg:         5.39 ms
  P50:         4.99 ms
  P95:         8.22 ms
  P99:         10.31 ms
  Min:         3.54 ms
  Max:         10.31 ms

--- Vector Search Benchmark (filtered: index_group_name=Ladieswear, specific category) ---
Queries: 100, Top-K: 10, Threads: 8

Filtered vector search (index_group_name=Ladieswear) results (100 queries, 8 threads):
  Throughput:  1345.3 queries/sec
  Avg:         4.75 ms
  P50:         4.47 ms
  P95:         6.69 ms
  P99:         8.09 ms
  Min:         3.48 ms
  Max:         8.09 ms

Benchmark complete.
```
