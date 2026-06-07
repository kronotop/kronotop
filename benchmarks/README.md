# Kronotop Benchmarks

## Methodology

These benchmarks do not use bulk reads or writes. Every operation is issued on its own, and each request runs as a
single FoundationDB transaction. The client does not pipeline requests. The goal is to approximate real OLTP workloads,
where each request is an independent transaction rather than a batched or amortized operation.

## Deployment

- [Benchmark Deployment](benchmark-deployment.md) — Three-host EC2 setup with FoundationDB, Kronotop, and benchmark
  client.

## Results

- [Bucket Query with Load](results/bucket-query-with-load.md) — Query latency across five scenarios: full scan,
  equality, range, compound index, and sorted limit.
- [Bucket Mixed with Load](results/bucket-mixed-with-load.md) — Mixed workload with concurrent queries, inserts,
  updates, and deletes.
- [Bucket Vector — H&M](results/bucket-vector-hnm.md) — Vector similarity search on H&M product embeddings.
- [Bucket Vector — Senator Tweets](results/bucket-vector-senator-tweets.md) — Vector similarity search on senator tweet
  embeddings.
