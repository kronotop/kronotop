/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.benchmark.bucket;

import com.kronotop.benchmark.BenchmarkRunner;
import com.kronotop.benchmark.BenchmarkRunner.BenchmarkResult;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * BUCKET.INSERT throughput benchmark. Measures docs/sec and per-batch latency percentiles
 * across configurable batch sizes, with optional comparison against a single-field-indexed bucket.
 * <p>
 * Usage: kronotop-benchmark bucket insert [options]
 * <p>
 * Options:
 * --host          Kronotop host (default: localhost)
 * --port          Kronotop port (default: 5484)
 * --bucket NAME   Bucket name (default: bucket-benchmark)
 * --threads N     Concurrent insert threads (default: 1)
 * --total-docs N  Total documents to insert per scenario (default: 10000)
 * --batch-sizes   Comma-separated batch sizes (default: 1,10,50,100)
 * --warmup-docs N Warmup document count (default: 100)
 * --indexed       Also run with a single-field index on "category"
 * --output-dir PATH  Write per-scenario latency CSVs to PATH (omit to disable)
 */
public class InsertBenchmark extends AbstractBucketBenchmark {
    private static final String BUCKET_NAME_DEFAULT = "bucket-benchmark";
    private static final String INDEX_SCHEMA = """
            {"category": {"bson_type": "string"}}
            """;

    private final String host;
    private final int port;
    private final String bucketName;
    private final int threads;
    private final int totalDocs;
    private final int[] batchSizes;
    private final int warmupDocs;
    private final boolean indexed;
    private final Path outputDir;

    public InsertBenchmark(String host, int port, String bucketName, int threads,
                           int totalDocs, int[] batchSizes, int warmupDocs, boolean indexed,
                           Path outputDir) {
        this.host = host;
        this.port = port;
        this.bucketName = bucketName;
        this.threads = threads;
        this.totalDocs = totalDocs;
        this.batchSizes = batchSizes;
        this.warmupDocs = warmupDocs;
        this.indexed = indexed;
        this.outputDir = outputDir;
    }

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 5484;
        String bucketName = BUCKET_NAME_DEFAULT;
        int threads = 1;
        int totalDocs = 10000;
        int[] batchSizes = {1, 10, 50, 100};
        int warmupDocs = 100;
        boolean indexed = false;
        Path outputDir = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> host = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--bucket" -> bucketName = args[++i];
                case "--threads" -> threads = Integer.parseInt(args[++i]);
                case "--total-docs" -> totalDocs = Integer.parseInt(args[++i]);
                case "--batch-sizes" -> batchSizes = Arrays.stream(args[++i].split(","))
                        .mapToInt(Integer::parseInt).toArray();
                case "--warmup-docs" -> warmupDocs = Integer.parseInt(args[++i]);
                case "--indexed" -> indexed = true;
                case "--output-dir" -> outputDir = Path.of(args[++i]);
                default -> {
                    System.err.println("Unknown option: " + args[i]);
                    System.exit(1);
                }
            }
        }

        new InsertBenchmark(host, port, bucketName, threads, totalDocs, batchSizes, warmupDocs, indexed, outputDir).run();
    }

    public void run() throws Exception {
        System.out.println("=== Kronotop Bucket Insert Benchmark ===");
        System.out.printf("Host: %s:%d%n", host, port);
        System.out.printf("Bucket: %s%n", bucketName);
        System.out.printf("Threads: %d%n", threads);
        System.out.printf("Total docs per scenario: %,d%n", totalDocs);
        System.out.printf("Batch sizes: %s%n", Arrays.toString(batchSizes));
        System.out.printf("Indexed: %s%n", indexed);
        System.out.printf("Output dir: %s%n", outputDir != null ? outputDir : "(disabled)");
        if (indexed) {
            System.out.printf("Index schema: %s%n", INDEX_SCHEMA.strip());
        }
        System.out.printf("Sample document: %s%n%n", sampleDocument().toJson());

        List<byte[]> pool = generatePool(totalDocs);

        RedisClient adminClient = RedisClient.create(String.format("redis://%s:%d", host, port));
        try (StatefulRedisConnection<String, String> adminConn = adminClient.connect()) {
            BucketCommandBuilder<String, String> adminCmd = new BucketCommandBuilder<>(StringCodec.UTF8);

            for (int batchSize : batchSizes) {
                List<List<byte[]>> batches = partitionIntoBatches(pool, batchSize);
                int totalOps = batches.size();
                int warmupOps = Math.max(1, warmupDocs / batchSize);

                resetBucket(adminConn, adminCmd, false);
                BenchmarkResult result = BenchmarkRunner.run(
                        host, port, ByteArrayCodec.INSTANCE, threads, totalOps, warmupOps,
                        this::setupWorkerSession,
                        (conn, cmd, i) -> BenchmarkRunner.dispatch(conn,
                                cmd.insert(bucketName, batches.get(i % batches.size())))
                );
                BenchmarkRunner.reportLatencyStats("UNINDEXED batch=" + batchSize, result, threads);
                if (outputDir != null)
                    BenchmarkRunner.writeLatencies("UNINDEXED batch=" + batchSize, result, threads, outputDir);

                if (indexed) {
                    resetBucket(adminConn, adminCmd, true);
                    BenchmarkResult indexedResult = BenchmarkRunner.run(
                            host, port, ByteArrayCodec.INSTANCE, threads, totalOps, warmupOps,
                            this::setupWorkerSession,
                            (conn, cmd, i) -> BenchmarkRunner.dispatch(conn,
                                    cmd.insert(bucketName, batches.get(i % batches.size())))
                    );
                    BenchmarkRunner.reportLatencyStats("INDEXED   batch=" + batchSize, indexedResult, threads);
                    if (outputDir != null)
                        BenchmarkRunner.writeLatencies("INDEXED   batch=" + batchSize, indexedResult, threads, outputDir);
                }
            }
        } finally {
            adminClient.shutdown();
        }

        System.out.println("\nBenchmark complete.");
    }

    private void resetBucket(StatefulRedisConnection<String, String> conn,
                             BucketCommandBuilder<String, String> cmd,
                             boolean withIndex) throws Exception {
        try {
            BenchmarkRunner.dispatch(conn, cmd.remove(bucketName));
        } catch (Exception ignored) {
        }
        try {
            BenchmarkRunner.dispatch(conn, cmd.purge(bucketName));
        } catch (Exception ignored) {
        }
        if (withIndex) {
            BenchmarkRunner.dispatch(conn, cmd.create(bucketName, BucketCreateArgs.Builder.indexes(INDEX_SCHEMA)));
        } else {
            BenchmarkRunner.dispatch(conn, cmd.create(bucketName));
        }
    }
}
