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
import com.kronotop.commands.BucketQueryArgs;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import java.nio.file.Path;
import java.util.List;

/**
 * BUCKET.QUERY throughput benchmark. Measures QPS and latency percentiles across five query scenarios:
 * full scan, equality lookup, range scan, compound filter, and sorted top-K. Each scenario rotates
 * through multiple BSON query variants to avoid artificial FDB page cache warmth.
 * <p>
 * FULL_SCAN filters on {@code active} (boolean), which is not indexed. Every query reads all documents
 * and applies the predicate as a post-filter, making it the worst-case scan scenario.
 * <p>
 * Usage: kronotop-benchmark bucket query [options]
 * <p>
 * Options:
 * --host          Kronotop host (default: localhost)
 * --port          Kronotop port (default: 5484)
 * --bucket NAME   Bucket name (default: query-benchmark)
 * --threads N     Concurrent query threads (default: 1)
 * --total-docs N  Documents to insert during load phase (default: 50000)
 * --queries N     Total queries per scenario, distributed across all threads (default: 1000)
 * --warmup N      Warmup rounds per thread before measurement; effective total = N × threads (default: 100)
 * --skip-load     Skip the load phase (assume bucket already populated)
 * --output-dir PATH  Write per-scenario latency CSVs to PATH (omit to disable)
 */
public class QueryBenchmark extends AbstractBucketBenchmark {
    private static final String BUCKET_NAME_DEFAULT = "query-benchmark";
    private static final String INDEX_SCHEMA = """
            {
              "category": {"bson_type": "string"},
              "age": {"bson_type": "int32"},
              "score": {"bson_type": "double"},
              "$compound": [
                {
                  "name": "idx_category_age",
                  "fields": [
                    {"selector": "category", "bson_type": "string"},
                    {"selector": "age", "bson_type": "int32"}
                  ]
                },
                {
                  "name": "idx_category_score",
                  "fields": [
                    {"selector": "category", "bson_type": "string"},
                    {"selector": "score", "bson_type": "double"}
                  ]
                }
              ]
            }
            """;

    // Scenario 1 — full scan (active not indexed)
    private static final byte[][] Q_FULL_SCAN = {
            serializeBson(new BsonDocument("active", new BsonBoolean(true))),
            serializeBson(new BsonDocument("active", new BsonBoolean(false))),
    };

    // Scenario 2 — equality on indexed field
    private static final byte[][] Q_EQ_CATEGORY = {
            serializeBson(new BsonDocument("category", new BsonString("electronics"))),
            serializeBson(new BsonDocument("category", new BsonString("clothing"))),
            serializeBson(new BsonDocument("category", new BsonString("books"))),
            serializeBson(new BsonDocument("category", new BsonString("sports"))),
            serializeBson(new BsonDocument("category", new BsonString("food"))),
    };

    // Scenario 3 — range scan on indexed field
    private static final byte[][] Q_RANGE_AGE = {
            serializeBson(new BsonDocument("age", new BsonDocument("$gt", new BsonInt32(20)))),
            serializeBson(new BsonDocument("age", new BsonDocument("$gt", new BsonInt32(30)))),
            serializeBson(new BsonDocument("age", new BsonDocument("$gt", new BsonInt32(40)))),
            serializeBson(new BsonDocument("age", new BsonDocument("$gt", new BsonInt32(50)))),
            serializeBson(new BsonDocument("age", new BsonDocument("$gt", new BsonInt32(60)))),
    };

    // Scenario 4 — compound filter (category + age)
    private static final byte[][] Q_COMPOUND = {
            serializeBson(new BsonDocument().append("category", new BsonString("electronics")).append("age", new BsonDocument("$gt", new BsonInt32(30)))),
            serializeBson(new BsonDocument().append("category", new BsonString("clothing")).append("age", new BsonDocument("$gt", new BsonInt32(40)))),
            serializeBson(new BsonDocument().append("category", new BsonString("books")).append("age", new BsonDocument("$gt", new BsonInt32(30)))),
            serializeBson(new BsonDocument().append("category", new BsonString("sports")).append("age", new BsonDocument("$gt", new BsonInt32(50)))),
            serializeBson(new BsonDocument().append("category", new BsonString("food")).append("age", new BsonDocument("$gt", new BsonInt32(25)))),
    };

    // Scenario 5 — sorted top-K by score, filtered by category
    private static final byte[][] Q_SORT_LIMIT = {
            serializeBson(new BsonDocument("category", new BsonString("electronics"))),
            serializeBson(new BsonDocument("category", new BsonString("clothing"))),
            serializeBson(new BsonDocument("category", new BsonString("books"))),
            serializeBson(new BsonDocument("category", new BsonString("sports"))),
            serializeBson(new BsonDocument("category", new BsonString("food"))),
    };

    private final String host;
    private final int port;
    private final String bucketName;
    private final int threads;
    private final int totalDocs;
    private final int rounds;
    private final int warmup;
    private final boolean skipLoad;
    private final Path outputDir;

    public QueryBenchmark(String host, int port, String bucketName, int threads,
                          int totalDocs, int rounds, int warmup, boolean skipLoad, Path outputDir) {
        this.host = host;
        this.port = port;
        this.bucketName = bucketName;
        this.threads = threads;
        this.totalDocs = totalDocs;
        this.rounds = rounds;
        this.warmup = warmup;
        this.skipLoad = skipLoad;
        this.outputDir = outputDir;
    }

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 5484;
        String bucketName = BUCKET_NAME_DEFAULT;
        int threads = 1;
        int totalDocs = 50000;
        int rounds = 1000;
        int warmup = 100;
        boolean skipLoad = false;
        Path outputDir = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> host = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--bucket" -> bucketName = args[++i];
                case "--threads" -> threads = Integer.parseInt(args[++i]);
                case "--total-docs" -> totalDocs = Integer.parseInt(args[++i]);
                case "--queries" -> rounds = Integer.parseInt(args[++i]);
                case "--warmup" -> warmup = Integer.parseInt(args[++i]);
                case "--skip-load" -> skipLoad = true;
                case "--output-dir" -> outputDir = Path.of(args[++i]);
                default -> {
                    System.err.println("Unknown option: " + args[i]);
                    System.exit(1);
                }
            }
        }

        new QueryBenchmark(host, port, bucketName, threads, totalDocs, rounds, warmup, skipLoad, outputDir).run();
    }

    public void run() throws Exception {
        System.out.println("=== Kronotop Bucket Query Benchmark ===");
        System.out.printf("Host: %s:%d%n", host, port);
        System.out.printf("Bucket: %s%n", bucketName);
        System.out.printf("Threads: %d%n", threads);
        System.out.printf("Total docs: %,d%n", totalDocs);
        System.out.printf("Queries per scenario: %,d (total across all threads)%n", rounds);
        System.out.printf("Warmup: %d per thread (%,d total)%n", warmup, (long) warmup * threads);
        System.out.printf("Skip load: %s%n", skipLoad);
        System.out.println();
        System.out.println("Indexes:");
        System.out.println("  Single-field: category (string), age (int32), score (double)");
        System.out.println("  Compound:     idx_category_age  (category: string, age: int32)");
        System.out.println("                idx_category_score (category: string, score: double)");

        if (!skipLoad) {
            loadData();
        }

        System.out.println("\n=== Query Phase ===");

        System.out.println("\nRunning FULL_SCAN: {active: true|false}  (active not indexed — full scan with default LIMIT)");
        BenchmarkResult r1 = BenchmarkRunner.run(
                host, port, ByteArrayCodec.INSTANCE, threads, rounds, warmup,
                this::setupWorkerSession,
                (conn, cmd, i) -> BenchmarkRunner.dispatch(conn,
                        cmd.query(bucketName, Q_FULL_SCAN[i % Q_FULL_SCAN.length]))
        );
        BenchmarkRunner.reportLatencyStats("FULL_SCAN", r1, threads);
        if (outputDir != null) BenchmarkRunner.writeLatencies("FULL_SCAN", r1, threads, outputDir);

        System.out.println("\nRunning EQ_CATEGORY: {category: <electronics|clothing|books|sports|food>}  LIMIT 100");
        BenchmarkResult r2 = BenchmarkRunner.run(
                host, port, ByteArrayCodec.INSTANCE, threads, rounds, warmup,
                this::setupWorkerSession,
                (conn, cmd, i) -> BenchmarkRunner.dispatch(conn,
                        cmd.query(bucketName, Q_EQ_CATEGORY[i % Q_EQ_CATEGORY.length], new BucketQueryArgs().limit(100)))
        );
        BenchmarkRunner.reportLatencyStats("EQ_CATEGORY", r2, threads);
        if (outputDir != null) BenchmarkRunner.writeLatencies("EQ_CATEGORY", r2, threads, outputDir);

        System.out.println("\nRunning RANGE_AGE: {age: {$gt: 20|30|40|50|60}}  LIMIT 100");
        BenchmarkResult r3 = BenchmarkRunner.run(
                host, port, ByteArrayCodec.INSTANCE, threads, rounds, warmup,
                this::setupWorkerSession,
                (conn, cmd, i) -> BenchmarkRunner.dispatch(conn,
                        cmd.query(bucketName, Q_RANGE_AGE[i % Q_RANGE_AGE.length], new BucketQueryArgs().limit(100)))
        );
        BenchmarkRunner.reportLatencyStats("RANGE_AGE", r3, threads);
        if (outputDir != null) BenchmarkRunner.writeLatencies("RANGE_AGE", r3, threads, outputDir);

        System.out.println("\nRunning COMPOUND: {category: <electronics|clothing|books|sports|food>, age: {$gt: <30|40|30|50|25>}}  LIMIT 100");
        BenchmarkResult r4 = BenchmarkRunner.run(
                host, port, ByteArrayCodec.INSTANCE, threads, rounds, warmup,
                this::setupWorkerSession,
                (conn, cmd, i) -> BenchmarkRunner.dispatch(conn,
                        cmd.query(bucketName, Q_COMPOUND[i % Q_COMPOUND.length], new BucketQueryArgs().limit(100)))
        );
        BenchmarkRunner.reportLatencyStats("COMPOUND", r4, threads);
        if (outputDir != null) BenchmarkRunner.writeLatencies("COMPOUND", r4, threads, outputDir);

        System.out.println("\nRunning SORT_LIMIT: {category: <electronics|clothing|books|sports|food>}  SORTBY score ASC  LIMIT 10");
        BenchmarkResult r5 = BenchmarkRunner.run(
                host, port, ByteArrayCodec.INSTANCE, threads, rounds, warmup,
                this::setupWorkerSession,
                (conn, cmd, i) -> BenchmarkRunner.dispatch(conn,
                        cmd.query(bucketName, Q_SORT_LIMIT[i % Q_SORT_LIMIT.length],
                                new BucketQueryArgs().sortBy("score", "ASC").limit(10)))
        );
        BenchmarkRunner.reportLatencyStats("SORT_LIMIT", r5, threads);
        if (outputDir != null) BenchmarkRunner.writeLatencies("SORT_LIMIT", r5, threads, outputDir);

        System.out.println("\nBenchmark complete.");
    }

    private void loadData() throws Exception {
        RedisClient adminClient = RedisClient.create(String.format("redis://%s:%d", host, port));
        try (StatefulRedisConnection<String, String> adminConn = adminClient.connect()) {
            BucketCommandBuilder<String, String> adminCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            setupBucket(adminConn, adminCmd);
        } finally {
            adminClient.shutdown();
        }

        List<byte[]> pool = generatePool(totalDocs);
        bulkInsert(partitionIntoBatches(pool, 100), bucketName, host, port);
    }

    private void setupBucket(StatefulRedisConnection<String, String> conn,
                             BucketCommandBuilder<String, String> cmd) throws Exception {
        try {
            BenchmarkRunner.dispatch(conn, cmd.remove(bucketName));
        } catch (Exception ignored) {
        }
        try {
            BenchmarkRunner.dispatch(conn, cmd.purge(bucketName));
        } catch (Exception ignored) {
        }
        BenchmarkRunner.dispatch(conn, cmd.create(bucketName, BucketCreateArgs.Builder.indexes(INDEX_SCHEMA)));
    }
}
