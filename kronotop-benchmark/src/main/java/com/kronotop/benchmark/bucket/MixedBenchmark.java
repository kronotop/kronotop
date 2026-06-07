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
import org.bson.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Mixed workload bucket benchmark. Runs QUERY, INSERT, UPDATE, and DELETE operations
 * concurrently with configurable per-operation percentages. Each operation type's latencies
 * are tracked independently and optionally written to separate CSV files.
 * <p>
 * Usage: kronotop-benchmark bucket mixed [options]
 * <p>
 * Options:
 * --host              Kronotop host (default: localhost)
 * --port              Kronotop port (default: 5484)
 * --bucket NAME       Bucket name (default: mixed-benchmark)
 * --threads N         Concurrent worker threads (default: 4)
 * --total-ops N       Total operations across all types (default: 10000)
 * --preload-docs N    Documents to insert before benchmark phase (default: 50000)
 * --warmup N          Warmup operations per thread before measurement (default: 200)
 * --query-pct N       Percentage of QUERY operations (default: 70)
 * --insert-pct N      Percentage of INSERT operations (default: 15)
 * --update-pct N      Percentage of UPDATE operations (default: 10)
 * --delete-pct N      Percentage of DELETE operations (default: 5)
 * --delete-limit N    LIMIT per DELETE operation (default: 5)
 * --update-limit N    LIMIT per UPDATE operation (default: 5)
 * --skip-load         Skip the preload phase (assume bucket already populated)
 * --output-dir PATH   Write per-operation latency CSVs to PATH (omit to disable)
 */
public class MixedBenchmark extends AbstractBucketBenchmark {
    private static final String BUCKET_NAME_DEFAULT = "mixed-benchmark";
    private static final String INDEX_SCHEMA = """
            {
              "category": {"bson_type": "string"},
              "age":      {"bson_type": "int32"},
              "score":    {"bson_type": "double"},
              "$compound": [
                {
                  "name": "idx_category_age",
                  "fields": [
                    {"selector": "category", "bson_type": "string"},
                    {"selector": "age",      "bson_type": "int32"}
                  ]
                }
              ]
            }
            """;

    // QUERY — EQ_CATEGORY, 5 variants, LIMIT 100
    private static final byte[][] Q_EQ_CATEGORY = {
            serializeBson(new BsonDocument("category", new BsonString("electronics"))),
            serializeBson(new BsonDocument("category", new BsonString("clothing"))),
            serializeBson(new BsonDocument("category", new BsonString("books"))),
            serializeBson(new BsonDocument("category", new BsonString("sports"))),
            serializeBson(new BsonDocument("category", new BsonString("food"))),
    };

    // DELETE — 5 category variants: {category: X, age: {$lt: 25}}
    // Category partitioning spreads threads across independent document sets, reducing write-write conflicts.
    private static final byte[][] Q_DELETE_VARIANTS = {
            serializeBson(new BsonDocument().append("category", new BsonString("electronics"))
                    .append("age", new BsonDocument("$lt", new BsonInt32(25)))),
            serializeBson(new BsonDocument().append("category", new BsonString("clothing"))
                    .append("age", new BsonDocument("$lt", new BsonInt32(25)))),
            serializeBson(new BsonDocument().append("category", new BsonString("books"))
                    .append("age", new BsonDocument("$lt", new BsonInt32(25)))),
            serializeBson(new BsonDocument().append("category", new BsonString("sports"))
                    .append("age", new BsonDocument("$lt", new BsonInt32(25)))),
            serializeBson(new BsonDocument().append("category", new BsonString("food"))
                    .append("age", new BsonDocument("$lt", new BsonInt32(25)))),
    };

    // UPDATE filter — 5 category variants: {category: X, active: false}
    private static final byte[][] Q_UPDATE_FILTER_VARIANTS = {
            serializeBson(new BsonDocument().append("category", new BsonString("electronics"))
                    .append("active", new BsonBoolean(false))),
            serializeBson(new BsonDocument().append("category", new BsonString("clothing"))
                    .append("active", new BsonBoolean(false))),
            serializeBson(new BsonDocument().append("category", new BsonString("books"))
                    .append("active", new BsonBoolean(false))),
            serializeBson(new BsonDocument().append("category", new BsonString("sports"))
                    .append("active", new BsonBoolean(false))),
            serializeBson(new BsonDocument().append("category", new BsonString("food"))
                    .append("active", new BsonBoolean(false))),
    };

    // UPDATE — 5 variants: reactivate inactive docs with different score values
    private static final byte[][] Q_UPDATE_DOCS = {
            serializeBson(new BsonDocument("$set",
                    new BsonDocument().append("active", new BsonBoolean(true)).append("score", new BsonDouble(95.0)))),
            serializeBson(new BsonDocument("$set",
                    new BsonDocument().append("active", new BsonBoolean(true)).append("score", new BsonDouble(72.5)))),
            serializeBson(new BsonDocument("$set",
                    new BsonDocument().append("active", new BsonBoolean(true)).append("score", new BsonDouble(50.0)))),
            serializeBson(new BsonDocument("$set",
                    new BsonDocument().append("active", new BsonBoolean(true)).append("score", new BsonDouble(88.8)))),
            serializeBson(new BsonDocument("$set",
                    new BsonDocument().append("active", new BsonBoolean(true)).append("score", new BsonDouble(33.3)))),
    };

    private final String host;
    private final int port;
    private final String bucketName;
    private final int threads;
    private final int totalOps;
    private final int preloadDocs;
    private final int warmupPerThread;
    private final int queryPct;
    private final int insertPct;
    private final int updatePct;
    private final int deletePct;
    private final int deleteLimit;
    private final int updateLimit;
    private final boolean skipLoad;
    private final Path outputDir;

    public MixedBenchmark(String host, int port, String bucketName, int threads,
                          int totalOps, int preloadDocs, int warmupPerThread,
                          int queryPct, int insertPct, int updatePct, int deletePct,
                          int deleteLimit, int updateLimit,
                          boolean skipLoad, Path outputDir) {
        this.host = host;
        this.port = port;
        this.bucketName = bucketName;
        this.threads = threads;
        this.totalOps = totalOps;
        this.preloadDocs = preloadDocs;
        this.warmupPerThread = warmupPerThread;
        this.queryPct = queryPct;
        this.insertPct = insertPct;
        this.updatePct = updatePct;
        this.deletePct = deletePct;
        this.deleteLimit = deleteLimit;
        this.updateLimit = updateLimit;
        this.skipLoad = skipLoad;
        this.outputDir = outputDir;
    }

    private static void printOpLine(String label, long[] lat, int count, int total) {
        if (count == 0) {
            System.out.printf("    %-7s %6d ops (%5.1f%%)  (no data)%n", label, 0, 0.0);
            return;
        }
        long[] sorted = Arrays.copyOf(lat, count);
        Arrays.sort(sorted);
        double avgMs = Arrays.stream(sorted).average().orElse(0) / 1_000_000.0;
        double p50Ms = sorted[(int) Math.ceil(count * 0.50) - 1] / 1_000_000.0;
        double p95Ms = sorted[(int) Math.ceil(count * 0.95) - 1] / 1_000_000.0;
        double p99Ms = sorted[(int) Math.ceil(count * 0.99) - 1] / 1_000_000.0;
        double minMs = sorted[0] / 1_000_000.0;
        double maxMs = sorted[count - 1] / 1_000_000.0;
        double pct = (count * 100.0) / total;
        System.out.printf("    %-7s %6d ops (%5.1f%%)  Avg: %6.2f ms  P50: %6.2f ms  P95: %6.2f ms  P99: %6.2f ms  Min: %6.2f ms  Max: %6.2f ms%n",
                label, count, pct, avgMs, p50Ms, p95Ms, p99Ms, minMs, maxMs);
    }

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 5484;
        String bucketName = BUCKET_NAME_DEFAULT;
        int threads = 4;
        int totalOps = 10000;
        int preloadDocs = 50000;
        int warmupPerThread = 200;
        int queryPct = 70;
        int insertPct = 15;
        int updatePct = 10;
        int deletePct = 5;
        int deleteLimit = 5;
        int updateLimit = 5;
        boolean skipLoad = false;
        Path outputDir = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> host = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--bucket" -> bucketName = args[++i];
                case "--threads" -> threads = Integer.parseInt(args[++i]);
                case "--total-ops" -> totalOps = Integer.parseInt(args[++i]);
                case "--preload-docs" -> preloadDocs = Integer.parseInt(args[++i]);
                case "--warmup" -> warmupPerThread = Integer.parseInt(args[++i]);
                case "--query-pct" -> queryPct = Integer.parseInt(args[++i]);
                case "--insert-pct" -> insertPct = Integer.parseInt(args[++i]);
                case "--update-pct" -> updatePct = Integer.parseInt(args[++i]);
                case "--delete-pct" -> deletePct = Integer.parseInt(args[++i]);
                case "--delete-limit" -> deleteLimit = Integer.parseInt(args[++i]);
                case "--update-limit" -> updateLimit = Integer.parseInt(args[++i]);
                case "--skip-load" -> skipLoad = true;
                case "--output-dir" -> outputDir = Path.of(args[++i]);
                default -> {
                    System.err.println("Unknown option: " + args[i]);
                    System.exit(1);
                }
            }
        }

        if (queryPct + insertPct + updatePct + deletePct != 100) {
            System.err.printf("Error: --query-pct (%d) + --insert-pct (%d) + --update-pct (%d) + --delete-pct (%d) must equal 100, got %d%n",
                    queryPct, insertPct, updatePct, deletePct,
                    queryPct + insertPct + updatePct + deletePct);
            System.exit(1);
        }

        new MixedBenchmark(host, port, bucketName, threads, totalOps, preloadDocs,
                warmupPerThread, queryPct, insertPct, updatePct, deletePct,
                deleteLimit, updateLimit, skipLoad, outputDir).run();
    }

    public void run() throws Exception {
        System.out.println("=== Kronotop Bucket Mixed Benchmark ===");
        System.out.printf("Host: %s:%d%n", host, port);
        System.out.printf("Bucket: %s%n", bucketName);
        System.out.printf("Threads: %d%n", threads);
        System.out.printf("Total ops: %,d%n", totalOps);
        System.out.printf("Preload docs: %,d%n", preloadDocs);
        System.out.printf("Warmup per thread: %d%n", warmupPerThread);
        System.out.printf("Op mix: QUERY=%d%%  INSERT=%d%%  UPDATE=%d%%  DELETE=%d%%%n",
                queryPct, insertPct, updatePct, deletePct);
        System.out.printf("Delete limit: %d  Update limit: %d%n", deleteLimit, updateLimit);
        System.out.printf("Skip load: %s%n", skipLoad);
        System.out.printf("Output dir: %s%n%n", outputDir != null ? outputDir : "(disabled)");

        List<byte[]> insertPool = generatePool(preloadDocs);

        if (!skipLoad) {
            loadData(insertPool);
        }

        System.out.println("\n=== Mixed Benchmark Phase ===");
        MixedResult mixed = runMixed(insertPool);

        report(mixed);

        if (outputDir != null) {
            writeCSVs(mixed);
        }

        System.out.println("\nBenchmark complete.");
    }

    private void loadData(List<byte[]> pool) throws Exception {
        RedisClient adminClient = RedisClient.create(String.format("redis://%s:%d", host, port));
        try (StatefulRedisConnection<String, String> adminConn = adminClient.connect()) {
            BucketCommandBuilder<String, String> adminCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            try {
                BenchmarkRunner.dispatch(adminConn, adminCmd.remove(bucketName));
            } catch (Exception ignored) {
            }
            try {
                BenchmarkRunner.dispatch(adminConn, adminCmd.purge(bucketName));
            } catch (Exception ignored) {
            }
            BenchmarkRunner.dispatch(adminConn, adminCmd.create(bucketName, BucketCreateArgs.Builder.indexes(INDEX_SCHEMA)));
        } finally {
            adminClient.shutdown();
        }

        bulkInsert(partitionIntoBatches(pool, 100), bucketName, host, port);
    }

    private MixedResult runMixed(List<byte[]> insertPool) throws Exception {
        int effectiveThreads = Math.min(threads, totalOps);
        int opsPerThread = totalOps / effectiveThreads;

        final int queryThreshold = queryPct;
        final int insertThreshold = queryPct + insertPct;
        final int updateThreshold = queryPct + insertPct + updatePct;

        BucketQueryArgs queryArgs = new BucketQueryArgs().limit(100);
        BucketQueryArgs deleteArgs = new BucketQueryArgs().limit(deleteLimit);
        BucketQueryArgs updateArgs = new BucketQueryArgs().limit(updateLimit);

        CountDownLatch readyLatch = new CountDownLatch(effectiveThreads);
        CountDownLatch startLatch = new CountDownLatch(1);

        @SuppressWarnings("unchecked")
        Future<ThreadResult>[] futures = new Future[effectiveThreads];

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int t = 0; t < effectiveThreads; t++) {
                final int threadId = t;
                final int count = (threadId == effectiveThreads - 1)
                        ? totalOps - (opsPerThread * (effectiveThreads - 1))
                        : opsPerThread;
                final int startIndex = threadId * opsPerThread;

                futures[t] = executor.submit(() -> {
                    RedisClient threadClient = RedisClient.create(
                            String.format("redis://%s:%d", host, port));
                    try (StatefulRedisConnection<byte[], byte[]> conn =
                                 threadClient.connect(ByteArrayCodec.INSTANCE)) {

                        BucketCommandBuilder<byte[], byte[]> cmd =
                                new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
                        setupWorkerSession(conn);

                        Random rng = new Random(threadId * 31L + 17L);

                        // Warmup
                        try {
                            for (int i = 0; i < warmupPerThread; i++) {
                                int roll = rng.nextInt(100);
                                int idx = startIndex + (i % count);
                                try {
                                    if (roll < queryThreshold) {
                                        BenchmarkRunner.dispatch(conn,
                                                cmd.query(bucketName, Q_EQ_CATEGORY[idx % Q_EQ_CATEGORY.length], queryArgs));
                                    } else if (roll < insertThreshold) {
                                        BenchmarkRunner.dispatch(conn,
                                                cmd.insert(bucketName, insertPool.get(idx % insertPool.size())));
                                    } else if (roll < updateThreshold) {
                                        BenchmarkRunner.dispatch(conn,
                                                cmd.update(bucketName, Q_UPDATE_FILTER_VARIANTS[i % Q_UPDATE_FILTER_VARIANTS.length], Q_UPDATE_DOCS[i % Q_UPDATE_DOCS.length], updateArgs));
                                    } else {
                                        BenchmarkRunner.dispatch(conn,
                                                cmd.delete(bucketName, Q_DELETE_VARIANTS[i % Q_DELETE_VARIANTS.length], deleteArgs));
                                    }
                                } catch (Exception e) {
                                    if (!e.getMessage().contains("NOT_COMMITTED")) throw e;
                                }
                            }
                        } finally {
                            readyLatch.countDown();
                        }
                        startLatch.await();

                        // Measurement
                        long[] qLat = new long[count];
                        long[] iLat = new long[count];
                        long[] uLat = new long[count];
                        long[] dLat = new long[count];
                        int qCount = 0, iCount = 0, uCount = 0, dCount = 0, conflictCount = 0;

                        for (int i = 0; i < count; i++) {
                            int roll = rng.nextInt(100);
                            int queryIdx = startIndex + i;
                            long t0 = System.nanoTime();
                            try {
                                if (roll < queryThreshold) {
                                    BenchmarkRunner.dispatch(conn,
                                            cmd.query(bucketName, Q_EQ_CATEGORY[queryIdx % Q_EQ_CATEGORY.length], queryArgs));
                                    qLat[qCount++] = System.nanoTime() - t0;
                                } else if (roll < insertThreshold) {
                                    BenchmarkRunner.dispatch(conn,
                                            cmd.insert(bucketName, insertPool.get(queryIdx % insertPool.size())));
                                    iLat[iCount++] = System.nanoTime() - t0;
                                } else if (roll < updateThreshold) {
                                    BenchmarkRunner.dispatch(conn,
                                            cmd.update(bucketName, Q_UPDATE_FILTER_VARIANTS[i % Q_UPDATE_FILTER_VARIANTS.length], Q_UPDATE_DOCS[i % Q_UPDATE_DOCS.length], updateArgs));
                                    uLat[uCount++] = System.nanoTime() - t0;
                                } else {
                                    BenchmarkRunner.dispatch(conn,
                                            cmd.delete(bucketName, Q_DELETE_VARIANTS[i % Q_DELETE_VARIANTS.length], deleteArgs));
                                    dLat[dCount++] = System.nanoTime() - t0;
                                }
                            } catch (Exception e) {
                                if (!e.getMessage().contains("NOT_COMMITTED")) throw e;
                                conflictCount++;
                            }
                        }

                        return new ThreadResult(qLat, qCount, iLat, iCount, uLat, uCount, dLat, dCount, conflictCount);
                    } finally {
                        threadClient.shutdown();
                    }
                });
            }

            readyLatch.await();
            long wallStart = System.nanoTime();
            startLatch.countDown();

            List<ThreadResult> results = new ArrayList<>(effectiveThreads);
            for (Future<ThreadResult> f : futures) {
                results.add(f.get());
            }
            long wallEnd = System.nanoTime();

            return aggregate(results, wallEnd - wallStart);
        }
    }

    private MixedResult aggregate(List<ThreadResult> results, long wallClockNanos) {
        int totalQ = 0, totalI = 0, totalU = 0, totalD = 0, totalConflicts = 0;
        for (ThreadResult r : results) {
            totalQ += r.qCount();
            totalI += r.iCount();
            totalU += r.uCount();
            totalD += r.dCount();
            totalConflicts += r.conflictCount();
        }

        long[] qAll = new long[totalQ];
        long[] iAll = new long[totalI];
        long[] uAll = new long[totalU];
        long[] dAll = new long[totalD];

        int qOff = 0, iOff = 0, uOff = 0, dOff = 0;
        for (ThreadResult r : results) {
            System.arraycopy(r.qLat(), 0, qAll, qOff, r.qCount());
            qOff += r.qCount();
            System.arraycopy(r.iLat(), 0, iAll, iOff, r.iCount());
            iOff += r.iCount();
            System.arraycopy(r.uLat(), 0, uAll, uOff, r.uCount());
            uOff += r.uCount();
            System.arraycopy(r.dLat(), 0, dAll, dOff, r.dCount());
            dOff += r.dCount();
        }

        return new MixedResult(qAll, totalQ, iAll, totalI, uAll, totalU, dAll, totalD, totalConflicts, wallClockNanos);
    }

    private void report(MixedResult mixed) {
        int effectiveThreads = Math.min(threads, totalOps);
        int totalOpsExecuted = mixed.qCount() + mixed.iCount() + mixed.uCount() + mixed.dCount();

        long[] allLat = new long[totalOpsExecuted];
        int off = 0;
        System.arraycopy(mixed.qLat(), 0, allLat, off, mixed.qCount());
        off += mixed.qCount();
        System.arraycopy(mixed.iLat(), 0, allLat, off, mixed.iCount());
        off += mixed.iCount();
        System.arraycopy(mixed.uLat(), 0, allLat, off, mixed.uCount());
        off += mixed.uCount();
        System.arraycopy(mixed.dLat(), 0, allLat, off, mixed.dCount());

        BenchmarkResult overall = new BenchmarkResult(allLat, totalOpsExecuted, mixed.wallClockNanos());
        BenchmarkRunner.reportLatencyStats("MIXED", overall, effectiveThreads);

        System.out.println("\n  Operation breakdown:");
        printOpLine("QUERY", mixed.qLat(), mixed.qCount(), totalOpsExecuted);
        printOpLine("INSERT", mixed.iLat(), mixed.iCount(), totalOpsExecuted);
        printOpLine("UPDATE", mixed.uLat(), mixed.uCount(), totalOpsExecuted);
        printOpLine("DELETE", mixed.dLat(), mixed.dCount(), totalOpsExecuted);
        if (mixed.conflictCount() > 0) {
            System.out.printf("    CONFLICT %5d skipped (%5.1f%% of attempts)%n",
                    mixed.conflictCount(),
                    mixed.conflictCount() * 100.0 / (totalOpsExecuted + mixed.conflictCount()));
        }
    }

    private void writeCSVs(MixedResult mixed) throws IOException {
        int effectiveThreads = Math.min(threads, totalOps);
        long w = mixed.wallClockNanos();
        BenchmarkRunner.writeLatencies("mixed_query",
                new BenchmarkResult(mixed.qLat(), mixed.qCount(), w), effectiveThreads, outputDir);
        BenchmarkRunner.writeLatencies("mixed_insert",
                new BenchmarkResult(mixed.iLat(), mixed.iCount(), w), effectiveThreads, outputDir);
        BenchmarkRunner.writeLatencies("mixed_update",
                new BenchmarkResult(mixed.uLat(), mixed.uCount(), w), effectiveThreads, outputDir);
        BenchmarkRunner.writeLatencies("mixed_delete",
                new BenchmarkResult(mixed.dLat(), mixed.dCount(), w), effectiveThreads, outputDir);
    }

    private record ThreadResult(
            long[] qLat, int qCount,
            long[] iLat, int iCount,
            long[] uLat, int uCount,
            long[] dLat, int dCount,
            int conflictCount
    ) {
    }

    private record MixedResult(
            long[] qLat, int qCount,
            long[] iLat, int iCount,
            long[] uLat, int uCount,
            long[] dLat, int dCount,
            int conflictCount,
            long wallClockNanos
    ) {
    }
}
