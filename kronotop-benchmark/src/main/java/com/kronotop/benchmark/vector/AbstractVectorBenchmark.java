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

package com.kronotop.benchmark.vector;

import com.kronotop.benchmark.BenchmarkRunner;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketVectorArgs;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.SessionAttributeKeywords;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import tools.jackson.databind.ObjectMapper;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for vector search benchmarks. Implements the common execution flow via the
 * Template Method pattern. Subclasses provide dataset-specific bucket schema, data loading,
 * query vector preparation, and filter scenarios.
 */
abstract class AbstractVectorBenchmark {
    protected static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> POISON_PILL = new ArrayList<>(0);

    protected final String host;
    protected final int port;
    protected final String bucketName;
    protected final int batchSize;
    protected final int maxDocs;
    protected final int searchRounds;
    protected final int topK;
    protected final int threads;
    protected final boolean skipLoad;
    protected final boolean skipFilter;
    protected final Path outputDir;
    protected final List<float[]> queryVectors = new ArrayList<>();

    protected AbstractVectorBenchmark(String host, int port, String bucketName,
                                      int batchSize, int maxDocs, int searchRounds,
                                      int topK, int threads, boolean skipLoad, boolean skipFilter,
                                      Path outputDir) {
        this.host = host;
        this.port = port;
        this.bucketName = bucketName;
        this.batchSize = batchSize;
        this.maxDocs = maxDocs;
        this.searchRounds = searchRounds;
        this.topK = topK;
        this.threads = threads;
        this.skipLoad = skipLoad;
        this.skipFilter = skipFilter;
        this.outputDir = outputDir;
    }

    protected static float[] randomVector(Random rng, int dims) {
        float[] v = new float[dims];
        for (int i = 0; i < dims; i++) {
            v[i] = rng.nextFloat() * 2 - 1;
        }
        return v;
    }

    public final void run() throws Exception {
        System.out.printf("=== Kronotop Vector Search Benchmark (%s) ===%n", datasetLabel());
        System.out.printf("Host: %s:%d%n", host, port);
        System.out.printf("Bucket: %s%n", bucketName);
        System.out.printf("Batch size: %d%n", batchSize);
        System.out.printf("Max docs: %s%n", maxDocs > 0 ? maxDocs : "all");
        System.out.printf("Search rounds: %d%n", searchRounds);
        System.out.printf("Top-K: %d%n", topK);
        System.out.printf("Threads: %d%n%n", threads);

        if (!skipLoad) {
            RedisClient client = RedisClient.create(String.format("redis://%s:%d", host, port));
            try (StatefulRedisConnection<String, String> connection = client.connect()) {
                KronotopCommandBuilder<String, String> kronotopCmd = new KronotopCommandBuilder<>(StringCodec.UTF8);
                BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

                BenchmarkRunner.dispatch(connection, kronotopCmd.sessionAttributeSet(
                        SessionAttributeKeywords.INPUT_TYPE, "JSON"));
                System.out.println("Session configured: INPUT_TYPE=JSON");

                createBucket(connection, cmd);
            } finally {
                client.shutdown();
            }

            int workerCount = Math.max(1, threads);
            BlockingQueue<List<String>> queue = new LinkedBlockingQueue<>(workerCount * 2);
            AtomicInteger totalInserted = new AtomicInteger();
            long loadStart = System.nanoTime();

            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int t = 0; t < workerCount; t++) {
                    executor.submit(() -> {
                        RedisClient wc = RedisClient.create(String.format("redis://%s:%d", host, port));
                        try (StatefulRedisConnection<String, String> conn = wc.connect()) {
                            KronotopCommandBuilder<String, String> workerKronotopCmd =
                                    new KronotopCommandBuilder<>(StringCodec.UTF8);
                            BucketCommandBuilder<String, String> workerCmd =
                                    new BucketCommandBuilder<>(StringCodec.UTF8);
                            BenchmarkRunner.dispatch(conn,
                                    workerKronotopCmd.sessionAttributeSet(SessionAttributeKeywords.INPUT_TYPE, "JSON"));

                            while (true) {
                                List<String> batch = queue.take();
                                if (batch == POISON_PILL) break;
                                insertBatch(conn, workerCmd, batch);
                                int n = totalInserted.addAndGet(batch.size());
                                if (n % (10 * batchSize) < batch.size()) {
                                    long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - loadStart);
                                    System.out.printf("  Inserted %,d docs - %.0f docs/sec%n",
                                            n, n / (ms / 1000.0));
                                }
                            }
                        } finally {
                            wc.shutdown();
                        }
                        return null;
                    });
                }

                try {
                    produceBatches(queue);
                } finally {
                    for (int t = 0; t < workerCount; t++) {
                        queue.put(POISON_PILL);
                    }
                }
            }

            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - loadStart);
            int total = totalInserted.get();
            System.out.printf("Load complete: %,d docs in %.1f sec (%.0f docs/sec)%n",
                    total, ms / 1000.0, total / (ms / 1000.0));
        } else {
            System.out.println("Skipping data load phase.");
        }

        prepareQueryVectors();

        benchmarkVectorSearch();
        if (!skipFilter) {
            runFilteredBenchmarks();
        }

        System.out.println("\nBenchmark complete.");
    }

    protected abstract void createBucket(StatefulRedisConnection<String, String> connection,
                                         BucketCommandBuilder<String, String> cmd) throws Exception;

    protected abstract void produceBatches(BlockingQueue<List<String>> queue) throws Exception;

    protected abstract void prepareQueryVectors() throws Exception;

    protected abstract void runFilteredBenchmarks() throws Exception;

    protected abstract String datasetLabel();

    protected void benchmarkVectorSearch() throws Exception {
        System.out.println("\n--- Vector Search Benchmark (unfiltered) ---");
        int queriesRun = Math.min(searchRounds, queryVectors.size());
        System.out.printf("Queries: %d, Top-K: %d, Threads: %d%n", queriesRun, topK, threads);

        int warmup = Math.clamp(queriesRun / threads, 1, 10);

        BenchmarkRunner.BenchmarkResult result = BenchmarkRunner.run(
                host, port, threads, queriesRun, warmup,
                (conn, cmd, i) -> BenchmarkRunner.dispatch(conn,
                        cmd.vector(bucketName, "embeddings",
                                queryVectors.get(i % queryVectors.size()), BucketVectorArgs.Builder.top(topK)))
        );

        BenchmarkRunner.reportLatencyStats("Unfiltered vector search", result, threads);
        if (outputDir != null) BenchmarkRunner.writeLatencies("UNFILTERED_VECTOR_SEARCH", result, threads, outputDir);
    }

    protected void benchmarkFilteredVectorSearch(String fieldName, String fieldValue,
                                                 String description) throws Exception {
        System.out.printf("%n--- Vector Search Benchmark (filtered: %s=%s, %s) ---%n",
                fieldName, fieldValue, description);
        int queriesRun = Math.min(searchRounds, queryVectors.size());
        System.out.printf("Queries: %d, Top-K: %d, Threads: %d%n", queriesRun, topK, threads);

        String filter = "{\"" + fieldName + "\": \"" + fieldValue + "\"}";
        int warmup = Math.clamp(queriesRun / threads, 1, 10);

        BenchmarkRunner.BenchmarkResult result = BenchmarkRunner.run(
                host, port, threads, queriesRun, warmup,
                (conn, cmd, i) -> BenchmarkRunner.dispatch(conn,
                        cmd.vector(bucketName, "embeddings",
                                queryVectors.get(i % queryVectors.size()), new BucketVectorArgs().top(topK).filter(filter)))
        );

        BenchmarkRunner.reportLatencyStats("Filtered vector search (" + fieldName + "=" + fieldValue + ")",
                result, threads);
        if (outputDir != null)
            BenchmarkRunner.writeLatencies("FILTERED_VECTOR_SEARCH_" + fieldName + "=" + fieldValue, result, threads, outputDir);
    }

    protected void insertBatch(StatefulRedisConnection<String, String> connection,
                               BucketCommandBuilder<String, String> cmd,
                               List<String> documents) throws Exception {
        BenchmarkRunner.dispatch(connection, cmd.insert(bucketName, documents));
    }
}
