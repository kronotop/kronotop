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

package com.kronotop.benchmark;

import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.SessionAttributeKeywords;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.RedisCommand;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * Multi-threaded benchmark execution utility. Each thread creates its own connection
 * for realistic multi-client simulation. Uses virtual threads for lightweight concurrency.
 */
public class BenchmarkRunner {

    private static final DateTimeFormatter TIMESTAMP_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    private static final DateTimeFormatter TIMESTAMP_ISO =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    public static BenchmarkResult run(String host, int port, int threads,
                                      int totalQueries, int warmupPerThread,
                                      QueryFunction<String, String> queryFn) throws Exception {
        return run(host, port, StringCodec.UTF8, threads, totalQueries, warmupPerThread,
                conn -> dispatch(conn, new KronotopCommandBuilder<>(StringCodec.UTF8)
                        .sessionAttributeSet(SessionAttributeKeywords.INPUT_TYPE, "JSON")),
                queryFn);
    }

    public static <K, V> BenchmarkResult run(String host, int port,
                                             RedisCodec<K, V> codec,
                                             int threads, int totalQueries, int warmupPerThread,
                                             ConnectionSetup<K, V> setup,
                                             QueryFunction<K, V> queryFn) throws Exception {
        if (totalQueries == 0) {
            return new BenchmarkResult(new long[0], 0, 0);
        }

        int effectiveThreads = Math.min(threads, totalQueries);
        long[][] perThreadLatencies = new long[effectiveThreads][];
        int[] perThreadCounts = new int[effectiveThreads];

        CountDownLatch readyLatch = new CountDownLatch(effectiveThreads);
        CountDownLatch startLatch = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(effectiveThreads);

            for (int t = 0; t < effectiveThreads; t++) {
                final int threadId = t;
                int queriesPerThread = totalQueries / effectiveThreads;
                int startIndex = threadId * queriesPerThread;
                if (threadId == effectiveThreads - 1) {
                    queriesPerThread = totalQueries - startIndex;
                }
                final int count = queriesPerThread;

                futures.add(executor.submit(() -> {
                    RedisClient threadClient = RedisClient.create(
                            String.format("redis://%s:%d", host, port));
                    try (StatefulRedisConnection<K, V> conn = threadClient.connect(codec)) {
                        BucketCommandBuilder<K, V> cmd = new BucketCommandBuilder<>(codec);

                        setup.setup(conn);

                        try {
                            for (int i = 0; i < warmupPerThread; i++) {
                                int queryIdx = startIndex + (i % count);
                                queryFn.execute(conn, cmd, queryIdx);
                            }
                        } finally {
                            readyLatch.countDown();
                        }
                        startLatch.await();

                        long[] latencies = new long[count];
                        for (int i = 0; i < count; i++) {
                            int queryIdx = startIndex + i;
                            long start = System.nanoTime();
                            queryFn.execute(conn, cmd, queryIdx);
                            latencies[i] = System.nanoTime() - start;
                        }

                        perThreadLatencies[threadId] = latencies;
                        perThreadCounts[threadId] = count;
                    } finally {
                        threadClient.shutdown();
                    }
                    return null;
                }));
            }

            readyLatch.await();

            long wallStart = System.nanoTime();
            startLatch.countDown();

            for (Future<?> f : futures) {
                f.get();
            }
            long wallEnd = System.nanoTime();

            int totalCount = 0;
            for (int c : perThreadCounts) {
                totalCount += c;
            }
            long[] allLatencies = new long[totalCount];
            int offset = 0;
            for (int t = 0; t < effectiveThreads; t++) {
                if (perThreadLatencies[t] != null) {
                    System.arraycopy(perThreadLatencies[t], 0, allLatencies, offset, perThreadCounts[t]);
                    offset += perThreadCounts[t];
                }
            }

            return new BenchmarkResult(allLatencies, totalCount, wallEnd - wallStart);
        }
    }

    public static void reportLatencyStats(String label, BenchmarkResult result, int threads) {
        int count = result.totalQueries();
        if (count == 0) {
            System.out.println("No queries executed.");
            return;
        }

        long[] sorted = Arrays.copyOf(result.allLatencies(), count);
        Arrays.sort(sorted);

        double avgMs = Arrays.stream(sorted).average().orElse(0) / 1_000_000.0;
        double p50Ms = sorted[(int) Math.ceil(count * 0.50) - 1] / 1_000_000.0;
        double p95Ms = sorted[(int) Math.ceil(count * 0.95) - 1] / 1_000_000.0;
        double p99Ms = sorted[(int) Math.ceil(count * 0.99) - 1] / 1_000_000.0;
        double minMs = sorted[0] / 1_000_000.0;
        double maxMs = sorted[count - 1] / 1_000_000.0;
        double qps = count / (result.wallClockNanos() / 1_000_000_000.0);

        System.out.printf("%n%s results (%d queries, %d threads):%n", label, count, threads);
        System.out.printf("  Throughput:  %.1f queries/sec%n", qps);
        System.out.printf("  Avg:         %.2f ms%n", avgMs);
        System.out.printf("  P50:         %.2f ms%n", p50Ms);
        System.out.printf("  P95:         %.2f ms%n", p95Ms);
        System.out.printf("  P99:         %.2f ms%n", p99Ms);
        System.out.printf("  Min:         %.2f ms%n", minMs);
        System.out.printf("  Max:         %.2f ms%n", maxMs);
        System.out.printf("  Duration:    %.2f sec%n", result.wallClockNanos() / 1_000_000_000.0);
    }

    public static void writeLatencies(String scenario, BenchmarkResult result, int threads, Path outputDir)
            throws IOException {
        Files.createDirectories(outputDir);
        LocalDateTime now = LocalDateTime.now();
        String ts = now.format(TIMESTAMP_FMT);
        String filename = scenario.toLowerCase().replaceAll("[^a-z0-9]+", "_") + "_" + ts + ".csv";
        Path file = outputDir.resolve(filename);
        double wallMs = result.wallClockNanos() / 1_000_000.0;
        double qps = result.totalQueries() / (result.wallClockNanos() / 1_000_000_000.0);
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            w.write("# scenario: " + scenario);
            w.newLine();
            w.write("# threads: " + threads);
            w.newLine();
            w.write("# queries: " + result.totalQueries());
            w.newLine();
            w.write(String.format("# wall_clock_ms: %.2f", wallMs));
            w.newLine();
            w.write(String.format("# qps: %.1f", qps));
            w.newLine();
            w.write("# timestamp: " + now.format(TIMESTAMP_ISO));
            w.newLine();
            w.write("latency_ns");
            w.newLine();
            for (long v : result.allLatencies()) {
                w.write(Long.toString(v));
                w.newLine();
            }
        }
        System.out.printf("  Latencies written → %s%n", file.toAbsolutePath());
    }

    public static <K, V, T> void dispatch(StatefulRedisConnection<K, V> connection,
                                          RedisCommand<K, V, T> command)
            throws ExecutionException, InterruptedException, TimeoutException {
        AsyncCommand<K, V, T> asyncCommand = new AsyncCommand<>(command);
        connection.dispatch(asyncCommand);
        asyncCommand.get(30, TimeUnit.SECONDS);
    }
    @FunctionalInterface
    public interface ConnectionSetup<K, V> {
        void setup(StatefulRedisConnection<K, V> connection) throws Exception;
    }

    @FunctionalInterface
    public interface QueryFunction<K, V> {
        void execute(StatefulRedisConnection<K, V> connection,
                     BucketCommandBuilder<K, V> cmd,
                     int queryIndex) throws Exception;
    }

    public record BenchmarkResult(long[] allLatencies, int totalQueries, long wallClockNanos) {
    }
}
