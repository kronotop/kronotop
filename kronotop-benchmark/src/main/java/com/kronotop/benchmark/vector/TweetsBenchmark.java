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
import com.kronotop.commands.BucketCreateArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;

/**
 * Vector search benchmark using the US Senator Tweets dataset (384-dimensional embeddings).
 * <p>
 * Usage: kronotop-benchmark vector tweets [options]
 * <p>
 * Options:
 * --host HOST          Kronotop host (default: localhost)
 * --port PORT          Kronotop port (default: 5484)
 * --data FILE          Path to JSONL data file (required unless --skip-load)
 * --bucket NAME        Bucket name (default: senator-tweets)
 * --batch-size N       Insert batch size (default: 100)
 * --max-docs N         Max documents to load, 0 for all (default: 0)
 * --search-rounds N    Number of search rounds (default: 100)
 * --top-k N            Top-K results per search (default: 10)
 * --threads N          Number of concurrent search threads (default: 1)
 * --skip-load          Skip data loading phase
 * --skip-filter        Skip filtered vector search benchmarks
 * --output-dir PATH    Write per-scenario latency CSVs to PATH (omit to disable)
 */
public class TweetsBenchmark extends AbstractVectorBenchmark {
    private static final String BUCKET_NAME_DEFAULT = "senator-tweets";
    private static final int EMBEDDING_DIM = 384;

    private final String dataFile;

    public TweetsBenchmark(String host, int port, String dataFile, String bucketName,
                           int batchSize, int maxDocs, int searchRounds, int topK,
                           int threads, boolean skipLoad, boolean skipFilter, Path outputDir) {
        super(host, port, bucketName, batchSize, maxDocs, searchRounds, topK, threads, skipLoad, skipFilter, outputDir);
        this.dataFile = dataFile;
    }

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 5484;
        String dataFile = null;
        String bucketName = BUCKET_NAME_DEFAULT;
        int batchSize = 100;
        int maxDocs = 0;
        int searchRounds = 100;
        int topK = 10;
        int threads = 1;
        boolean skipLoad = false;
        boolean skipFilter = false;
        Path outputDir = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> host = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--data" -> dataFile = args[++i];
                case "--bucket" -> bucketName = args[++i];
                case "--batch-size" -> batchSize = Integer.parseInt(args[++i]);
                case "--max-docs" -> maxDocs = Integer.parseInt(args[++i]);
                case "--search-rounds" -> searchRounds = Integer.parseInt(args[++i]);
                case "--top-k" -> topK = Integer.parseInt(args[++i]);
                case "--threads" -> threads = Integer.parseInt(args[++i]);
                case "--skip-load" -> skipLoad = true;
                case "--skip-filter" -> skipFilter = true;
                case "--output-dir" -> outputDir = Path.of(args[++i]);
                default -> {
                    System.err.println("Unknown option: " + args[i]);
                    System.exit(1);
                }
            }
        }

        if (dataFile == null && !skipLoad) {
            System.err.println("Error: --data <file> is required (or use --skip-load)");
            System.exit(1);
        }

        TweetsBenchmark benchmark = new TweetsBenchmark(
                host, port, dataFile, bucketName, batchSize, maxDocs, searchRounds, topK,
                threads, skipLoad, skipFilter, outputDir);
        benchmark.run();
    }

    @Override
    protected String datasetLabel() {
        return "Tweets";
    }

    @Override
    protected void createBucket(StatefulRedisConnection<String, String> connection,
                                BucketCommandBuilder<String, String> cmd) throws Exception {
        System.out.println("Creating bucket: " + bucketName);

        String indexes = """
                {
                  "$vector": {"field": "embeddings", "dimensions": %d, "distance": "cosine"},
                  "date": {"bson_type": "string"},
                  "id": {"bson_type": "int64"},
                  "username": {"bson_type": "string"},
                  "text": {"bson_type": "string"},
                  "party": {"bson_type": "string"},
                  "labels": {"bson_type": "int32"}
                }
                """.formatted(EMBEDDING_DIM);

        try {
            BenchmarkRunner.dispatch(connection, cmd.create(bucketName, BucketCreateArgs.Builder.indexes(indexes)));
            System.out.println("Bucket created successfully.");
        } catch (Exception e) {
            System.out.println("Bucket creation: " + e.getMessage());
        }
    }

    @Override
    protected void produceBatches(BlockingQueue<List<String>> queue) throws Exception {
        System.out.println("Loading data from: " + dataFile);
        Path path = Path.of(dataFile);
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("Data file not found: " + dataFile);
        }

        int docCount = 0;
        List<String> batch = new ArrayList<>(batchSize);
        Random rng = new Random(42);

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (maxDocs > 0 && docCount >= maxDocs) {
                    break;
                }

                if (queryVectors.size() < searchRounds && rng.nextDouble() < 0.01) {
                    JsonNode node = MAPPER.readTree(line);
                    ArrayNode embNode = (ArrayNode) node.get("embeddings");
                    float[] vec = new float[embNode.size()];
                    for (int i = 0; i < embNode.size(); i++) {
                        vec[i] = embNode.get(i).floatValue();
                    }
                    queryVectors.add(vec);
                }

                batch.add(line);
                docCount++;

                if (batch.size() >= batchSize) {
                    queue.put(batch);
                    batch = new ArrayList<>(batchSize);
                }
            }

            if (!batch.isEmpty()) {
                queue.put(batch);
            }
        }
    }

    @Override
    protected void prepareQueryVectors() {
        if (queryVectors.isEmpty()) {
            System.out.println("No query vectors available. Generating random vectors for search benchmark.");
            Random rng = new Random(42);
            for (int i = 0; i < searchRounds; i++) {
                queryVectors.add(randomVector(rng, EMBEDDING_DIM));
            }
        }
    }

    @Override
    protected void runFilteredBenchmarks() throws Exception {
        benchmarkFilteredVectorSearch("username", "SenatorShaheen", "~2.4% selectivity");
        benchmarkFilteredVectorSearch("party", "Democrat", "~50% selectivity");
    }
}
