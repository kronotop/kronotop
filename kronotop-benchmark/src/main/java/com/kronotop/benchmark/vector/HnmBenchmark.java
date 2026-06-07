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
import com.kronotop.benchmark.NpyReader;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;

/**
 * Vector search benchmark using the H&M fashion product dataset (2048-dimensional embeddings).
 * Loads product documents by merging payloads.jsonl with vectors.npy.
 * <p>
 * Usage: kronotop-benchmark vector hnm [options]
 * <p>
 * Options:
 * --host HOST          Kronotop host (default: localhost)
 * --port PORT          Kronotop port (default: 5484)
 * --data-dir DIR       Path to H&M dataset directory (required unless --skip-load)
 * --bucket NAME        Bucket name (default: hnm-products)
 * --batch-size N       Insert batch size (default: 50)
 * --max-docs N         Max documents to load, 0 for all (default: 0)
 * --search-rounds N    Number of search rounds (default: 100)
 * --top-k N            Top-K results per search (default: 10)
 * --threads N          Number of concurrent search threads (default: 1)
 * --skip-load          Skip data loading phase
 * --skip-filter        Skip filtered vector search benchmarks
 * --output-dir PATH    Write per-scenario latency CSVs to PATH (omit to disable)
 */
public class HnmBenchmark extends AbstractVectorBenchmark {
    private static final String BUCKET_NAME_DEFAULT = "hnm-products";
    private static final int EMBEDDING_DIM = 2048;

    private final String dataDir;

    public HnmBenchmark(String host, int port, String dataDir, String bucketName,
                        int batchSize, int maxDocs, int searchRounds, int topK,
                        int threads, boolean skipLoad, boolean skipFilter, Path outputDir) {
        super(host, port, bucketName, batchSize, maxDocs, searchRounds, topK, threads, skipLoad, skipFilter, outputDir);
        this.dataDir = dataDir;
    }

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 5484;
        String dataDir = null;
        String bucketName = BUCKET_NAME_DEFAULT;
        int batchSize = 50;
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
                case "--data-dir" -> dataDir = args[++i];
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

        if (dataDir == null && !skipLoad) {
            System.err.println("Error: --data-dir <directory> is required (or use --skip-load)");
            System.exit(1);
        }

        HnmBenchmark benchmark = new HnmBenchmark(
                host, port, dataDir, bucketName, batchSize, maxDocs, searchRounds, topK,
                threads, skipLoad, skipFilter, outputDir);
        benchmark.run();
    }

    @Override
    protected String datasetLabel() {
        return "H&M";
    }

    @Override
    protected void createBucket(StatefulRedisConnection<String, String> connection,
                                BucketCommandBuilder<String, String> cmd) {
        System.out.println("Creating bucket: " + bucketName);

        String indexes = """
                {
                  "$vector": {"field": "embeddings", "dimensions": %d, "distance": "cosine"},
                  "product_type_name": {"bson_type": "string"},
                  "product_group_name": {"bson_type": "string"},
                  "garment_group_name": {"bson_type": "string"},
                  "graphical_appearance_name": {"bson_type": "string"},
                  "colour_group_name": {"bson_type": "string"},
                  "perceived_colour_value_name": {"bson_type": "string"},
                  "perceived_colour_master_name": {"bson_type": "string"},
                  "department_name": {"bson_type": "string"},
                  "index_group_name": {"bson_type": "string"},
                  "section_name": {"bson_type": "string"}
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
        Path dirPath = Path.of(dataDir);
        Path payloadsPath = dirPath.resolve("payloads.jsonl");
        Path vectorsPath = dirPath.resolve("vectors.npy");

        if (!Files.exists(payloadsPath)) {
            throw new IllegalArgumentException("payloads.jsonl not found in: " + dataDir);
        }
        if (!Files.exists(vectorsPath)) {
            throw new IllegalArgumentException("vectors.npy not found in: " + dataDir);
        }

        System.out.println("Loading data from: " + dataDir);

        int docCount = 0;
        List<String> batch = new ArrayList<>(batchSize);

        try (NpyReader npy = new NpyReader(vectorsPath);
             BufferedReader reader = Files.newBufferedReader(payloadsPath)) {

            System.out.printf("Vectors shape: (%d, %d)%n", npy.rows(), npy.cols());

            String line;
            int rowIndex = 0;
            while ((line = reader.readLine()) != null) {
                if (maxDocs > 0 && docCount >= maxDocs) {
                    break;
                }

                if (rowIndex >= npy.rows()) {
                    System.out.println("Warning: payloads.jsonl has more lines than vectors.npy rows. Stopping.");
                    break;
                }

                ObjectNode doc = (ObjectNode) MAPPER.readTree(line);
                float[] vector = npy.readRow(rowIndex);
                ArrayNode embArray = MAPPER.createArrayNode();
                for (float v : vector) {
                    embArray.add(v);
                }
                doc.set("embeddings", embArray);

                batch.add(MAPPER.writeValueAsString(doc));
                rowIndex++;
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
    protected void prepareQueryVectors() throws Exception {
        if (dataDir == null) {
            System.out.println("No data directory specified. Generating random query vectors.");
            fillRandomQueryVectors();
            return;
        }

        Path testsPath = Path.of(dataDir).resolve("tests.jsonl");
        if (!Files.exists(testsPath)) {
            System.out.println("tests.jsonl not found. Generating random query vectors.");
            fillRandomQueryVectors();
            return;
        }

        System.out.println("Loading query vectors from tests.jsonl...");
        try (BufferedReader reader = Files.newBufferedReader(testsPath)) {
            String line;
            while ((line = reader.readLine()) != null && queryVectors.size() < searchRounds) {
                JsonNode node = MAPPER.readTree(line);
                ArrayNode queryNode = (ArrayNode) node.get("query");
                float[] vec = new float[queryNode.size()];
                for (int i = 0; i < queryNode.size(); i++) {
                    vec[i] = queryNode.get(i).floatValue();
                }
                queryVectors.add(vec);
            }
        }
        System.out.printf("Loaded %d query vectors.%n", queryVectors.size());
    }

    private void fillRandomQueryVectors() {
        Random rng = new Random(42);
        for (int i = 0; i < searchRounds; i++) {
            queryVectors.add(randomVector(rng, EMBEDDING_DIM));
        }
    }

    @Override
    protected void runFilteredBenchmarks() throws Exception {
        benchmarkFilteredVectorSearch("product_group_name", "Garment Upper body", "~40% selectivity");
        benchmarkFilteredVectorSearch("colour_group_name", "Black", "~21% selectivity");
        benchmarkFilteredVectorSearch("index_group_name", "Ladieswear", "specific category");
    }
}
