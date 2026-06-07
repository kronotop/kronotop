/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.vector;

import com.kronotop.volume.EntryMetadata;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class OnHeapVectorGraphIndexPQTest {
    private static final int DIMENSIONS = 12;
    private static final int PQ_THRESHOLD = 50;
    private static final int PQ_DIVISOR = 3; // 12 / 3 = 4 subspaces

    private OnHeapVectorGraphIndex graph;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        graph = new OnHeapVectorGraphIndex(DIMENSIONS, VectorSimilarityFunction.COSINE, PQ_THRESHOLD, PQ_DIVISOR);
        executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @AfterEach
    void tearDown() throws IOException {
        graph.close();
        executor.close();
    }

    private EntryMetadata newEntryMetadata() {
        return new EntryMetadata(1L, new byte[8], 0L, 100L, 1L);
    }

    private float[] randomVector(Random rng) {
        float[] v = new float[DIMENSIONS];
        for (int i = 0; i < DIMENSIONS; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    void shouldNotTrainPQBelowThreshold() {
        // Behavior: PQ training does not occur when the number of vectors is below the threshold.
        Random rng = new Random(42);
        for (int i = 0; i < PQ_THRESHOLD - 1; i++) {
            graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), randomVector(rng), executor).join();
        }

        assertFalse(graph.isPqTrained());
        assertNull(graph.getPqVectors());
        assertNull(graph.getPq());
    }

    @Test
    void shouldTrainPQAtThreshold() {
        // Behavior: PQ training occurs when the number of vectors reaches the threshold.
        Random rng = new Random(42);
        for (int i = 0; i < PQ_THRESHOLD; i++) {
            graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), randomVector(rng), executor).join();
        }

        assertTrue(graph.isPqTrained());
        assertNotNull(graph.getPqVectors());
        assertNotNull(graph.getPq());
    }

    @Test
    void shouldContinueAddingAfterPQTraining() {
        // Behavior: Vectors added after PQ training are encoded and searchable.
        Random rng = new Random(42);
        int total = PQ_THRESHOLD + 20;
        for (int i = 0; i < total; i++) {
            graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), randomVector(rng), executor).join();
        }

        assertTrue(graph.isPqTrained());
        assertEquals(total, graph.size());
    }

    @Test
    void shouldSearchCorrectlyAfterPQTraining() {
        // Behavior: Search returns the nearest neighbor after PQ training completes.
        Random rng = new Random(42);
        float[] target = randomVector(rng);
        ObjectId targetOid = new ObjectId();
        graph.addGraphNode(targetOid, 0, newEntryMetadata(), target, executor).join();

        // Fill up to the threshold with random vectors
        for (int i = 1; i < PQ_THRESHOLD + 10; i++) {
            graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), randomVector(rng), executor).join();
        }

        assertTrue(graph.isPqTrained());

        SearchResult result = graph.search(target, 1);
        assertEquals(1, result.getNodes().length);
        // The exact target vector should be the top result
        assertEquals(0, result.getNodes()[0].node);
    }

    @Test
    void shouldFlushPQVectorsFile(@TempDir Path tempDir) throws IOException {
        // Behavior: Flushing after PQ training produces .index, .vmeta, and .pqv files.
        Random rng = new Random(42);
        for (int i = 0; i < PQ_THRESHOLD; i++) {
            graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), randomVector(rng), executor).join();
        }

        assertTrue(graph.isPqTrained());
        graph.flush(tempDir);

        try (Stream<Path> files = Files.list(tempDir)) {
            var allFiles = files.toList();
            long indexCount = allFiles.stream().filter(p -> p.toString().endsWith(".index")).count();
            long metaCount = allFiles.stream().filter(p -> p.toString().endsWith(".vmeta")).count();
            long pqCount = allFiles.stream().filter(p -> p.toString().endsWith(".pqv")).count();
            assertEquals(1, indexCount);
            assertEquals(1, metaCount);
            assertEquals(1, pqCount);
        }
    }

    @Test
    void shouldNotFlushPQVectorsFileWithoutTraining(@TempDir Path tempDir) throws IOException {
        // Behavior: Flushing before PQ training does not produce a .pqv file.
        Random rng = new Random(42);
        for (int i = 0; i < 5; i++) {
            graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), randomVector(rng), executor).join();
        }

        assertFalse(graph.isPqTrained());
        graph.flush(tempDir);

        try (Stream<Path> files = Files.list(tempDir)) {
            long pqCount = files.filter(p -> p.toString().endsWith(".pqv")).count();
            assertEquals(0, pqCount);
        }
    }

    @Test
    void shouldSkipPQTrainingWhenThresholdIsZero() throws IOException {
        // Behavior: PQ training is disabled when the threshold is 0.
        try (OnHeapVectorGraphIndex noTrainGraph = new OnHeapVectorGraphIndex(
                DIMENSIONS, VectorSimilarityFunction.COSINE, 0, PQ_DIVISOR)) {
            Random rng = new Random(42);
            for (int i = 0; i < 100; i++) {
                noTrainGraph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), randomVector(rng), executor).join();
            }
            assertFalse(noTrainGraph.isPqTrained());
        }
    }

    @Test
    void shouldTrainPQWithEuclideanSimilarity() throws IOException {
        // Behavior: PQ training with EUCLIDEAN similarity uses centered quantization and produces searchable results.
        try (OnHeapVectorGraphIndex euclideanGraph = new OnHeapVectorGraphIndex(
                DIMENSIONS, VectorSimilarityFunction.EUCLIDEAN, PQ_THRESHOLD, PQ_DIVISOR)) {
            Random rng = new Random(42);
            float[] target = randomVector(rng);
            euclideanGraph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), target, executor).join();
            for (int i = 1; i < PQ_THRESHOLD; i++) {
                euclideanGraph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), randomVector(rng), executor).join();
            }
            assertTrue(euclideanGraph.isPqTrained());

            SearchResult result = euclideanGraph.search(target, 1);
            assertEquals(1, result.getNodes().length);
            assertEquals(0, result.getNodes()[0].node);
        }
    }

    @Test
    void shouldFlushAndSearchPQWithEuclideanOnDisk(@TempDir Path tempDir) throws IOException {
        // Behavior: A PQ-trained EUCLIDEAN index flushed to disk produces .pqv files and supports on-disk search.
        try (OnHeapVectorGraphIndex euclideanGraph = new OnHeapVectorGraphIndex(
                DIMENSIONS, VectorSimilarityFunction.EUCLIDEAN, PQ_THRESHOLD, PQ_DIVISOR)) {
            Random rng = new Random(42);
            float[] target = randomVector(rng);
            euclideanGraph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), target, executor).join();
            for (int i = 1; i < PQ_THRESHOLD; i++) {
                euclideanGraph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), randomVector(rng), executor).join();
            }
            assertTrue(euclideanGraph.isPqTrained());
            euclideanGraph.flush(tempDir);

            try (Stream<Path> files = Files.list(tempDir)) {
                assertTrue(files.anyMatch(p -> p.toString().endsWith(".pqv")));
            }

            Path indexFile;
            try (Stream<Path> files = Files.list(tempDir)) {
                indexFile = files.filter(p -> p.toString().endsWith(".index")).findFirst().orElseThrow();
            }
            try (OnDiskVectorGraphIndex diskIndex = new OnDiskVectorGraphIndex(
                    indexFile, VectorSimilarityFunction.EUCLIDEAN)) {
                SearchResult result = diskIndex.search(target, 1);
                assertEquals(1, result.getNodes().length);
                assertTrue(result.getNodes()[0].score > 0);
            }
        }
    }
}
