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

import com.apple.foundationdb.Transaction;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.*;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.VolumeTestUtil;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class VectorGraphIndexGroupTest extends BaseStandaloneInstanceTest {
    private static final String SELECTOR = "embedding";
    private static final int DIMENSIONS = 3;
    private static final int PQ_DIMENSIONS = 12;
    private static final int PQ_THRESHOLD = 50;
    private static final int PQ_DIVISOR = 3;
    private VectorGraphIndexGroup group;
    private ExecutorService executor;
    private BucketMetadata metadata;
    private VectorIndex vectorIndex;

    private VectorIndex createVectorIndex() {
        createBucket(TEST_BUCKET);
        String name = VectorIndexNameGenerator.generate(SELECTOR, DIMENSIONS, DistanceFunction.COSINE);
        VectorIndexDefinition definition = VectorIndexDefinition.create(
                name, SELECTOR, DIMENSIONS, DistanceFunction.COSINE, IndexStatus.WAITING);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            VectorIndexUtil.create(tx, getBucketMetadata(TEST_BUCKET), definition);
            tr.commit().join();
        }
        BucketMetadata md = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        return md.vectorIndexes().getIndexBySelector(SELECTOR, IndexSelectionPolicy.ALL);
    }

    @BeforeEach
    void setUp() {
        vectorIndex = createVectorIndex();
        metadata = getBucketMetadata(TEST_BUCKET);
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    @AfterEach
    public void tearDown() {
        group.closeAll();
        executor.close();
        cleanupVectorIndexDir();
        super.tearDown();
    }

    private void cleanupVectorIndexDir() {
        Path vectorDir = VectorGraphIndexGroup.resolveVectorDir(context.getDataDir().resolve("bucket"));
        if (Files.isDirectory(vectorDir)) {
            try (Stream<Path> walk = Files.walk(vectorDir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                    }
                });
            } catch (IOException ignored) {
            }
        }
    }

    private EntryMetadata newEntryMetadata(long segment) {
        return new EntryMetadata(1L, new byte[8], 0L, segment, 1L);
    }

    private OnHeapVectorGraphIndex newIndex() {
        return new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE);
    }

    private OnHeapVectorGraphIndex newPqIndex() {
        return new OnHeapVectorGraphIndex(PQ_DIMENSIONS, VectorSimilarityFunction.COSINE, PQ_THRESHOLD, PQ_DIVISOR);
    }

    private float[] randomPqVector(Random rng) {
        float[] v = new float[PQ_DIMENSIONS];
        for (int i = 0; i < PQ_DIMENSIONS; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    void shouldReturnEmptyListWhenNoIndexes() {
        // Behavior: searchAll on a group with no on-heap indexes returns an empty list.
        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 3, 0.0f, 1.0f);
        assertTrue(results.isEmpty());
    }

    @Test
    void shouldReturnEmptyListWhenAllIndexesAreEmpty() {
        // Behavior: searchAll on a group where all on-heap indexes have zero vectors returns an empty list.
        group.addOnHeap(newIndex());
        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 3, 0.0f, 1.0f);
        assertTrue(results.isEmpty());
    }

    @Test
    void shouldSearchSingleIndex() {
        // Behavior: searchAll with a single on-heap index returns the same results as searching that index directly.
        OnHeapVectorGraphIndex index = newIndex();
        index.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        index.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        index.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
        group.addOnHeap(index);

        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 2, 0.0f, 1.0f);
        assertEquals(2, results.size());
        assertTrue(results.get(0).score() >= results.get(1).score());
    }

    @Test
    void shouldMergeResultsFromMultipleIndexes() {
        // Behavior: searchAll merges results from two on-heap indexes and returns the global top-K by score.
        OnHeapVectorGraphIndex indexA = newIndex();
        indexA.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        indexA.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        group.addOnHeap(indexA);

        OnHeapVectorGraphIndex indexB = newIndex();
        indexB.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
        indexB.addGraphNode(new ObjectId(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
        group.addOnHeap(indexB);

        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 2, 0.0f, 1.0f);
        assertEquals(2, results.size());
        assertTrue(results.get(0).score() >= results.get(1).score());
        assertNotNull(results.get(0).location().entryMetadata());
    }

    @Test
    void shouldRespectTopKLimit() {
        // Behavior: searchAll returns at most topK results even when more candidates exist across indexes.
        OnHeapVectorGraphIndex indexA = newIndex();
        indexA.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        indexA.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        indexA.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
        group.addOnHeap(indexA);

        OnHeapVectorGraphIndex indexB = newIndex();
        indexB.addGraphNode(new ObjectId(), 0, newEntryMetadata(4), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
        indexB.addGraphNode(new ObjectId(), 0, newEntryMetadata(5), new float[]{0.1f, 0.9f, 0.0f}, executor).join();
        indexB.addGraphNode(new ObjectId(), 0, newEntryMetadata(6), new float[]{0.0f, 0.1f, 0.9f}, executor).join();
        group.addOnHeap(indexB);

        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 3, 0.0f, 1.0f);
        assertEquals(3, results.size());
    }

    @Test
    void shouldReturnResultsInDescendingScoreOrder() {
        // Behavior: searchAll returns results sorted from highest to lowest similarity score.
        OnHeapVectorGraphIndex indexA = newIndex();
        indexA.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        indexA.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.5f, 0.5f, 0.0f}, executor).join();
        group.addOnHeap(indexA);

        OnHeapVectorGraphIndex indexB = newIndex();
        indexB.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        indexB.addGraphNode(new ObjectId(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
        group.addOnHeap(indexB);

        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 4, 0.0f, 1.0f);
        for (int i = 0; i < results.size() - 1; i++) {
            assertTrue(results.get(i).score() >= results.get(i + 1).score(),
                    "Expected descending order but score at " + i + " (" + results.get(i).score() +
                            ") < score at " + (i + 1) + " (" + results.get(i + 1).score() + ")");
        }
    }

    private OnDiskVectorGraphIndex flushToOnDisk(Path tempDir, OnHeapVectorGraphIndex heapIndex, String name) throws IOException {
        Path flushDir = tempDir.resolve(name);
        Files.createDirectories(flushDir);
        heapIndex.flush(flushDir);
        heapIndex.close();

        Path indexFile = Files.list(flushDir)
                .filter(p -> p.toString().endsWith(".index"))
                .findFirst()
                .orElseThrow(() -> new IOException("No .index file found after flush"));
        return new OnDiskVectorGraphIndex(indexFile, VectorSimilarityFunction.COSINE);
    }

    @Test
    void shouldSearchOnDiskIndexes(@TempDir Path tempDir) throws IOException {
        // Behavior: searchAll returns results from on-disk indexes when no on-heap indexes exist.
        OnHeapVectorGraphIndex heapIndex = newIndex();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        OnDiskVectorGraphIndex diskIndex = flushToOnDisk(tempDir, heapIndex, "graph-a");
        group.addOnDisk(diskIndex);

        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 2, 0.0f, 1.0f);
        assertEquals(2, results.size());
        assertTrue(results.get(0).score() >= results.get(1).score());
    }

    @Test
    void shouldMergeOnHeapAndOnDiskResults(@TempDir Path tempDir) throws IOException {
        // Behavior: searchAll merges results from both on-heap and on-disk indexes into a single top-K list.
        OnHeapVectorGraphIndex heapForDisk = newIndex();
        heapForDisk.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapForDisk.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

        OnDiskVectorGraphIndex diskIndex = flushToOnDisk(tempDir, heapForDisk, "graph-disk");
        group.addOnDisk(diskIndex);

        OnHeapVectorGraphIndex heapIndex = newIndex();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
        group.addOnHeap(heapIndex);

        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 3, 0.0f, 1.0f);
        assertEquals(3, results.size());
        assertTrue(results.get(0).score() >= results.get(1).score());
        assertTrue(results.get(1).score() >= results.get(2).score());
    }

    @Test
    void shouldSearchOnlyOnDiskWhenNoOnHeapIndexes(@TempDir Path tempDir) throws IOException {
        // Behavior: searchAll works correctly when only on-disk indexes are present in the group.
        OnHeapVectorGraphIndex heapA = newIndex();
        heapA.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapA.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        OnDiskVectorGraphIndex diskA = flushToOnDisk(tempDir, heapA, "graph-a");
        group.addOnDisk(diskA);

        OnHeapVectorGraphIndex heapB = newIndex();
        heapB.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.5f, 0.5f, 0.0f}, executor).join();
        heapB.addGraphNode(new ObjectId(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
        OnDiskVectorGraphIndex diskB = flushToOnDisk(tempDir, heapB, "graph-b");
        group.addOnDisk(diskB);

        assertTrue(group.getOnHeapIndexes().isEmpty());
        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 3, 0.0f, 1.0f);
        assertEquals(3, results.size());
        assertNotNull(results.get(0).location().entryMetadata());
    }

    @Test
    void shouldFlushOnHeapIndexesToDisk(@TempDir Path tempDir) throws IOException {
        // Behavior: flush writes .index and .vmeta files for each non-empty, unflushed on-heap index.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        OnHeapVectorGraphIndex index = newIndex();
        index.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        index.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        group.addOnHeap(index);

        group.flush(tempDir);

        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(tempDir)
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        assertTrue(Files.isDirectory(indexDir));

        try (Stream<Path> files = Files.list(indexDir)) {
            List<Path> allFiles = files.toList();
            long indexCount = allFiles.stream().filter(p -> p.toString().endsWith(".index")).count();
            long metaCount = allFiles.stream().filter(p -> p.toString().endsWith(".vmeta")).count();
            long completeCount = allFiles.stream().filter(p -> p.toString().endsWith(".complete")).count();
            assertEquals(1, indexCount);
            assertEquals(1, metaCount);
            assertEquals(1, completeCount);
        }

        assertTrue(index.isFlushed());

        // On-disk indexes should be populated after a flush
        assertEquals(1, group.getOnDiskIndexes().size());
    }

    @Test
    void shouldReturnEmptyListWhenDirectoryDoesNotExist() throws IOException {
        // Behavior: openOnDiskIndexes returns an empty list when the index directory does not exist.
        List<OnDiskVectorGraphIndex> result = VectorGraphIndexGroup.openOnDiskIndexes(
                context.getDataDir().resolve("bucket"), metadata.uuid(),
                vectorIndex.definition().id(), VectorSimilarityFunction.COSINE);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldReturnEmptyListWhenDirectoryIsEmpty() throws IOException {
        // Behavior: openOnDiskIndexes returns an empty list when the directory exists but contains no complete index sets.
        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(context.getDataDir().resolve("bucket"))
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        Files.createDirectories(indexDir);
        List<OnDiskVectorGraphIndex> result = VectorGraphIndexGroup.openOnDiskIndexes(
                context.getDataDir().resolve("bucket"), metadata.uuid(),
                vectorIndex.definition().id(), VectorSimilarityFunction.COSINE);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldLoadSingleOnDiskIndex() throws IOException {
        // Behavior: openOnDiskIndexes loads a single flushed index and search works on the loaded indexes.
        OnHeapVectorGraphIndex heapIndex = newIndex();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(context.getDataDir().resolve("bucket"))
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        Files.createDirectories(indexDir);
        heapIndex.flush(indexDir);
        heapIndex.close();

        List<OnDiskVectorGraphIndex> result = VectorGraphIndexGroup.openOnDiskIndexes(
                context.getDataDir().resolve("bucket"), metadata.uuid(),
                vectorIndex.definition().id(), VectorSimilarityFunction.COSINE);
        assertEquals(1, result.size());

        VectorGraphIndexGroup loaded = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        for (OnDiskVectorGraphIndex idx : result) {
            loaded.addOnDisk(idx);
        }
        List<MergedNodeScore> results = loaded.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 2, 0.0f, 1.0f);
        assertEquals(2, results.size());
        assertTrue(results.get(0).score() >= results.get(1).score());
        loaded.closeAll();
    }

    @Test
    void shouldLoadMultipleOnDiskIndexes() throws IOException {
        // Behavior: openOnDiskIndexes loads all complete index sets in the directory as separate on-disk indexes.
        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(context.getDataDir().resolve("bucket"))
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        Files.createDirectories(indexDir);

        OnHeapVectorGraphIndex heapA = newIndex();
        heapA.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapA.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        heapA.flush(indexDir);
        heapA.close();

        OnHeapVectorGraphIndex heapB = newIndex();
        heapB.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.5f, 0.5f, 0.0f}, executor).join();
        heapB.addGraphNode(new ObjectId(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
        heapB.flush(indexDir);
        heapB.close();

        List<OnDiskVectorGraphIndex> result = VectorGraphIndexGroup.openOnDiskIndexes(
                context.getDataDir().resolve("bucket"), metadata.uuid(),
                vectorIndex.definition().id(), VectorSimilarityFunction.COSINE);
        assertEquals(2, result.size());

        VectorGraphIndexGroup loaded = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        for (OnDiskVectorGraphIndex idx : result) {
            loaded.addOnDisk(idx);
        }
        List<MergedNodeScore> results = loaded.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 3, 0.0f, 1.0f);
        assertEquals(3, results.size());
        loaded.closeAll();
    }

    @Test
    void shouldSkipFlushedAndEmptyIndexes(@TempDir Path tempDir) {
        // Behavior: flush skips already-flushed and empty indexes without creating files.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        // Add an empty index
        group.addOnHeap(newIndex());

        group.flush(tempDir);

        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(tempDir)
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        assertFalse(Files.exists(indexDir));
    }

    @Test
    void shouldCreateOnHeapIndexWhenNoneExists() {
        // Behavior: getOrCreateOnHeap creates a new on-heap index when the group has none.
        OnHeapVectorGraphIndex graph = group.getOrCreateOnHeap(3, VectorSimilarityFunction.COSINE, 0, 6);
        assertNotNull(graph);
        assertEquals(1, group.getOnHeapIndexes().size());
        assertSame(graph, group.getOnHeapIndexes().getLast());
    }

    @Test
    void shouldReturnExistingOnHeapIndex() {
        // Behavior: getOrCreateOnHeap returns the existing on-heap index without creating a new one.
        OnHeapVectorGraphIndex first = group.getOrCreateOnHeap(3, VectorSimilarityFunction.COSINE, 0, 6);
        OnHeapVectorGraphIndex second = group.getOrCreateOnHeap(3, VectorSimilarityFunction.COSINE, 0, 6);
        assertSame(first, second);
        assertEquals(1, group.getOnHeapIndexes().size());
    }

    @Test
    void shouldRotateOnHeapIndex() {
        // Behavior: rotateOnHeap creates a new active index and returns the previous one when expected matches.
        OnHeapVectorGraphIndex original = group.getOrCreateOnHeap(3, VectorSimilarityFunction.COSINE, 0, 6);
        OnHeapVectorGraphIndex previous = group.rotateOnHeap(original, 3, VectorSimilarityFunction.COSINE, 0, 6);

        assertSame(original, previous);
        assertEquals(2, group.getOnHeapIndexes().size());

        // The active index (last) should be the new one, not the original.
        OnHeapVectorGraphIndex active = group.getOrCreateOnHeap(3, VectorSimilarityFunction.COSINE, 0, 6);
        assertNotSame(original, active);
        assertSame(active, group.getOnHeapIndexes().getLast());
    }

    @Test
    void shouldNotRotateWhenExpectedDoesNotMatch() {
        // Behavior: rotateOnHeap returns null without creating a new index when expected does not match the active index.
        OnHeapVectorGraphIndex active = group.getOrCreateOnHeap(3, VectorSimilarityFunction.COSINE, 0, 6);

        try (OnHeapVectorGraphIndex stale = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
            OnHeapVectorGraphIndex result = group.rotateOnHeap(stale, 3, VectorSimilarityFunction.COSINE, 0, 6);

            assertNull(result);
            assertEquals(1, group.getOnHeapIndexes().size());
            assertSame(active, group.getOnHeapIndexes().getLast());
        } catch (IOException ignored) {
        }
    }

    @Test
    void shouldNotCreateMultipleIndexesWhenConcurrentThreadsRotate() throws Exception {
        // Behavior: When multiple threads concurrently call rotateOnHeap with the same expected graph,
        // only one rotation succeeds and the rest return null.
        OnHeapVectorGraphIndex original = group.getOrCreateOnHeap(3, VectorSimilarityFunction.COSINE, 0, 6);

        int threadCount = 50;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    OnHeapVectorGraphIndex previous = group.rotateOnHeap(
                            original, 3, VectorSimilarityFunction.COSINE, 0, 6);
                    if (previous != null) {
                        successCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();

        assertEquals(1, successCount.get());
        assertEquals(2, group.getOnHeapIndexes().size());
    }

    @Test
    void shouldSearchAllSkipFlushedIndexes(@TempDir Path tempDir) throws IOException {
        // Behavior: searchAll skips flushed on-heap indexes and returns results only from active ones.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        OnHeapVectorGraphIndex flushedIndex = newIndex();
        flushedIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        flushedIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        group.addOnHeap(flushedIndex);

        // Flush to mark it as flushed
        Path indexDir = tempDir.resolve("flush-dir");
        Files.createDirectories(indexDir);
        flushedIndex.flush(indexDir);

        // Add a live index
        OnHeapVectorGraphIndex activeIndex = newIndex();
        activeIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
        group.addOnHeap(activeIndex);

        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 5, 0.0f, 1.0f);
        // Only the active index's single node should appear
        assertEquals(1, results.size());
    }

    @Test
    void shouldSearchAllReturnResultsAfterFlushWithoutRestart(@TempDir Path tempDir) {
        // Behavior: searchAll returns results from flushed indexes via on-disk path without requiring a restart.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        OnHeapVectorGraphIndex index = newIndex();
        index.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        index.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        index.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
        group.addOnHeap(index);

        // Verify search works before flush
        List<MergedNodeScore> beforeFlush = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 3, 0.0f, 1.0f);
        assertEquals(3, beforeFlush.size());

        group.flush(tempDir);

        // On-heap indexes should be removed, on-disk should be populated
        assertTrue(group.getOnHeapIndexes().isEmpty());
        assertEquals(1, group.getOnDiskIndexes().size());

        // Search should still return results via on-disk indexes
        List<MergedNodeScore> afterFlush = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 3, 0.0f, 1.0f);
        assertEquals(3, afterFlush.size());
        assertTrue(afterFlush.get(0).score() >= afterFlush.get(1).score());
    }

    @Test
    void shouldSearchAcrossFlushedOnDiskAndNewOnHeapIndexes(@TempDir Path tempDir) {
        // Behavior: After flushing on-heap indexes to disk and adding new on-heap indexes,
        // searchAll returns merged results from both flushed on-disk and new on-heap indexes.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        // Step 1: Create an on-heap index with vectors and flush it to disk.
        OnHeapVectorGraphIndex firstIndex = newIndex();
        firstIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        firstIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        group.addOnHeap(firstIndex);

        group.flush(tempDir);

        // Flushed index should be on disk, on-heap list should be empty.
        assertTrue(group.getOnHeapIndexes().isEmpty());
        assertEquals(1, group.getOnDiskIndexes().size());

        // Step 2: Add a new on-heap index with different vectors.
        OnHeapVectorGraphIndex secondIndex = newIndex();
        secondIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
        secondIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
        group.addOnHeap(secondIndex);

        // Step 3: Search across both sources.
        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 4, 0.0f, 1.0f);
        assertEquals(4, results.size());

        // Verify descending score order (correct top-K merge).
        for (int i = 0; i < results.size() - 1; i++) {
            assertTrue(results.get(i).score() >= results.get(i + 1).score(),
                    "Expected descending order but score at " + i + " (" + results.get(i).score() +
                            ") < score at " + (i + 1) + " (" + results.get(i + 1).score() + ")");
        }
    }

    @Test
    void shouldFlushSingleOnHeapIndexToDisk(@TempDir Path tempDir) {
        // Behavior: flushSingle writes only the specified on-heap index to disk,
        // leaving other on-heap indexes untouched.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        OnHeapVectorGraphIndex target = newIndex();
        target.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        target.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        group.addOnHeap(target);

        OnHeapVectorGraphIndex active = newIndex();
        active.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.5f, 0.5f, 0.0f}, executor).join();
        group.addOnHeap(active);

        group.flushSingle(tempDir, target);

        // Target should be flushed, removed from on-heap, and promoted to on-disk.
        assertTrue(target.isFlushed());
        assertEquals(1, group.getOnHeapIndexes().size());
        assertSame(active, group.getOnHeapIndexes().getFirst());
        assertEquals(1, group.getOnDiskIndexes().size());

        // Search should return results from both on-disk (target) and on-heap (active).
        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 3, 0.0f, 1.0f);
        assertEquals(3, results.size());
    }

    @Test
    void shouldFlushSingleSkipAlreadyFlushedIndex(@TempDir Path tempDir) {
        // Behavior: flushSingle is a no-op when the given index is already flushed.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        OnHeapVectorGraphIndex index = newIndex();
        index.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        group.addOnHeap(index);

        group.flushSingle(tempDir, index);
        int onDiskCount = group.getOnDiskIndexes().size();

        // Calling again should be a no-op.
        group.flushSingle(tempDir, index);
        assertEquals(onDiskCount, group.getOnDiskIndexes().size());
    }

    @Test
    void shouldFlushSingleSkipEmptyIndex(@TempDir Path tempDir) {
        // Behavior: flushSingle is a no-op when the given index has no vectors.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        OnHeapVectorGraphIndex emptyIndex = newIndex();
        group.addOnHeap(emptyIndex);

        group.flushSingle(tempDir, emptyIndex);

        assertFalse(emptyIndex.isFlushed());
        assertEquals(1, group.getOnHeapIndexes().size());
        assertTrue(group.getOnDiskIndexes().isEmpty());
    }

    @Test
    void shouldCreateSearchSessionFromMixedIndexes(@TempDir Path tempDir) throws IOException {
        // Behavior: createSearchSession builds a session that merges results from both on-heap and on-disk indexes.
        OnHeapVectorGraphIndex heapForDisk = newIndex();
        heapForDisk.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapForDisk.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

        OnDiskVectorGraphIndex diskIndex = flushToOnDisk(tempDir, heapForDisk, "session-disk");
        group.addOnDisk(diskIndex);

        OnHeapVectorGraphIndex heapIndex = newIndex();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
        group.addOnHeap(heapIndex);

        try (VectorSearchSession session = group.createSearchSession(new float[]{1.0f, 0.0f, 0.0f})) {
            List<MergedNodeScore> results = session.search(3, 0.0f, 1.0f);
            assertEquals(3, results.size());
            for (int i = 0; i < results.size() - 1; i++) {
                assertTrue(results.get(i).score() >= results.get(i + 1).score(),
                        "Expected descending order at index " + i);
            }
        }
    }

    @Test
    void shouldCreateEmptySessionWhenNoSearchableIndexes() throws IOException {
        // Behavior: createSearchSession with only empty indexes returns a session whose search yields empty results.
        group.addOnHeap(newIndex());

        try (VectorSearchSession session = group.createSearchSession(new float[]{1.0f, 0.0f, 0.0f})) {
            List<MergedNodeScore> results = session.search(3, 0.0f, 1.0f);
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void shouldRemoveFlushedIndexesAfterGroupFlush(@TempDir Path tempDir) {
        // Behavior: after a group flush, flushed on-heap indexes are removed from the list.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        OnHeapVectorGraphIndex index = newIndex();
        index.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        group.addOnHeap(index);

        // Add an empty index that won't be flushed
        OnHeapVectorGraphIndex emptyIndex = newIndex();
        group.addOnHeap(emptyIndex);

        group.flush(tempDir);

        // The flushed index should be removed; the empty one remains
        assertEquals(1, group.getOnHeapIndexes().size());
        assertSame(emptyIndex, group.getOnHeapIndexes().getFirst());
    }

    @Test
    void shouldFlushPQTrainedIndexAndLoadFromDisk() throws IOException {
        // Behavior: A PQ-trained on-heap index flushed via group produces .pqv files and the loaded on-disk index supports search.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        OnHeapVectorGraphIndex index = newPqIndex();
        Random rng = new Random(42);
        float[] target = randomPqVector(rng);
        index.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), target, executor).join();
        for (int i = 1; i < PQ_THRESHOLD; i++) {
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(i + 1), randomPqVector(rng), executor).join();
        }
        assertTrue(index.isPqTrained());
        group.addOnHeap(index);

        Path bucketDir = context.getDataDir().resolve("bucket");
        group.flush(bucketDir);

        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(bucketDir)
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        assertTrue(Files.isDirectory(indexDir));
        try (Stream<Path> files = Files.list(indexDir)) {
            assertTrue(files.anyMatch(p -> p.toString().endsWith(".pqv")));
        }

        List<OnDiskVectorGraphIndex> result = VectorGraphIndexGroup.openOnDiskIndexes(
                bucketDir, metadata.uuid(), vectorIndex.definition().id(), VectorSimilarityFunction.COSINE);
        assertEquals(1, result.size());

        VectorGraphIndexGroup loaded = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        for (OnDiskVectorGraphIndex idx : result) {
            loaded.addOnDisk(idx);
        }
        List<MergedNodeScore> results = loaded.searchAll(target, 1, 0.0f, 1.0f);
        assertEquals(1, results.size());
        assertTrue(results.getFirst().score() > 0);
        loaded.closeAll();
    }

    @Test
    void shouldCreateSearchSessionWithPQOnDiskIndex(@TempDir Path tempDir) throws IOException {
        // Behavior: A search session across a PQ on-disk index and a non-PQ on-heap index merges results correctly.
        OnHeapVectorGraphIndex pqHeap = newPqIndex();
        Random rng = new Random(42);
        float[] target = randomPqVector(rng);
        pqHeap.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), target, executor).join();
        for (int i = 1; i < PQ_THRESHOLD; i++) {
            pqHeap.addGraphNode(new ObjectId(), 0, newEntryMetadata(i + 1), randomPqVector(rng), executor).join();
        }

        OnDiskVectorGraphIndex diskIndex = flushToOnDisk(tempDir, pqHeap, "pq-disk");
        group.addOnDisk(diskIndex);

        OnHeapVectorGraphIndex heapIndex = new OnHeapVectorGraphIndex(PQ_DIMENSIONS, VectorSimilarityFunction.COSINE);
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(100), randomPqVector(rng), executor).join();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(101), randomPqVector(rng), executor).join();
        group.addOnHeap(heapIndex);

        try (VectorSearchSession session = group.createSearchSession(target)) {
            List<MergedNodeScore> results = session.search(3, 0.0f, 1.0f);
            assertEquals(3, results.size());
            for (int i = 0; i < results.size() - 1; i++) {
                assertTrue(results.get(i).score() >= results.get(i + 1).score(),
                        "Expected descending order at index " + i);
            }
        }
    }

    @Test
    void shouldReportNotReadyAfterFailedBootstrap() {
        // Behavior: A group whose bootstrap failed reports isReady=false.
        CompletableFuture<Void> future = new CompletableFuture<>();
        VectorGraphIndexGroup failedGroup = new VectorGraphIndexGroup(context, metadata, vectorIndex, future);

        failedGroup.markFailed(new IOException("disk read error"));

        assertFalse(failedGroup.isReady());
    }

    @Test
    void shouldPropagateFailureOnAwaitReady() {
        // Behavior: awaitReady throws CompletionException when bootstrap failed.
        CompletableFuture<Void> future = new CompletableFuture<>();
        VectorGraphIndexGroup failedGroup = new VectorGraphIndexGroup(context, metadata, vectorIndex, future);

        failedGroup.markFailed(new IOException("disk read error"));

        assertThrows(CompletionException.class, failedGroup::awaitReady);
    }

    @Test
    void shouldReportReadyAfterSuccessfulBootstrap() {
        // Behavior: A group whose bootstrap completed successfully reports isReady=true.
        CompletableFuture<Void> future = new CompletableFuture<>();
        VectorGraphIndexGroup bootstrappingGroup = new VectorGraphIndexGroup(context, metadata, vectorIndex, future);

        assertFalse(bootstrappingGroup.isReady());

        bootstrappingGroup.markReady();

        assertTrue(bootstrappingGroup.isReady());
    }

    @Test
    void shouldBootstrapSlowPathWhenMutationLogHasEntries() {
        // Behavior: bootstrapVectorGroup takes the slow-path when mutation log has entries,
        // returning a not-immediately-ready group that becomes ready after background recovery.
        BucketService service = context.getService(BucketService.NAME);

        ObjectId oid = new ObjectId();
        byte[] encodedEntry = new IndexEntry(1, VolumeTestUtil.generateEntryMetadata(1, 1, 0, 1, "test").encode()).encode();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VectorIndexMaintainer.setMutationLog(
                    tr,
                    vectorIndex.subspace(),
                    MutationLogMarker.INSERT,
                    oid.toByteArray(),
                    encodedEntry,
                    new float[]{0.1f, 0.2f, 0.3f},
                    0
            );
            tr.commit().join();
        }

        VectorGraphIndexGroup bootstrapped = service.bootstrapVectorGroup(metadata, vectorIndex);

        assertFalse(bootstrapped.isReady());

        bootstrapped.awaitReady();

        assertTrue(bootstrapped.isReady());
        assertFalse(bootstrapped.getOnHeapIndexes().isEmpty());
        assertEquals(1, bootstrapped.getOnHeapIndexes().getFirst().size());
        bootstrapped.closeAll();
    }

    @Test
    void shouldBootstrapFastPathWhenNoDataExists() {
        // Behavior: bootstrapVectorGroup takes the fast-path when no disk data and no mutation log
        // entries exist, returning an immediately ready group.
        BucketService service = context.getService(BucketService.NAME);

        VectorGraphIndexGroup bootstrapped = service.bootstrapVectorGroup(metadata, vectorIndex);

        assertTrue(bootstrapped.isReady());
        assertTrue(bootstrapped.isEmpty());
    }

    @Test
    void shouldSkipAndDeleteOrphanedFilesWithoutCompleteSentinel() throws IOException {
        // Behavior: openOnDiskIndexes returns an empty list and deletes orphaned .index/.vmeta files
        // that lack a corresponding .complete sentinel.
        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(context.getDataDir().resolve("bucket"))
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        Files.createDirectories(indexDir);

        String fakeId = new ObjectId().toHexString();
        Files.createFile(indexDir.resolve(fakeId + ".index"));
        Files.createFile(indexDir.resolve(fakeId + ".vmeta"));

        List<OnDiskVectorGraphIndex> result = VectorGraphIndexGroup.openOnDiskIndexes(
                context.getDataDir().resolve("bucket"), metadata.uuid(),
                vectorIndex.definition().id(), VectorSimilarityFunction.COSINE);
        assertTrue(result.isEmpty());

        try (Stream<Path> remaining = Files.list(indexDir)) {
            assertEquals(0, remaining.count());
        }
    }

    @Test
    void shouldBootstrapFastPathWhenOnlyOrphanedIndexFilesExist() throws IOException {
        // Behavior: bootstrapVectorGroup takes the fast-path (immediately ready) when .index files
        // exist on disk but lack a .complete sentinel, since they are not recognized as valid data.
        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(context.getDataDir().resolve("bucket"))
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        Files.createDirectories(indexDir);

        String fakeId = new ObjectId().toHexString();
        Files.createFile(indexDir.resolve(fakeId + ".index"));
        Files.createFile(indexDir.resolve(fakeId + ".vmeta"));

        BucketService service = context.getService(BucketService.NAME);
        VectorGraphIndexGroup bootstrapped = service.bootstrapVectorGroup(metadata, vectorIndex);

        assertTrue(bootstrapped.isReady());
        assertTrue(bootstrapped.isEmpty());
    }

    @Test
    void shouldLoadOnDiskIndexesWithCompleteSentinel() throws IOException {
        // Behavior: End-to-end: flush produces .complete sentinel → openOnDiskIndexes recognizes valid
        // index set → search returns results from the loaded on-disk index.
        OnHeapVectorGraphIndex heapIndex = newIndex();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(context.getDataDir().resolve("bucket"))
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        Files.createDirectories(indexDir);
        heapIndex.flush(indexDir);
        heapIndex.close();

        // Verify .complete sentinel exists
        try (Stream<Path> files = Files.list(indexDir)) {
            long completeCount = files.filter(p -> p.getFileName().toString().endsWith(".complete")).count();
            assertEquals(1, completeCount);
        }

        List<OnDiskVectorGraphIndex> result = VectorGraphIndexGroup.openOnDiskIndexes(
                context.getDataDir().resolve("bucket"), metadata.uuid(),
                vectorIndex.definition().id(), VectorSimilarityFunction.COSINE);
        assertEquals(1, result.size());

        VectorGraphIndexGroup loaded = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        for (OnDiskVectorGraphIndex idx : result) {
            loaded.addOnDisk(idx);
        }
        List<MergedNodeScore> results = loaded.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 2, 0.0f, 1.0f);
        assertEquals(2, results.size());
        assertTrue(results.get(0).score() >= results.get(1).score());
        loaded.closeAll();
    }
}
