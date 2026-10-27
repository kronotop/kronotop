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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.TestUtil;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
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
        try (Transaction tr = context.getTransactionManager().create()) {
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
        Path vectorDir = VectorGraphIndexGroup.resolveVectorDir(
                context.getDataDir().resolve("bucket"),
                VectorGraphIndexGroup.ownerShardId(metadata)
        );
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

    private Versionstamp versionstamp(int userVersion) {
        return Versionstamp.complete(new byte[10], userVersion);
    }

    private OnHeapVectorGraphIndex newIndex() {
        return new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE);
    }

    private OnHeapVectorGraphIndex newPqIndex() {
        return new OnHeapVectorGraphIndex(PQ_DIMENSIONS, VectorSimilarityFunction.COSINE, PQ_THRESHOLD, PQ_DIVISOR);
    }

    private OnHeapVectorGraphIndex openOnHeap() {
        return group.getOrCreateOnHeap(3, VectorSimilarityFunction.COSINE, 0, 6);
    }

    private OnHeapVectorGraphIndex rotateOnHeap(OnHeapVectorGraphIndex previous) {
        group.rotateOnHeap(previous, 3, VectorSimilarityFunction.COSINE, 0, 6);
        return openOnHeap();
    }

    private OnHeapVectorGraphIndex openPqOnHeap() {
        return group.getOrCreateOnHeap(PQ_DIMENSIONS, VectorSimilarityFunction.COSINE, PQ_THRESHOLD, PQ_DIVISOR);
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
        openOnHeap();
        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 3, 0.0f, 1.0f);
        assertTrue(results.isEmpty());
    }

    @Test
    void shouldSearchSingleIndex() {
        // Behavior: searchAll with a single on-heap index returns the same results as searching that index directly.
        OnHeapVectorGraphIndex index = openOnHeap();
        index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 2, 0.0f, 1.0f);
        assertEquals(2, results.size());
        assertTrue(results.get(0).score() >= results.get(1).score());
    }

    @Test
    void shouldMergeResultsFromMultipleIndexes() {
        // Behavior: searchAll merges results from two on-heap indexes and returns the global top-K by score.
        OnHeapVectorGraphIndex indexA = openOnHeap();
        indexA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        indexA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

        OnHeapVectorGraphIndex indexB = rotateOnHeap(indexA);
        indexB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
        indexB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 2, 0.0f, 1.0f);
        assertEquals(2, results.size());
        assertTrue(results.get(0).score() >= results.get(1).score());
        assertNotNull(results.get(0).location().entryMetadata());
    }

    @Test
    void shouldRespectTopKLimit() {
        // Behavior: searchAll returns at most topK results even when more candidates exist across indexes.
        OnHeapVectorGraphIndex indexA = openOnHeap();
        indexA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        indexA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        indexA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        OnHeapVectorGraphIndex indexB = rotateOnHeap(indexA);
        indexB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(4), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
        indexB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(5), new float[]{0.1f, 0.9f, 0.0f}, executor).join();
        indexB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(6), new float[]{0.0f, 0.1f, 0.9f}, executor).join();

        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 3, 0.0f, 1.0f);
        assertEquals(3, results.size());
    }

    @Test
    void shouldReturnResultsInDescendingScoreOrder() {
        // Behavior: searchAll returns results sorted from highest to lowest similarity score.
        OnHeapVectorGraphIndex indexA = openOnHeap();
        indexA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        indexA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.5f, 0.5f, 0.0f}, executor).join();

        OnHeapVectorGraphIndex indexB = rotateOnHeap(indexA);
        indexB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        indexB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

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
        return new OnDiskVectorGraphIndex(vectorIndex.subspace(), indexFile, VectorSimilarityFunction.COSINE);
    }

    @Test
    void shouldSearchOnDiskIndexes(@TempDir Path tempDir) throws IOException {
        // Behavior: searchAll returns results from on-disk indexes when no on-heap indexes exist.
        OnHeapVectorGraphIndex heapIndex = newIndex();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

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
        heapForDisk.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapForDisk.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

        OnDiskVectorGraphIndex diskIndex = flushToOnDisk(tempDir, heapForDisk, "graph-disk");
        group.addOnDisk(diskIndex);

        OnHeapVectorGraphIndex heapIndex = openOnHeap();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 3, 0.0f, 1.0f);
        assertEquals(3, results.size());
        assertTrue(results.get(0).score() >= results.get(1).score());
        assertTrue(results.get(1).score() >= results.get(2).score());
    }

    @Test
    void shouldSearchOnlyOnDiskWhenNoOnHeapIndexes(@TempDir Path tempDir) throws IOException {
        // Behavior: searchAll works correctly when only on-disk indexes are present in the group.
        OnHeapVectorGraphIndex heapA = newIndex();
        heapA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        OnDiskVectorGraphIndex diskA = flushToOnDisk(tempDir, heapA, "graph-a");
        group.addOnDisk(diskA);

        OnHeapVectorGraphIndex heapB = newIndex();
        heapB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.5f, 0.5f, 0.0f}, executor).join();
        heapB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
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

        OnHeapVectorGraphIndex index = openOnHeap();
        index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

        group.flush(tempDir);

        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(tempDir, VectorGraphIndexGroup.ownerShardId(metadata))
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
                context,
                vectorIndex.subspace(),
                context.getDataDir().resolve("bucket"), VectorGraphIndexGroup.ownerShardId(metadata), metadata.uuid(),
                vectorIndex.definition().id(), VectorSimilarityFunction.COSINE);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldReturnEmptyListWhenDirectoryIsEmpty() throws IOException {
        // Behavior: openOnDiskIndexes returns an empty list when the directory exists but contains no complete index sets.
        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(context.getDataDir().resolve("bucket"), VectorGraphIndexGroup.ownerShardId(metadata))
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        Files.createDirectories(indexDir);
        List<OnDiskVectorGraphIndex> result = VectorGraphIndexGroup.openOnDiskIndexes(
                context,
                vectorIndex.subspace(),
                context.getDataDir().resolve("bucket"), VectorGraphIndexGroup.ownerShardId(metadata), metadata.uuid(),
                vectorIndex.definition().id(), VectorSimilarityFunction.COSINE);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldLoadSingleOnDiskIndex() throws IOException {
        // Behavior: openOnDiskIndexes loads a single flushed index and search works on the loaded indexes.
        OnHeapVectorGraphIndex heapIndex = newIndex();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(context.getDataDir().resolve("bucket"), VectorGraphIndexGroup.ownerShardId(metadata))
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        Files.createDirectories(indexDir);
        heapIndex.flush(indexDir);
        heapIndex.close();

        List<OnDiskVectorGraphIndex> result = VectorGraphIndexGroup.openOnDiskIndexes(
                context,
                vectorIndex.subspace(),
                context.getDataDir().resolve("bucket"), VectorGraphIndexGroup.ownerShardId(metadata), metadata.uuid(),
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
        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(context.getDataDir().resolve("bucket"), VectorGraphIndexGroup.ownerShardId(metadata))
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        Files.createDirectories(indexDir);

        OnHeapVectorGraphIndex heapA = newIndex();
        heapA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        heapA.flush(indexDir);
        heapA.close();

        OnHeapVectorGraphIndex heapB = newIndex();
        heapB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.5f, 0.5f, 0.0f}, executor).join();
        heapB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
        heapB.flush(indexDir);
        heapB.close();

        List<OnDiskVectorGraphIndex> result = VectorGraphIndexGroup.openOnDiskIndexes(
                context,
                vectorIndex.subspace(),
                context.getDataDir().resolve("bucket"), VectorGraphIndexGroup.ownerShardId(metadata), metadata.uuid(),
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
        openOnHeap();

        group.flush(tempDir);

        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(tempDir, VectorGraphIndexGroup.ownerShardId(metadata))
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
    void shouldForEachOnHeapVisitAllIndexesInGenerationOrder() {
        // Behavior: forEachOnHeap visits every on-heap index in the same generation order as getOnHeapIndexes.
        OnHeapVectorGraphIndex first = group.getOrCreateOnHeap(3, VectorSimilarityFunction.COSINE, 0, 6);
        OnHeapVectorGraphIndex second = rotateOnHeap(first);
        rotateOnHeap(second);

        List<OnHeapVectorGraphIndex> visited = new ArrayList<>();
        group.forEachOnHeap(visited::add);

        assertEquals(3, visited.size());
        assertEquals(group.getOnHeapIndexes(), visited);
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
    void shouldNotCreateMultipleIndexesWhenConcurrentThreadsGetOrCreate() throws Exception {
        // Behavior: When multiple threads concurrently call getOrCreateOnHeap on an empty group,
        // only one on-heap index is created and every thread receives the same instance.
        int threadCount = 50;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        Set<OnHeapVectorGraphIndex> seen = ConcurrentHashMap.newKeySet();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    seen.add(group.getOrCreateOnHeap(3, VectorSimilarityFunction.COSINE, 0, 6));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();

        assertEquals(1, seen.size());
        assertSame(seen.iterator().next(), group.getOrCreateOnHeap(3, VectorSimilarityFunction.COSINE, 0, 6));
    }

    @Test
    void shouldSearchAllSkipFlushedIndexes(@TempDir Path tempDir) throws IOException {
        // Behavior: searchAll skips flushed on-heap indexes and returns results only from active ones.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        OnHeapVectorGraphIndex flushedIndex = openOnHeap();
        flushedIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        flushedIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

        // Flush to mark it as flushed
        Path indexDir = tempDir.resolve("flush-dir");
        Files.createDirectories(indexDir);
        flushedIndex.flush(indexDir);

        // Add a live index
        OnHeapVectorGraphIndex activeIndex = rotateOnHeap(flushedIndex);
        activeIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.9f, 0.1f, 0.0f}, executor).join();

        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 5, 0.0f, 1.0f);
        // Only the active index's single node should appear
        assertEquals(1, results.size());
    }

    @Test
    void shouldSearchAllReturnResultsAfterFlushWithoutRestart(@TempDir Path tempDir) {
        // Behavior: searchAll returns results from flushed indexes via on-disk path without requiring a restart.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        OnHeapVectorGraphIndex index = openOnHeap();
        index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

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
        OnHeapVectorGraphIndex firstIndex = openOnHeap();
        firstIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        firstIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

        group.flush(tempDir);

        // Flushed index should be on disk, on-heap list should be empty.
        assertTrue(group.getOnHeapIndexes().isEmpty());
        assertEquals(1, group.getOnDiskIndexes().size());

        // Step 2: Add a new on-heap index with different vectors.
        OnHeapVectorGraphIndex secondIndex = rotateOnHeap(firstIndex);
        secondIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
        secondIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

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

        OnHeapVectorGraphIndex target = openOnHeap();
        target.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        target.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

        OnHeapVectorGraphIndex active = rotateOnHeap(target);
        active.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.5f, 0.5f, 0.0f}, executor).join();

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

        OnHeapVectorGraphIndex index = openOnHeap();
        index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

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

        OnHeapVectorGraphIndex emptyIndex = openOnHeap();

        group.flushSingle(tempDir, emptyIndex);

        assertFalse(emptyIndex.isFlushed());
        assertEquals(1, group.getOnHeapIndexes().size());
        assertTrue(group.getOnDiskIndexes().isEmpty());
    }

    @Test
    void shouldRejectDeleteOnFlushedOnHeapIndex(@TempDir Path tempDir) {
        // Behavior: A delete on a flushed on-heap graph throws. The graph is already on disk, so a
        // delete applied to the in-memory copy is lost.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);
        ObjectId objectId = new ObjectId();

        OnHeapVectorGraphIndex onHeap = openOnHeap();
        onHeap.addGraphNode(objectId, versionstamp(1), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

        group.flushSingle(tempDir, onHeap);
        assertTrue(onHeap.isFlushed());

        // The delete holds the reference it read before the flush.
        assertThrows(IllegalStateException.class, () -> onHeap.markNodeDeleted(objectId, versionstamp(2)));

        OnDiskVectorGraphIndex onDisk = group.getOnDiskIndexes().getFirst();
        GraphNodeRef ref = onDisk.getMetadata().findNodeRef(objectId);
        assertNotNull(ref);
        assertNotNull(onDisk.getMetadata().findDocumentLocation(ref.ordinal()));
    }

    @Test
    void shouldCreateSearchSessionFromMixedIndexes(@TempDir Path tempDir) throws IOException {
        // Behavior: createSearchSession builds a session that merges results from both on-heap and on-disk indexes.
        OnHeapVectorGraphIndex heapForDisk = newIndex();
        heapForDisk.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapForDisk.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

        OnDiskVectorGraphIndex diskIndex = flushToOnDisk(tempDir, heapForDisk, "session-disk");
        group.addOnDisk(diskIndex);

        OnHeapVectorGraphIndex heapIndex = openOnHeap();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

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
        openOnHeap();

        try (VectorSearchSession session = group.createSearchSession(new float[]{1.0f, 0.0f, 0.0f})) {
            List<MergedNodeScore> results = session.search(3, 0.0f, 1.0f);
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void shouldRemoveFlushedIndexesAfterGroupFlush(@TempDir Path tempDir) {
        // Behavior: after a group flush, flushed on-heap indexes are removed from the list.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        OnHeapVectorGraphIndex index = openOnHeap();
        index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

        // Add an empty index that won't be flushed
        OnHeapVectorGraphIndex emptyIndex = rotateOnHeap(index);

        group.flush(tempDir);

        // The flushed index should be removed; the empty one remains
        assertEquals(1, group.getOnHeapIndexes().size());
        assertSame(emptyIndex, group.getOnHeapIndexes().getFirst());
    }

    @Test
    void shouldFlushPQTrainedIndexAndLoadFromDisk() throws IOException {
        // Behavior: A PQ-trained on-heap index flushed via group produces .pqv files and the loaded on-disk index supports search.
        group = new VectorGraphIndexGroup(context, metadata, vectorIndex);

        OnHeapVectorGraphIndex index = openPqOnHeap();
        Random rng = new Random(42);
        float[] target = randomPqVector(rng);
        index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), target, executor).join();
        for (int i = 1; i < PQ_THRESHOLD; i++) {
            index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(i + 1), randomPqVector(rng), executor).join();
        }
        assertTrue(index.isPqTrained());

        Path bucketDir = context.getDataDir().resolve("bucket");
        group.flush(bucketDir);

        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(bucketDir, VectorGraphIndexGroup.ownerShardId(metadata))
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        assertTrue(Files.isDirectory(indexDir));
        try (Stream<Path> files = Files.list(indexDir)) {
            assertTrue(files.anyMatch(p -> p.toString().endsWith(".pqv")));
        }

        List<OnDiskVectorGraphIndex> result = VectorGraphIndexGroup.openOnDiskIndexes(
                context,
                vectorIndex.subspace(),
                bucketDir, VectorGraphIndexGroup.ownerShardId(metadata), metadata.uuid(), vectorIndex.definition().id(), VectorSimilarityFunction.COSINE);
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
        pqHeap.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), target, executor).join();
        for (int i = 1; i < PQ_THRESHOLD; i++) {
            pqHeap.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(i + 1), randomPqVector(rng), executor).join();
        }

        OnDiskVectorGraphIndex diskIndex = flushToOnDisk(tempDir, pqHeap, "pq-disk");
        group.addOnDisk(diskIndex);

        OnHeapVectorGraphIndex heapIndex = group.getOrCreateOnHeap(PQ_DIMENSIONS, VectorSimilarityFunction.COSINE, 0, 6);
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(100), randomPqVector(rng), executor).join();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(101), randomPqVector(rng), executor).join();

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

        try (Transaction tr = context.getTransactionManager().create()) {
            VectorIndexMaintainer.appendMutation(
                    tr,
                    vectorIndex.subspace(),
                    new ObjectId().toByteArray(),
                    MutationLogKind.INSERT,
                    1,
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
        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(context.getDataDir().resolve("bucket"), VectorGraphIndexGroup.ownerShardId(metadata))
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        Files.createDirectories(indexDir);

        String fakeId = new ObjectId().toHexString();
        Files.createFile(indexDir.resolve(fakeId + ".index"));
        Files.createFile(indexDir.resolve(fakeId + ".vmeta"));

        List<OnDiskVectorGraphIndex> result = VectorGraphIndexGroup.openOnDiskIndexes(
                context,
                vectorIndex.subspace(),
                context.getDataDir().resolve("bucket"), VectorGraphIndexGroup.ownerShardId(metadata), metadata.uuid(),
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
        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(context.getDataDir().resolve("bucket"), VectorGraphIndexGroup.ownerShardId(metadata))
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
        // Behavior: End-to-end: flush produces .complete sentinel -> openOnDiskIndexes recognizes valid
        // index set -> search returns results from the loaded on-disk index.
        OnHeapVectorGraphIndex heapIndex = newIndex();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        Path indexDir = VectorGraphIndexGroup.resolveVectorDir(context.getDataDir().resolve("bucket"), VectorGraphIndexGroup.ownerShardId(metadata))
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
                context,
                vectorIndex.subspace(),
                context.getDataDir().resolve("bucket"), VectorGraphIndexGroup.ownerShardId(metadata), metadata.uuid(),
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
    void shouldKeepNewerDeleteTombstoneWhenOlderIsPut() {
        // Behavior: A delete tombstone with an older versionstamp does not overwrite a newer one.
        ObjectId objectId = new ObjectId();
        Versionstamp newer = versionstamp(2);
        Versionstamp older = versionstamp(1);

        group.putDeleteTombstone(objectId, newer);
        group.putDeleteTombstone(objectId, older);

        assertEquals(newer, group.removeDeleteTombstone(objectId));
    }

    @Test
    void shouldReplaceDeleteTombstoneWhenNewerIsPut() {
        // Behavior: A delete tombstone with a newer versionstamp replaces the older one.
        ObjectId objectId = new ObjectId();
        Versionstamp older = versionstamp(1);
        Versionstamp newer = versionstamp(2);

        group.putDeleteTombstone(objectId, older);
        group.putDeleteTombstone(objectId, newer);

        assertEquals(newer, group.removeDeleteTombstone(objectId));
    }

    private VectorGraphIndexGroup registryGroup() {
        BucketService service = context.getService(BucketService.NAME);
        VectorGraphIndexGroup registryGroup = service.getVectorGraphRegistry().computeIfAbsent(
                metadata, vectorIndex, () -> service.bootstrapVectorGroup(metadata, vectorIndex));
        registryGroup.awaitReady();
        return registryGroup;
    }

    @Test
    void shouldTombstoneStaleAddWhenNewerNodeExists() {
        // Behavior: An add with an older versionstamp than the node already in the graph enters the graph
        // and is tombstoned at once. The graph counts the dead node until flush, the object stays bound to
        // the newer vector, and search returns only the newer vector.
        VectorGraphIndexGroup registryGroup = registryGroup();
        BucketService service = context.getService(BucketService.NAME);
        ObjectId objectId = new ObjectId();
        float[] oldVector = new float[]{1.0f, 0.0f, 0.0f};
        float[] newVector = new float[]{0.0f, 1.0f, 0.0f};
        CollectedVector newCv = new CollectedVector(objectId, 0, newEntryMetadata(2), newVector,
                vectorIndex.definition(), 3);
        CollectedVector oldCv = new CollectedVector(objectId, 0, newEntryMetadata(1), oldVector,
                vectorIndex.definition(), 1);

        VectorNodeWriter writer = new VectorNodeWriter(service, metadata);
        writer.write(newCv, versionstamp(3));
        OnHeapVectorGraphIndex onHeap = registryGroup.getOnHeapIndexes().getLast();
        GraphNodeRef newRef = onHeap.getMetadata().findNodeRef(objectId);
        assertNotNull(newRef);

        writer.write(oldCv, versionstamp(1));

        assertEquals(newRef, onHeap.getMetadata().findNodeRef(objectId));
        assertEquals(2, onHeap.size());
        List<MergedNodeScore> results = registryGroup.searchAll(oldVector, 2, 0.0f, 1.0f);
        assertEquals(1, results.size());
        assertEquals(newCv.metadata(), results.getFirst().location().entryMetadata());
    }

    @Test
    void shouldSkipStaleDeleteWhenNewerNodeExists() {
        // Behavior: A delete with an older versionstamp than the node in the graph does not remove the node
        // and stores no tombstone. A delete with a newer versionstamp removes the node.
        VectorGraphIndexGroup registryGroup = registryGroup();
        BucketService service = context.getService(BucketService.NAME);
        long vectorIndexId = vectorIndex.definition().id();
        ObjectId objectId = new ObjectId();
        CollectedVector cv = new CollectedVector(objectId, 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f},
                vectorIndex.definition(), 3);
        new VectorNodeWriter(service, metadata).write(cv, versionstamp(3));
        OnHeapVectorGraphIndex onHeap = registryGroup.getOnHeapIndexes().getLast();
        GraphNodeRef ref = onHeap.getMetadata().findNodeRef(objectId);
        assertNotNull(ref);

        VectorNodeRemover remover = new VectorNodeRemover(service, metadata, vectorIndexId);
        remover.remove(objectId, versionstamp(2));

        assertEquals(ref, onHeap.getMetadata().findNodeRef(objectId));
        assertNull(registryGroup.removeDeleteTombstone(objectId));

        remover = new VectorNodeRemover(service, metadata, vectorIndexId);
        remover.remove(objectId, versionstamp(4));

        assertNull(onHeap.getMetadata().findNodeRef(objectId));
    }

    @Test
    void shouldConvergeWhenLiveAddRacesWithStaleAdd() throws Exception {
        // Behavior: A stale add (older versionstamp, late post-commit hook) and a live newer add of the same
        // object run at the same time. In any order the object ends bound to the newer vector and the older vector is
        // not searchable.
        VectorGraphIndexGroup registryGroup = registryGroup();
        BucketService service = context.getService(BucketService.NAME);
        float[] oldVector = new float[]{1.0f, 0.0f, 0.0f};
        float[] newVector = new float[]{0.0f, 1.0f, 0.0f};
        for (int i = 0; i < 50; i++) {
            ObjectId objectId = new ObjectId();
            CollectedVector oldCv = new CollectedVector(objectId, 0, newEntryMetadata(1), oldVector,
                    vectorIndex.definition(), 1);
            CollectedVector newCv = new CollectedVector(objectId, 0, newEntryMetadata(2), newVector,
                    vectorIndex.definition(), 3);

            Thread stale = Thread.ofVirtual().start(() -> new VectorNodeWriter(service, metadata).write(oldCv, versionstamp(1)));
            Thread live = Thread.ofVirtual().start(() -> new VectorNodeWriter(service, metadata).write(newCv, versionstamp(3)));
            stale.join();
            live.join();

            GraphNodeRef ref = null;
            for (OnHeapVectorGraphIndex onHeap : registryGroup.getOnHeapIndexes()) {
                GraphNodeRef candidate = onHeap.getMetadata().findNodeRef(objectId);
                if (candidate != null) {
                    assertNull(ref, "object must be bound in one graph only");
                    ref = candidate;
                }
            }
            assertNotNull(ref);
            assertEquals(versionstamp(3), ref.versionstamp());

            List<MergedNodeScore> results = registryGroup.searchAll(oldVector, 4, 0.0f, 1.0f);
            for (MergedNodeScore result : results) {
                if (result.location().objectId().equals(objectId)) {
                    assertEquals(newCv.metadata(), result.location().entryMetadata());
                }
            }

            VectorNodeRemover remover = new VectorNodeRemover(service, metadata, vectorIndex.definition().id());
            remover.remove(objectId, versionstamp(4));
        }
    }

    @Test
    void shouldLeaveNoLiveNodeWhenAddRacesWithNewerDelete() throws Exception {
        // Behavior: An add and a newer delete of the same object run at the same time. In any order no live
        // node remains: the delete either marks the added node deleted or stores a tombstone that the add
        // consumes.
        VectorGraphIndexGroup registryGroup = registryGroup();
        BucketService service = context.getService(BucketService.NAME);
        long vectorIndexId = vectorIndex.definition().id();
        float[] vector = new float[]{1.0f, 0.0f, 0.0f};
        for (int i = 0; i < 50; i++) {
            ObjectId objectId = new ObjectId();
            CollectedVector cv = new CollectedVector(objectId, 0, newEntryMetadata(1), vector,
                    vectorIndex.definition(), 1);

            Thread add = Thread.ofVirtual().start(() -> new VectorNodeWriter(service, metadata).write(cv, versionstamp(1)));
            Thread delete = Thread.ofVirtual().start(() -> {
                VectorNodeRemover remover = new VectorNodeRemover(service, metadata, vectorIndexId);
                remover.remove(objectId, versionstamp(2));
            });
            add.join();
            delete.join();

            for (OnHeapVectorGraphIndex onHeap : registryGroup.getOnHeapIndexes()) {
                assertNull(onHeap.getMetadata().findNodeRef(objectId));
            }
            for (MergedNodeScore result : registryGroup.searchAll(vector, 4, 0.0f, 1.0f)) {
                assertNotEquals(objectId, result.location().objectId());
            }
        }
    }

    @Test
    void shouldWriteDeletedOrdinalToFoundationDB(@TempDir Path tempDir) throws IOException {
        // Behavior: markNodeDeleted on an on-disk index writes (DELETED_ORDINALS, baseName, ordinal) to FDB.
        OnHeapVectorGraphIndex heapIndex = newIndex();
        ObjectId objectId = new ObjectId();
        heapIndex.addGraphNode(objectId, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

        try (OnDiskVectorGraphIndex onDisk = flushToOnDisk(tempDir, heapIndex, "deleted-ordinal")) {
            int ordinal = onDisk.getMetadata().findNodeRef(objectId).ordinal();
            try (Transaction tr = context.getTransactionManager().create()) {
                onDisk.markNodeDeleted(tr, ordinal);
                tr.commit().join();
            }

            DirectorySubspace indexSubspace = vectorIndex.subspace();
            byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.DELETED_ORDINALS.getValue()));
            List<KeyValue> entries;
            try (Transaction tr = context.getTransactionManager().create()) {
                entries = tr.getRange(prefix, ByteArrayUtil.strinc(prefix)).asList().join();
            }
            assertEquals(1, entries.size());
            Tuple key = indexSubspace.unpack(entries.getFirst().getKey());
            assertEquals(ordinal, key.getLong(2));
            assertEquals(0, entries.getFirst().getValue().length);
        }
    }

    @Test
    void shouldExcludeDeletedOrdinalFromSearch(@TempDir Path tempDir) throws IOException {
        // Behavior: markNodeDeletedOnDisk writes the ordinal to FDB, sets the bit and the node leaves the search results.
        OnHeapVectorGraphIndex heapIndex = newIndex();
        ObjectId objectId = new ObjectId();
        heapIndex.addGraphNode(objectId, versionstamp(1), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), versionstamp(1), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

        OnDiskVectorGraphIndex onDisk = flushToOnDisk(tempDir, heapIndex, "exclude-deleted");
        group.addOnDisk(onDisk);
        int ordinal = onDisk.getMetadata().findNodeRef(objectId).ordinal();
        assertFalse(onDisk.isNodeDeleted(ordinal));
        assertEquals(2, group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 2, 0.0f, 1.0f).size());

        assertTrue(group.markNodeDeletedOnDisk(objectId, versionstamp(2)));

        assertTrue(onDisk.isNodeDeleted(ordinal));
        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 2, 0.0f, 1.0f);
        assertEquals(1, results.size());
        assertNotEquals(objectId, results.getFirst().location().objectId());
    }

    @Test
    void shouldNotMarkOnDiskNodeNewerThanDelete(@TempDir Path tempDir) throws IOException {
        // Behavior: A delete older than the on-disk node reports the node as found but does not hide it.
        OnHeapVectorGraphIndex heapIndex = newIndex();
        ObjectId objectId = new ObjectId();
        heapIndex.addGraphNode(objectId, versionstamp(5), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

        OnDiskVectorGraphIndex onDisk = flushToOnDisk(tempDir, heapIndex, "newer-than-delete");
        group.addOnDisk(onDisk);

        assertTrue(group.markNodeDeletedOnDisk(objectId, versionstamp(2)));
        assertFalse(onDisk.isNodeDeleted(onDisk.getMetadata().findNodeRef(objectId).ordinal()));
        assertFalse(group.markNodeDeletedOnDisk(new ObjectId(), versionstamp(2)));
    }

    @Test
    void shouldNotMarkOnDiskNodeWithSameVersionstampAsDelete(@TempDir Path tempDir) throws IOException {
        // Behavior: An UPDATE deletes the old node and adds the new node. Both carry the versionstamp of the
        // UPDATE. A delete with the same versionstamp as the on-disk node reports the node as found but must
        // not mark it deleted, because the node is not older than the delete.
        OnHeapVectorGraphIndex heapIndex = newIndex();
        ObjectId objectId = new ObjectId();
        heapIndex.addGraphNode(objectId, versionstamp(5), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

        OnDiskVectorGraphIndex onDisk = flushToOnDisk(tempDir, heapIndex, "same-vs");
        group.addOnDisk(onDisk);

        assertTrue(group.markNodeDeletedOnDisk(objectId, versionstamp(5)));
        assertFalse(onDisk.isNodeDeleted(onDisk.getMetadata().findNodeRef(objectId).ordinal()));
    }

    @Test
    void shouldLoadDeletedOrdinalsOnBootstrap() throws IOException {
        // Behavior: openOnDiskIndexes reads (DELETED_ORDINALS, baseName, ordinal) from FDB into the bitset.
        BucketService service = context.getService(BucketService.NAME);
        OnHeapVectorGraphIndex heapIndex = group.getOrCreateOnHeap(DIMENSIONS, VectorSimilarityFunction.COSINE, 0, 0);
        ObjectId objectId = new ObjectId();
        heapIndex.addGraphNode(objectId, versionstamp(1), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), versionstamp(1), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        group.flush(service.getBucketDataDir());

        OnDiskVectorGraphIndex flushed = group.getOnDiskIndexes().getFirst();
        int ordinal = flushed.getMetadata().findNodeRef(objectId).ordinal();
        try (Transaction tr = context.getTransactionManager().create()) {
            flushed.markNodeDeleted(tr, ordinal);
            tr.commit().join();
        }
        group.getOnDiskIndexes().remove(flushed);
        flushed.close();

        List<OnDiskVectorGraphIndex> reopened = VectorGraphIndexGroup.openOnDiskIndexes(
                context,
                vectorIndex.subspace(),
                service.getBucketDataDir(), VectorGraphIndexGroup.ownerShardId(metadata), metadata.uuid(),
                vectorIndex.definition().id(), VectorSimilarityFunction.COSINE);
        assertEquals(1, reopened.size());
        OnDiskVectorGraphIndex onDisk = reopened.getFirst();
        group.addOnDisk(onDisk);

        assertTrue(onDisk.isNodeDeleted(ordinal));
        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 2, 0.0f, 1.0f);
        assertEquals(1, results.size());
        assertNotEquals(objectId, results.getFirst().location().objectId());
    }

    @Test
    void shouldLoadDeletedOrdinalsPerOnDiskIndex() throws IOException {
        // Behavior: Deleted ordinals are keyed by the index base name. Ordinal 0 deleted in one on-disk index
        // stays live in another on-disk index after bootstrap.
        BucketService service = context.getService(BucketService.NAME);
        ObjectId objectIdA = new ObjectId();
        ObjectId objectIdB = new ObjectId();
        OnHeapVectorGraphIndex heapA = group.getOrCreateOnHeap(DIMENSIONS, VectorSimilarityFunction.COSINE, 0, 0);
        heapA.addGraphNode(objectIdA, versionstamp(1), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        group.flush(service.getBucketDataDir());
        OnHeapVectorGraphIndex heapB = group.getOrCreateOnHeap(DIMENSIONS, VectorSimilarityFunction.COSINE, 0, 0);
        heapB.addGraphNode(objectIdB, versionstamp(1), 0, newEntryMetadata(2), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
        group.flush(service.getBucketDataDir());

        List<OnDiskVectorGraphIndex> flushed = new ArrayList<>(group.getOnDiskIndexes());
        assertEquals(2, flushed.size());
        OnDiskVectorGraphIndex flushedA = flushed.stream()
                .filter(index -> index.getMetadata().findNodeRef(objectIdA) != null)
                .findFirst()
                .orElseThrow();
        assertEquals(0, flushedA.getMetadata().findNodeRef(objectIdA).ordinal());
        try (Transaction tr = context.getTransactionManager().create()) {
            flushedA.markNodeDeleted(tr, 0);
            tr.commit().join();
        }
        for (OnDiskVectorGraphIndex index : flushed) {
            group.getOnDiskIndexes().remove(index);
            index.close();
        }

        List<OnDiskVectorGraphIndex> reopened = VectorGraphIndexGroup.openOnDiskIndexes(
                context,
                vectorIndex.subspace(),
                service.getBucketDataDir(), VectorGraphIndexGroup.ownerShardId(metadata), metadata.uuid(),
                vectorIndex.definition().id(), VectorSimilarityFunction.COSINE);
        assertEquals(2, reopened.size());
        for (OnDiskVectorGraphIndex index : reopened) {
            group.addOnDisk(index);
            boolean holdsA = index.getMetadata().findNodeRef(objectIdA) != null;
            assertEquals(holdsA, index.isNodeDeleted(0));
        }

        List<MergedNodeScore> results = group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 2, 0.0f, 1.0f);
        assertEquals(1, results.size());
        assertEquals(objectIdB, results.getFirst().location().objectId());
    }

    @Test
    void shouldLoadDeletedOrdinalsAcrossPages() throws IOException {
        // Behavior: The deleted ordinal load is paged. A set larger than one page is read completely.
        BucketService service = context.getService(BucketService.NAME);
        int total = VectorIndexMaintainer.DELETED_ORDINALS_PAGE_SIZE + 1;
        OnHeapVectorGraphIndex heapIndex = group.getOrCreateOnHeap(DIMENSIONS, VectorSimilarityFunction.COSINE, 0, 0);
        Random rng = new Random(7);
        List<CompletableFuture<?>> adds = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            float[] vector = new float[]{rng.nextFloat(), rng.nextFloat(), rng.nextFloat()};
            adds.add(heapIndex.addGraphNode(new ObjectId(), versionstamp(1), 0, newEntryMetadata(i + 1), vector, executor));
        }
        adds.forEach(CompletableFuture::join);
        group.flush(service.getBucketDataDir());

        OnDiskVectorGraphIndex flushed = group.getOnDiskIndexes().getFirst();
        assertEquals(total, flushed.size());
        try (Transaction tr = context.getTransactionManager().create()) {
            for (int ordinal = 0; ordinal < total; ordinal++) {
                flushed.markNodeDeleted(tr, ordinal);
            }
            tr.commit().join();
        }
        group.getOnDiskIndexes().remove(flushed);
        flushed.close();

        List<OnDiskVectorGraphIndex> reopened = VectorGraphIndexGroup.openOnDiskIndexes(
                context,
                vectorIndex.subspace(),
                service.getBucketDataDir(), VectorGraphIndexGroup.ownerShardId(metadata), metadata.uuid(),
                vectorIndex.definition().id(), VectorSimilarityFunction.COSINE);
        assertEquals(1, reopened.size());
        OnDiskVectorGraphIndex onDisk = reopened.getFirst();
        group.addOnDisk(onDisk);
        for (int ordinal = 0; ordinal < total; ordinal++) {
            assertTrue(onDisk.isNodeDeleted(ordinal), "ordinal " + ordinal);
        }
        assertTrue(group.searchAll(new float[]{1.0f, 0.0f, 0.0f}, 5, 0.0f, 1.0f).isEmpty());
    }
}
