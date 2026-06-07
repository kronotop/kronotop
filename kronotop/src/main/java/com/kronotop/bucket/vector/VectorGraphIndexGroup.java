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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.collect.Iterables;
import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.VectorIndex;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

/**
 * Manages a collection of on-heap and on-disk vector graph indexes for a single vector index definition.
 * Provides lifecycle management (bootstrap, flush, close) and unified search across all contained indexes.
 *
 * <p>A group transitions through two states:
 * <ul>
 *   <li><b>Not ready</b> — background bootstrap is loading on-disk indexes and replaying the mutation log.
 *       All write operations are blocked until bootstrap completes.</li>
 *   <li><b>Ready</b> — the group is fully initialized and available for reads and writes.
 *       A group created via the fast-path (no data to recover) is immediately ready.</li>
 * </ul>
 */
public class VectorGraphIndexGroup {
    private static final Logger LOGGER = LoggerFactory.getLogger(VectorGraphIndexGroup.class);
    public static String VECTOR_INDEX_DIR = "vector-indexes";
    private final List<OnHeapVectorGraphIndex> onHeapIndexes = new CopyOnWriteArrayList<>();
    private final List<OnDiskVectorGraphIndex> onDiskIndexes = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<ObjectId, Versionstamp> deleteTombstones = new ConcurrentHashMap<>();

    private final Context context;
    private final BucketMetadata metadata;
    private final VectorIndex vectorIndex;
    private volatile CompletableFuture<Void> bootstrapFuture;

    /**
     * Creates an immediately ready group (fast-path) with no pending bootstrap work.
     */
    public VectorGraphIndexGroup(Context context, BucketMetadata metadata, VectorIndex vectorIndex) {
        this.context = context;
        this.metadata = metadata;
        this.vectorIndex = vectorIndex;
    }

    /**
     * Creates a group that requires background bootstrap before it becomes ready (slow-path).
     *
     * @param bootstrapFuture the future that will be completed when bootstrap finishes or fails
     */
    public VectorGraphIndexGroup(
            Context context,
            BucketMetadata metadata,
            VectorIndex vectorIndex,
            CompletableFuture<Void> bootstrapFuture
    ) {
        this.context = context;
        this.metadata = metadata;
        this.vectorIndex = vectorIndex;
        this.bootstrapFuture = bootstrapFuture;
    }

    /**
     * Resolves the root directory where all vector index files are stored under the given base directory.
     */
    public static Path resolveVectorDir(Path baseDir) {
        return baseDir.resolve(VECTOR_INDEX_DIR);
    }

    /**
     * Opens previously flushed on-disk indexes from the given directory. Only loads file sets
     * that have a {@code .complete} sentinel, indicating the flush finished successfully.
     * Orphaned files without a sentinel are deleted.
     *
     * @return opened indexes, or an empty list if the directory doesn't exist or contains no valid index sets
     */
    public static List<OnDiskVectorGraphIndex> openOnDiskIndexes(
            Path bucketDataDir,
            UUID bucketId,
            long indexId,
            VectorSimilarityFunction similarityFunction
    ) throws IOException {
        Path indexDir = resolveVectorDir(bucketDataDir)
                .resolve(bucketId.toString())
                .resolve(Long.toString(indexId));
        if (!Files.isDirectory(indexDir)) {
            return List.of();
        }

        List<String> completeBaseNames;
        try (Stream<Path> stream = Files.list(indexDir)) {
            completeBaseNames = stream
                    .filter(p -> p.getFileName().toString().endsWith(".complete"))
                    .map(p -> {
                        String fileName = p.getFileName().toString();
                        return fileName.substring(0, fileName.lastIndexOf('.'));
                    })
                    .toList();
        }

        // Delete orphaned files whose base name has no .complete sentinel
        try (Stream<Path> stream = Files.list(indexDir)) {
            for (Path file : stream.toList()) {
                String fileName = file.getFileName().toString();
                int dotIndex = fileName.lastIndexOf('.');
                if (dotIndex < 0) continue;
                if (fileName.endsWith(".complete")) continue;
                String baseName = fileName.substring(0, dotIndex);
                if (!completeBaseNames.contains(baseName)) {
                    LOGGER.warn("Deleting orphaned vector index file: {}", file);
                    Files.deleteIfExists(file);
                }
            }
        }

        if (completeBaseNames.isEmpty()) {
            return List.of();
        }

        List<OnDiskVectorGraphIndex> opened = new ArrayList<>();
        try {
            for (String baseName : completeBaseNames) {
                Path indexFile = indexDir.resolve(baseName + ".index");
                opened.add(new OnDiskVectorGraphIndex(indexFile, similarityFunction));
            }
        } catch (IOException e) {
            for (OnDiskVectorGraphIndex idx : opened) {
                try {
                    idx.close();
                } catch (IOException closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            throw e;
        }
        return opened;
    }

    /**
     * Blocks the calling thread until bootstrap completes. No-op for fast-path groups.
     *
     * @throws java.util.concurrent.CompletionException if bootstrap failed
     */
    public void awaitReady() {
        CompletableFuture<Void> future = bootstrapFuture;
        if (future != null) {
            future.join();
        }
    }

    /**
     * Signals that background bootstrap completed successfully, transitioning the group to the ready state.
     */
    public void markReady() {
        CompletableFuture<Void> future = bootstrapFuture;
        if (future != null) {
            future.complete(null);
        }
    }

    /**
     * Signals that background bootstrap failed. The group remains permanently not-ready.
     */
    public void markFailed(Throwable cause) {
        CompletableFuture<Void> future = bootstrapFuture;
        if (future != null) {
            future.completeExceptionally(cause);
        }
    }

    /**
     * Returns {@code true} if the group is fully initialized and available for reads and writes.
     */
    public boolean isReady() {
        CompletableFuture<Void> future = bootstrapFuture;
        if (future == null) return true;
        return future.isDone() && !future.isCompletedExceptionally();
    }

    /**
     * Returns the UUID of the bucket this group belongs to.
     */
    public UUID getBucketId() {
        return metadata.uuid();
    }

    /**
     * Adds an on-heap index to this group.
     */
    public void addOnHeap(OnHeapVectorGraphIndex index) {
        onHeapIndexes.add(index);
    }

    /**
     * Adds an on-disk index to this group.
     */
    public void addOnDisk(OnDiskVectorGraphIndex index) {
        onDiskIndexes.add(index);
    }

    /**
     * Returns the active (last) on-heap index, creating one if none exists.
     * Synchronized per-group instance to prevent duplicate creation from concurrent threads.
     */
    public synchronized OnHeapVectorGraphIndex getOrCreateOnHeap(
            int dimensions, VectorSimilarityFunction similarityFunction,
            int pqTrainingThreshold, int pqSubspaceDivisor) {
        if (!onHeapIndexes.isEmpty()) {
            return onHeapIndexes.getLast();
        }
        OnHeapVectorGraphIndex graph = new OnHeapVectorGraphIndex(dimensions, similarityFunction,
                pqTrainingThreshold, pqSubspaceDivisor);
        onHeapIndexes.add(graph);
        return graph;
    }

    /**
     * Atomically replaces the active on-heap index if it is still {@code expected}.
     * Returns {@code expected} (the previous active index to be flushed), or {@code null}
     * if another thread already rotated.
     */
    public synchronized OnHeapVectorGraphIndex rotateOnHeap(
            OnHeapVectorGraphIndex expected,
            int dimensions, VectorSimilarityFunction similarityFunction,
            int pqTrainingThreshold, int pqSubspaceDivisor) {
        if (onHeapIndexes.isEmpty() || onHeapIndexes.getLast() != expected) {
            return null;
        }
        OnHeapVectorGraphIndex fresh = new OnHeapVectorGraphIndex(dimensions, similarityFunction,
                pqTrainingThreshold, pqSubspaceDivisor);
        onHeapIndexes.add(fresh);
        return expected;
    }

    /**
     * Returns the live list of on-heap indexes in this group.
     */
    public List<OnHeapVectorGraphIndex> getOnHeapIndexes() {
        return onHeapIndexes;
    }

    /**
     * Returns the live list of on-disk indexes in this group.
     */
    public List<OnDiskVectorGraphIndex> getOnDiskIndexes() {
        return onDiskIndexes;
    }

    /**
     * Returns {@code true} if the group contains no indexes of either type.
     */
    public boolean isEmpty() {
        return onHeapIndexes.isEmpty() && onDiskIndexes.isEmpty();
    }

    private Iterable<SearchableVectorIndex> allIndexes() {
        return Iterables.concat(onHeapIndexes, onDiskIndexes);
    }

    /**
     * Creates a resumable search session across all active indexes for the given query vector.
     */
    public VectorSearchSession createSearchSession(float[] queryVector) {
        List<SearchHandle> handles = new ArrayList<>();
        for (SearchableVectorIndex index : allIndexes()) {
            if (!index.isSearchable()) continue;
            try {
                handles.add(index.openSearchHandle(queryVector));
            } catch (IllegalStateException e) {
                // On-heap index flushed between isSearchable() and openSearchHandle()
            }
        }
        return new VectorSearchSession(handles);
    }

    /**
     * Searches all on-heap and on-disk indexes and merges results into a single top-K list using a min-heap.
     */
    public List<MergedNodeScore> searchAll(float[] queryVector, int topK, float threshold, float overquery) {
        if (onHeapIndexes.isEmpty() && onDiskIndexes.isEmpty()) {
            return List.of();
        }

        try (VectorSearchSession session = createSearchSession(queryVector)) {
            return session.search(topK, threshold, overquery);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void truncateMutationLog(Versionstamp firstVersionstamp, Versionstamp latestVersionstamp) {
        if (firstVersionstamp == null || latestVersionstamp == null) {
            return;
        }
        DirectorySubspace indexSubspace = vectorIndex.subspace();
        byte[] begin = indexSubspace.pack(
                Tuple.from(IndexSubspaceMagic.MUTATION_LOG.getValue(), firstVersionstamp)
        );
        byte[] end = ByteArrayUtil.strinc(
                indexSubspace.pack(
                        Tuple.from(IndexSubspaceMagic.MUTATION_LOG.getValue(), latestVersionstamp)
                )
        );
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.clear(begin, end);
            tr.commit().join();
        }
    }

    /**
     * Flushes a single on-heap index to disk and promotes it to an on-disk index.
     */
    public void flushSingle(Path bucketDataDir, OnHeapVectorGraphIndex index) {
        if (index.isFlushed() || index.size() == 0) return;

        Versionstamp firstVersionstamp = index.getFirstVersionstamp();
        Versionstamp latestVersionstamp = index.getLatestVersionstamp();
        Path indexDir = resolveVectorDir(bucketDataDir)
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        Path indexPath;
        try {
            Files.createDirectories(indexDir);
            indexPath = index.flush(indexDir);
        } catch (IOException e) {
            LOGGER.error("Failed to flush vector graph index to {}", indexDir, e);
            return;
        }

        onHeapIndexes.remove(index);
        try {
            index.close();
        } catch (IOException e) {
            LOGGER.error("Failed to close flushed vector graph index", e);
        }

        if (indexPath != null) {
            try {
                onDiskIndexes.add(new OnDiskVectorGraphIndex(indexPath, index.getSimilarityFunction()));
            } catch (IOException e) {
                LOGGER.error("Failed to open flushed vector graph index from {}", indexPath, e);
            }
        }

        if (firstVersionstamp == null || latestVersionstamp == null) {
            LOGGER.warn("Skipping mutation log truncation for on-heap VectorGraphIndex {}: " +
                    "no versionstamp range recorded, advanceVersionstamp was never called", index.getId());
            return;
        }
        truncateMutationLog(firstVersionstamp, latestVersionstamp);
    }

    /**
     * Flushes all unflushed, non-empty on-heap indexes to disk under the given data directory.
     */
    public void flush(Path bucketDataDir) {
        for (OnHeapVectorGraphIndex index : onHeapIndexes) {
            if (index.isFlushed() || index.size() == 0) continue;
            flushSingle(bucketDataDir, index);
        }
    }

    /**
     * Records a delete tombstone for an ObjectId whose graph node hasn't been added yet.
     */
    public void putDeleteTombstone(ObjectId objectId, Versionstamp versionstamp) {
        deleteTombstones.put(objectId, versionstamp);
    }

    /**
     * Removes and returns the delete tombstone for the given ObjectId, or null if none exists.
     */
    public Versionstamp removeDeleteTombstone(ObjectId objectId) {
        return deleteTombstones.remove(objectId);
    }

    /**
     * Closes all indexes in both lists.
     */
    public void closeAll() {
        IOException firstException = null;

        for (SearchableVectorIndex index : allIndexes()) {
            try {
                index.close();
            } catch (IOException e) {
                if (firstException == null) firstException = e;
                else firstException.addSuppressed(e);
            }
        }

        if (firstException != null) {
            throw new UncheckedIOException(firstException);
        }
    }
}
