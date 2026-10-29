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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.VectorIndex;
import com.google.common.util.concurrent.Striped;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Holds the on-heap and on-disk vector graph indexes of one vector index definition.
 * Bootstraps, flushes, closes and searches them as one unit.
 *
 * <p>A group has two states:
 * <ul>
 *   <li>Not ready: background bootstrap is loading on-disk indexes and replaying the mutation log.
 *       All write operations are blocked until bootstrap completes.</li>
 *   <li>Ready: the group is available for reads and writes.
 *       A group created via the fast-path (no data to recover) is immediately ready.</li>
 * </ul>
 */
public class VectorGraphIndexGroup {
    private static final Logger LOGGER = LoggerFactory.getLogger(VectorGraphIndexGroup.class);
    private final ConcurrentSkipListMap<ObjectId, List<OnHeapVectorGraphIndex>> onHeapIndexes = new ConcurrentSkipListMap<>();
    private final List<OnDiskVectorGraphIndex> onDiskIndexes = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<ObjectId, Versionstamp> deleteTombstones = new ConcurrentHashMap<>();
    private final Striped<Lock> objectIdLocks = Striped.lock(2048);

    private final Context context;
    private final BucketService service;
    private final BucketMetadata metadata;
    private final VectorIndex vectorIndex;
    private final GenerationCounter generationCounter = new GenerationCounter(new ObjectId(), 100_000);
    private volatile CompletableFuture<Void> bootstrapFuture;

    /**
     * Creates a group that is ready at once. Used when there is no data to recover.
     */
    public VectorGraphIndexGroup(Context context, BucketMetadata metadata, VectorIndex vectorIndex) {
        this.context = context;
        this.service = context.getService(BucketService.NAME);
        this.metadata = metadata;
        this.vectorIndex = vectorIndex;
    }

    /**
     * Creates a group that becomes ready after a background bootstrap.
     *
     * @param bootstrapFuture completes when bootstrap finishes or fails
     */
    public VectorGraphIndexGroup(
            Context context,
            BucketMetadata metadata,
            VectorIndex vectorIndex,
            CompletableFuture<Void> bootstrapFuture
    ) {
        this(context, metadata, vectorIndex);
        this.bootstrapFuture = bootstrapFuture;
    }

    /**
     * Resolves the root directory for vector index files of the given shard.
     */
    public static Path resolveVectorDir(Path bucketDataDir, int shardId) {
        return bucketDataDir
                .resolve("shards")
                .resolve(Integer.toString(shardId))
                .resolve("derived-indexes")
                .resolve("vector");
    }

    /**
     * Returns the shard that owns the vector index files of the bucket.
     */
    public static int ownerShardId(BucketMetadata metadata) {
        return metadata.shards().getFirst();
    }

    /**
     * Opens the flushed on-disk indexes in the given directory. A file set is loaded only if its
     * {@code .complete} marker exists, which means the flush finished. Files without a marker
     * are deleted.
     *
     * @return the opened indexes, or an empty list if the directory does not exist or holds no complete file set
     */
    public static List<OnDiskVectorGraphIndex> openOnDiskIndexes(
            Context context,
            DirectorySubspace indexSubspace,
            Path bucketDataDir,
            int shardId,
            UUID bucketId,
            long indexId,
            VectorSimilarityFunction similarityFunction
    ) throws IOException {
        Path indexDir = resolveVectorDir(bucketDataDir, shardId)
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
                OnDiskVectorGraphIndex onDisk = new OnDiskVectorGraphIndex(indexSubspace, indexFile, similarityFunction);
                opened.add(onDisk);
                onDisk.loadDeletedOrdinals(context);
            }
        } catch (IOException | RuntimeException e) {
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

    private static OnHeapVectorGraphIndex lastOf(List<OnHeapVectorGraphIndex> list) {
        if (list == null) {
            return null;
        }
        try {
            return list.getLast();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    /**
     * Counts one use of the current generation and returns its id as bytes.
     * The array is shared across callers of the same generation; do not modify it.
     * Lock-free.
     */
    public byte[] takeGenerationId() {
        return generationCounter.take();
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
     * Marks bootstrap as finished. The group becomes ready.
     */
    public void markReady() {
        CompletableFuture<Void> future = bootstrapFuture;
        if (future != null) {
            future.complete(null);
        }
    }

    /**
     * Marks bootstrap as failed. The group never becomes ready.
     */
    public void markFailed(Throwable cause) {
        CompletableFuture<Void> future = bootstrapFuture;
        if (future != null) {
            future.completeExceptionally(cause);
        }
    }

    /**
     * Returns {@code true} if bootstrap finished without error, or if the group needed no bootstrap.
     */
    public boolean isReady() {
        CompletableFuture<Void> future = bootstrapFuture;
        if (future == null) return true;
        return future.isDone() && !future.isCompletedExceptionally();
    }

    public BucketMetadata getBucketMetadata() {
        return metadata;
    }

    /**
     * Returns the striped lock that guards add and delete of the given object.
     */
    public Lock lockFor(ObjectId objectId) {
        return objectIdLocks.get(objectId);
    }

    /**
     * Returns the vector index this group serves.
     */
    public VectorIndex getVectorIndex() {
        return vectorIndex;
    }

    /**
     * Adds an on-disk index to this group.
     */
    public void addOnDisk(OnDiskVectorGraphIndex index) {
        onDiskIndexes.add(index);
    }

    public void enqueueAllForFlush() {
        for (ObjectId generationId : onHeapIndexes.keySet()) {
            enqueueGenerationForFlush(generationId, true);
        }
    }

    /**
     * Adds a flush task for the given generation. In drain mode the task is due at once.
     */
    public void enqueueGenerationForFlush(ObjectId genId, boolean drainMode) {
        VectorFlushKey key = new VectorFlushKey(metadata.namespace(), metadata.name(), vectorIndex.definition().id(), genId);
        long due = 0;
        if (!drainMode) {
            due = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        }
        VectorFlushTask task = new VectorFlushTask(key, due);
        service.getVectorFlushQueue().add(task);
    }

    private void enqueuePreviousGenerationsForFlush(ObjectId genId) {
        for (ObjectId prevGenId : onHeapIndexes.headMap(genId).keySet()) {
            enqueueGenerationForFlush(prevGenId, false);
        }
    }

    private OnHeapVectorGraphIndex createOnHeap(ObjectId genId, int dimensions, VectorSimilarityFunction similarityFunction,
                                                int pqTrainingThreshold, int pqSubspaceDivisor) {
        List<OnHeapVectorGraphIndex> list = onHeapIndexes.computeIfAbsent(genId, (ignored) -> new CopyOnWriteArrayList<>());

        OnHeapVectorGraphIndex fresh = new OnHeapVectorGraphIndex(dimensions, similarityFunction,
                pqTrainingThreshold, pqSubspaceDivisor);
        list.add(fresh);
        enqueuePreviousGenerationsForFlush(genId);
        return fresh;
    }

    /**
     * Returns the newest on-heap index of the given generation, creating one if the generation has none.
     * The read path is lock-free; creation takes the group monitor to prevent duplicates.
     */
    public OnHeapVectorGraphIndex getOrCreateOnHeap(
            byte[] generationId,
            int dimensions, VectorSimilarityFunction similarityFunction,
            int pqTrainingThreshold, int pqSubspaceDivisor) {
        ObjectId genId = new ObjectId(generationId);
        OnHeapVectorGraphIndex current = lastOf(onHeapIndexes.get(genId));
        if (current != null) {
            return current;
        }
        synchronized (this) {
            current = lastOf(onHeapIndexes.get(genId));
            if (current != null) {
                return current;
            }
            return createOnHeap(genId, dimensions, similarityFunction, pqTrainingThreshold, pqSubspaceDivisor);
        }
    }

    /**
     * Adds a fresh on-heap index to the given generation if {@code expected} is still its newest index.
     * Does nothing if another thread already rotated.
     */
    public synchronized void rotateOnHeap(
            byte[] generationId,
            OnHeapVectorGraphIndex expected,
            int dimensions, VectorSimilarityFunction similarityFunction,
            int pqTrainingThreshold, int pqSubspaceDivisor) {
        ObjectId genId = new ObjectId(generationId);
        if (lastOf(onHeapIndexes.get(genId)) == expected) {
            // Newest index of the generation is full and needs rotation.
            createOnHeap(genId, dimensions, similarityFunction, pqTrainingThreshold, pqSubspaceDivisor);
        }
    }

    /**
     * Visits the on-heap indexes in this group, ordered by generation id, without copying.
     */
    public void forEachOnHeap(Consumer<OnHeapVectorGraphIndex> action) {
        for (List<OnHeapVectorGraphIndex> list : onHeapIndexes.values()) {
            for (OnHeapVectorGraphIndex index : list) {
                action.accept(index);
            }
        }
    }

    /**
     * Visits the on-heap indexes of the given generation. Does nothing if the generation has none.
     */
    public void forEachOnHeap(ObjectId generationId, Consumer<OnHeapVectorGraphIndex> action) {
        List<OnHeapVectorGraphIndex> generation = onHeapIndexes.get(generationId);
        if (generation == null) {
            return;
        }
        for (OnHeapVectorGraphIndex index : generation) {
            action.accept(index);
        }
    }

    /**
     * Returns a snapshot of the on-heap indexes in this group, ordered by generation id.
     * Intended for tests; production code uses {@link #forEachOnHeap(Consumer)}.
     */
    List<OnHeapVectorGraphIndex> getOnHeapIndexes() {
        List<OnHeapVectorGraphIndex> snapshot = new ArrayList<>();
        for (List<OnHeapVectorGraphIndex> list : onHeapIndexes.values()) {
            snapshot.addAll(list);
        }
        return snapshot;
    }

    private synchronized void removeOnHeap(OnHeapVectorGraphIndex index) {
        Iterator<Map.Entry<ObjectId, List<OnHeapVectorGraphIndex>>> it = onHeapIndexes.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<ObjectId, List<OnHeapVectorGraphIndex>> entry = it.next();
            if (entry.getValue().remove(index)) {
                if (entry.getValue().isEmpty()) {
                    it.remove();
                }
                return;
            }
        }
    }

    /**
     * Marks the node of the object as deleted in every on-disk index that holds it, unless the node carries a
     * newer versionstamp than the delete.
     *
     * @return {@code true} if at least one on-disk index holds the node
     */
    public boolean markNodeDeletedOnDisk(ObjectId objectId, Versionstamp deleteVs) {
        if (onDiskIndexes.isEmpty()) {
            return false;
        }
        boolean found = false;
        List<MarkedNode> marked = new ArrayList<>();
        try (Transaction tr = context.getTransactionManager().create()) {
            for (OnDiskVectorGraphIndex onDisk : onDiskIndexes) {
                GraphNodeRef ref = onDisk.getMetadata().findNodeRef(objectId);
                if (ref == null) {
                    continue;
                }
                found = true;
                if (onDisk.isNodeDeleted(ref.ordinal())) {
                    continue;
                }
                if (deleteVs.compareTo(ref.versionstamp()) > 0) {
                    onDisk.markNodeDeleted(tr, ref.ordinal());
                    marked.add(new MarkedNode(onDisk, ref.ordinal()));
                }
            }
            if (!marked.isEmpty()) {
                tr.commit().join();
            }
        }
        for (MarkedNode node : marked) {
            node.index().markNodeDeletedInMemory(node.ordinal());
        }
        return found;
    }

    public boolean hasNodeOnDisk(ObjectId objectId, Versionstamp versionstamp) {
        for (OnDiskVectorGraphIndex onDisk : onDiskIndexes) {
            GraphNodeRef ref = onDisk.getMetadata().findNodeRef(objectId);
            if (ref != null && ref.versionstamp().compareTo(versionstamp) >= 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the live list of on-disk indexes in this group.
     */
    public List<OnDiskVectorGraphIndex> getOnDiskIndexes() {
        return onDiskIndexes;
    }

    /**
     * Returns {@code true} if the group has no on-heap and no on-disk indexes.
     */
    public boolean isEmpty() {
        return onHeapIndexes.isEmpty() && onDiskIndexes.isEmpty();
    }

    public boolean hasNoOnHeapIndexes(ObjectId generationId) {
        List<OnHeapVectorGraphIndex> list = onHeapIndexes.get(generationId);
        if (list == null) {
            return true;
        }
        return list.isEmpty();
    }

    /**
     * Opens a search session over every searchable index of the group for the given query vector.
     */
    public VectorSearchSession createSearchSession(float[] queryVector) {
        List<SearchHandle> handles = new ArrayList<>();
        for (List<OnHeapVectorGraphIndex> list : onHeapIndexes.values()) {
            for (OnHeapVectorGraphIndex index : list) {
                openSearchHandle(index, queryVector, handles);
            }
        }
        for (OnDiskVectorGraphIndex index : onDiskIndexes) {
            openSearchHandle(index, queryVector, handles);
        }
        return new VectorSearchSession(handles);
    }

    private void openSearchHandle(SearchableVectorIndex index, float[] queryVector, List<SearchHandle> handles) {
        if (!index.isSearchable()) return;
        handles.add(index.openSearchHandle(queryVector));
    }

    /**
     * Searches every on-heap and on-disk index and returns the merged top-K result.
     */
    public List<MergedNodeScore> searchAll(float[] queryVector, int topK, float threshold, float overquery) {
        if (isEmpty()) {
            return List.of();
        }

        try (VectorSearchSession session = createSearchSession(queryVector)) {
            return session.search(topK, threshold, overquery);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes the on-heap index to disk, opens it as an on-disk index and removes the on-heap one
     * from the group. An empty index is skipped. On a write error the on-heap index stays in place,
     * so the flush maintainer can try again.
     */
    public void flush(Path bucketDataDir, OnHeapVectorGraphIndex index) {
        if (index.size() == 0) return;

        Path indexDir = resolveVectorDir(bucketDataDir, ownerShardId(metadata))
                .resolve(metadata.uuid().toString())
                .resolve(Long.toString(vectorIndex.definition().id()));
        Path indexPath;
        try {
            Files.createDirectories(indexDir);
            indexPath = index.flush(indexDir);
        } catch (IOException | RuntimeException e) {
            LOGGER.error("Failed to flush vector graph index to {}", indexDir, e);
            service.getMetrics().incrementVectorFlushFailures();
            return;
        }

        // Add the on-disk index before removing the on-heap index. Otherwise, a delete that runs
        // in between finds the node in no list, stores a tombstone, and the delete is lost.
        if (indexPath != null) {
            try {
                onDiskIndexes.add(new OnDiskVectorGraphIndex(vectorIndex.subspace(), indexPath, index.getSimilarityFunction()));
            } catch (IOException e) {
                LOGGER.error("Failed to open flushed vector graph index from {}", indexPath, e);
                service.getMetrics().incrementVectorFlushFailures();
            }
        }

        removeOnHeap(index);
        try {
            index.close();
        } catch (IOException e) {
            LOGGER.error("Failed to close flushed vector graph index", e);
        }
    }

    /**
     * Records a delete tombstone for an ObjectId whose graph node hasn't been added yet.
     * An older versionstamp does not overwrite an existing newer tombstone.
     */
    public void putDeleteTombstone(ObjectId objectId, Versionstamp versionstamp) {
        deleteTombstones.merge(objectId, versionstamp, (existing, incoming) ->
                incoming.compareTo(existing) >= 0 ? incoming : existing);
    }

    /**
     * Removes and returns the delete tombstone for the given ObjectId, or null if none exists.
     */
    public Versionstamp removeDeleteTombstone(ObjectId objectId) {
        return deleteTombstones.remove(objectId);
    }

    /**
     * Closes all on-heap and on-disk indexes.
     */
    public void closeAll() {
        IOException firstException = null;

        for (List<OnHeapVectorGraphIndex> list : onHeapIndexes.values()) {
            for (OnHeapVectorGraphIndex index : list) {
                firstException = close(index, firstException);
            }
        }
        for (OnDiskVectorGraphIndex index : onDiskIndexes) {
            firstException = close(index, firstException);
        }

        if (firstException != null) {
            throw new UncheckedIOException(firstException);
        }
    }

    private IOException close(SearchableVectorIndex index, IOException firstException) {
        try {
            index.close();
        } catch (IOException e) {
            if (firstException == null) return e;
            firstException.addSuppressed(e);
        }
        return firstException;
    }

    private record MarkedNode(OnDiskVectorGraphIndex index, int ordinal) {
    }
}
