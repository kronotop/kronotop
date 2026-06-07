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

package com.kronotop.bucket.index.maintenance;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.ReservedFieldName;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.vector.OnHeapVectorGraphIndex;
import com.kronotop.bucket.vector.VectorGraphIndexGroup;
import com.kronotop.bucket.vector.VectorGraphIndexRegistry;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.VolumeEntry;
import com.kronotop.volume.VolumeSession;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

/**
 * Builds vector graph indexes on existing bucket data in the background.
 *
 * <p>Delegates graph construction to {@link VectorGraphIndexGroup}, flushing to disk
 * incrementally when the on-heap graph exceeds the configured RAM threshold. This keeps
 * memory usage bounded regardless of dataset size.
 *
 * @see AbstractBuildingRoutine
 * @see VectorIndexMaintainer
 */
public class VectorIndexBuildingRoutine extends AbstractBuildingRoutine {
    private VectorGraphIndexGroup group;
    private VectorIndexDefinition vectorDefinition;
    private VectorSimilarityFunction similarityFunction;

    public VectorIndexBuildingRoutine(
            Context context,
            DirectorySubspace subspace,
            int shardId,
            Versionstamp taskId,
            IndexBuildingTask task
    ) {
        super(context, subspace, shardId, taskId, task);
    }

    @Override
    protected IndexHolder<?> lookupIndex(BucketMetadata metadata) {
        return metadata.vectorIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
    }

    private void initGroup() {
        BucketMetadata metadata = TransactionUtil.execute(context, tr ->
                BucketMetadataUtil.reload(context, tr, task.getNamespace(), task.getBucket())
        );
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
        if (vectorIndex == null) {
            stopped = true;
            return;
        }
        vectorDefinition = vectorIndex.definition();
        similarityFunction = OnHeapVectorGraphIndex.toSimilarityFunction(vectorDefinition.distance());

        VectorGraphIndexRegistry registry = service.getVectorGraphRegistry();
        group = registry.computeIfAbsent(metadata, vectorIndex, () -> service.bootstrapVectorGroup(metadata, vectorIndex));
        group.awaitReady();
    }

    @Override
    protected int indexBucketEntries(Transaction tr, BucketShard shard,
                                     BucketMetadata metadata, IndexBuildingTaskState state) {
        int total = 0;

        VersionstampedKeySelector begin = !state.bootstrapped() ?
                VersionstampedKeySelector.firstGreaterOrEqual(state.cursorVersionstamp()) :
                VersionstampedKeySelector.firstGreaterThan(state.cursorVersionstamp());

        VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterOrEqual(task.getUpper());
        VolumeSession session = new VolumeSession(tr, metadata.prefix());

        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.READWRITE);
        if (vectorIndex == null) {
            throw new IndexMaintenanceRoutineException(
                    "Vector index with id " + task.getIndexId() + " is not in BUILDING or READY status");
        }
        Iterable<VolumeEntry> entries = shard.volume().getRange(session, begin, end, INDEX_SCAN_BATCH_SIZE);
        Versionstamp versionstamp = null;

        for (VolumeEntry pair : entries) {
            checkForShutdown();
            total++;
            versionstamp = pair.key();

            // Extract ObjectId
            BsonValue idValue = SelectorMatcher.match(ReservedFieldName.ID.getValue(), pair.entry());
            if (idValue == null || !idValue.isObjectId()) {
                throw new IndexMaintenanceRoutineException("Document missing _id field or _id is not an ObjectId");
            }
            ObjectId objectId = idValue.asObjectId().getValue();
            byte[] objectIdBytes = objectId.toByteArray();

            // Extract vector
            BsonValue vectorValue = SelectorMatcher.match(vectorDefinition.selector(), pair.entry());
            float[] vector = VectorIndexMaintainer.parseVector(vectorValue);

            if (vector == null) {
                // Document doesn't have the vector field — skip
                continue;
            }

            if (vector.length != vectorDefinition.dimensions()) {
                // Pre-existing document with wrong dimensions — skip
                continue;
            }

            // Write FDB entry
            VectorIndexMaintainer.insertEntry(tr, vectorIndex, metadata, versionstamp,
                    objectIdBytes, shardId, pair.metadata(), vector);

            // Add to the on-heap graph via VectorGraphIndexGroup.
            OnHeapVectorGraphIndex graph = group.getOrCreateOnHeap(
                    vectorDefinition.dimensions(), similarityFunction,
                    service.getPqTrainingThreshold(), service.getPqSubspaceDivisor());

            EntryMetadata entryMetadata = EntryMetadata.decode(pair.metadata());
            graph.addGraphNode(objectId, shardId, entryMetadata, vector, service.getVectorGraphExecutor()).join();
            graph.advanceVersionstamp(versionstamp);

            // Flush to disk when the threshold exceeded, then continue with a fresh graph
            if (graph.ramBytesUsed() > service.getVectorFlushThresholdBytes()) {
                OnHeapVectorGraphIndex previous = group.rotateOnHeap(
                        graph,
                        vectorDefinition.dimensions(), similarityFunction,
                        service.getPqTrainingThreshold(), service.getPqSubspaceDivisor());
                if (previous != null && !previous.isFlushed() && previous.size() > 0) {
                    group.flushSingle(service.getBucketDataDir(), previous);
                }
            }
        }

        if (!state.bootstrapped()) {
            IndexBuildingTaskState.setBootstrapped(tr, subspace, taskId, true);
        }

        setCursor(tr, versionstamp);
        return total;
    }

    private void flushRemaining() {
        if (group == null) {
            return;
        }
        group.flush(service.getBucketDataDir());
    }

    @Override
    public void start() {
        initGroup();
        if (stopped) {
            return;
        }

        super.start();

        if (!stopped) {
            flushRemaining();
        }
    }
}
