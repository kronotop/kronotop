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

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.CommitHook;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.VectorIndex;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Post-commit hook that adds new vector nodes to the on-heap vector graph index.
 */
public record VectorNodeAddHook(
        BucketService service,
        BucketMetadata metadata,
        List<CollectedVector> collectedVectors,
        CompletableFuture<byte[]> trVersionFuture
) implements CommitHook {
    private static final Logger LOGGER = LoggerFactory.getLogger(VectorNodeAddHook.class);

    private boolean consumeNewerDeleteTombstone(VectorGraphIndexGroup group, ObjectId objectId, Versionstamp addVs) {
        Versionstamp deleteVs = group.removeDeleteTombstone(objectId);
        return deleteVs != null && deleteVs.compareTo(addVs) > 0;
    }

    private VectorGraphIndexGroup awaitReadyGroup(long vectorIndexId) {
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexById(vectorIndexId, IndexSelectionPolicy.ALL);
        VectorGraphIndexGroup group = service.getVectorGraphRegistry().computeIfAbsent(
                metadata,
                vectorIndex,
                () -> service.bootstrapVectorGroup(metadata, vectorIndex)
        );

        group.awaitReady();
        return group;
    }

    private void recordFailedAdd(Versionstamp addVs, CollectedVector cv) {
        VectorGraphIndexGroup group = awaitReadyGroup(cv.vectorIndexId());
        group.recordFailedAdd(cv.objectId(), new RetryEntry(addVs, cv));
    }

    @Override
    public void run() {
        if (service.isShuttingDown()) {
            return;
        }
        byte[] trVersion = trVersionFuture.join();
        for (CollectedVector cv : collectedVectors) {
            Versionstamp addVs = Versionstamp.complete(trVersion, cv.userVersion());
            try {
                VectorGraphIndexGroup group = awaitReadyGroup(cv.vectorIndexId());

                // Pre-check: skip the expensive graph add if a newer DELETE tombstone already exists.
                if (consumeNewerDeleteTombstone(group, cv.objectId(), addVs)) {
                    continue;
                }

                OnHeapVectorGraphIndex graph = group.getOrCreateOnHeap(
                        cv.definition().dimensions(),
                        OnHeapVectorGraphIndex.toSimilarityFunction(cv.definition().distance()),
                        service.getPqTrainingThreshold(),
                        service.getPqSubspaceDivisor()
                );
                graph.addGraphNode(cv.objectId(), cv.shardId(), cv.metadata(), cv.vector(), service.getVectorGraphExecutor()).join();

                // Post-check: catch tombstones set by a concurrent DELETE during addGraphNode.
                if (consumeNewerDeleteTombstone(group, cv.objectId(), addVs)) {
                    int ordinal = graph.getMetadata().findOrdinal(cv.objectId());
                    if (ordinal >= 0) {
                        graph.markNodeDeleted(cv.objectId(), ordinal);
                    }
                    continue;
                }

                graph.advanceVersionstamp(addVs);

                if (graph.ramBytesUsed() > service.getVectorFlushThresholdBytes()) {
                    OnHeapVectorGraphIndex previous = group.rotateOnHeap(
                            graph,
                            cv.definition().dimensions(),
                            OnHeapVectorGraphIndex.toSimilarityFunction(cv.definition().distance()),
                            service.getPqTrainingThreshold(),
                            service.getPqSubspaceDivisor()
                    );
                    if (previous != null && !previous.isFlushed() && previous.size() > 0) {
                        service.getVectorGraphExecutor().submit(() -> group.flushSingle(service.getBucketDataDir(), previous));
                    }
                }
            } catch (Exception e) {
                recordFailedAdd(addVs, cv);
                LOGGER.warn("Failed to add vector node to on-heap graph, objectId={}, vectorIndexId={}, versionstamp={}, recorded a retry entry: {}",
                        cv.objectId(), cv.vectorIndexId(), addVs, e.toString());
                LOGGER.debug("Stack trace for failed vector node add, objectId={}", cv.objectId(), e);
            }
        }
    }
}
