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
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import org.bson.types.ObjectId;

/**
 * Adds vector nodes to the on-heap vector graph index of a bucket.
 */
public final class VectorNodeWriter extends BaseVectorNode {

    public VectorNodeWriter(BucketService service, BucketMetadata metadata) {
        super(service, metadata);
    }

    private boolean consumeNewerDeleteTombstone(VectorGraphIndexGroup group, ObjectId objectId, Versionstamp addVs) {
        Versionstamp deleteVs = group.removeDeleteTombstone(objectId);
        return deleteVs != null && deleteVs.compareTo(addVs) > 0;
    }

    /**
     * Adds the vector as a node to the active on-heap graph. If the same object has a newer delete or a
     * newer node in any graph, the node is not added or is marked as deleted. When the graph grows past
     * the flush threshold, a new on-heap graph replaces it and the old one is flushed to disk in the background.
     *
     * @param cv    the vector and document metadata to add
     * @param addVs the versionstamp of the add operation
     */
    public void write(CollectedVector cv, Versionstamp addVs) {
        VectorGraphIndexGroup group = awaitReadyGroup(cv.definition().id());

        // An older add retry of this object must not run after this add. A later delete removes the
        // mapping, and a stale retry would then add the node back for a deleted document.
        group.discardStaleFailedAdd(cv.objectId(), addVs);

        // Pre-check: skip the expensive graph add if a newer DELETE tombstone already exists.
        if (consumeNewerDeleteTombstone(group, cv.objectId(), addVs)) {
            return;
        }

        OnHeapVectorGraphIndex graph = group.getOrCreateOnHeap(
                cv.definition().dimensions(),
                OnHeapVectorGraphIndex.toSimilarityFunction(cv.definition().distance()),
                service.getPqTrainingThreshold(),
                service.getPqSubspaceDivisor()
        );
        graph.addGraphNode(cv.objectId(), addVs, cv.shardId(), cv.metadata(), cv.vector(), service.getVectorGraphExecutor()).join();

        // Post-check: catch tombstones set by a concurrent DELETE during addGraphNode.
        Versionstamp deleteVs = group.removeDeleteTombstone(cv.objectId());
        if (deleteVs != null && deleteVs.compareTo(addVs) > 0) {
            try {
                graph.markNodeDeleted(cv.objectId(), deleteVs);
            } catch (IllegalStateException e) {
                // The graph was flushed after addGraphNode. The node is on disk, the delete retry finds it there.
                group.recordFailedOp(RetryEntry.delete(metadata, deleteVs, cv.definition().id(), cv.objectId()));
            }
            return;
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
    }
}
