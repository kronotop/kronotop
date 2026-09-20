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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Removes vector nodes from the graph indexes of a single vector index. One instance handles one batch of
 * deletes. Call {@link #remove} for each deleted vector, then {@link #flush} once at the end of the batch.
 */
public class VectorNodeRemover extends BaseVectorNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(VectorNodeRemover.class);
    private final long vectorIndexId;
    private final VectorGraphIndexGroup group;
    private Versionstamp maxVs = null;

    /**
     * Creates a remover for the given vector index and waits until its graph index group is ready.
     *
     * @param service       the bucket service
     * @param metadata      the bucket metadata
     * @param vectorIndexId the id of the vector index
     */
    public VectorNodeRemover(BucketService service, BucketMetadata metadata, long vectorIndexId) {
        super(service, metadata);
        this.vectorIndexId = vectorIndexId;
        this.group = awaitReadyGroup(vectorIndexId);
    }

    /**
     * Marks the node of the given vector as deleted in every on-heap and on-disk graph that holds it. If no graph
     * holds the node yet, a delete tombstone is stored, so a later add for the same object is skipped.
     *
     * @param dv        the deleted vector and its user version
     * @param trVersion the committed transaction version of the delete
     */
    public void remove(DeletedVector dv, byte[] trVersion) {
        remove(dv.objectId(), Versionstamp.complete(trVersion, dv.userVersion()));
    }

    /**
     * Marks the node of the given object as deleted in every on-heap and on-disk graph that holds it. If no graph
     * holds the node yet, a delete tombstone is stored. If the delete fails, a retry entry is recorded.
     *
     * @param objectId the object id of the deleted vector
     * @param deleteVs the versionstamp of the delete operation
     */
    public void remove(ObjectId objectId, Versionstamp deleteVs) {
        if (maxVs == null || deleteVs.compareTo(maxVs) > 0) {
            maxVs = deleteVs;
        }

        try {
            boolean found = false;
            for (OnHeapVectorGraphIndex onHeap : group.getOnHeapIndexes()) {
                int ordinal = onHeap.getMetadata().findOrdinal(objectId);
                if (ordinal >= 0) {
                    onHeap.markNodeDeleted(objectId, ordinal);
                    found = true;
                }
            }
            for (OnDiskVectorGraphIndex onDisk : group.getOnDiskIndexes()) {
                int ordinal = onDisk.getMetadata().findOrdinal(objectId);
                if (ordinal >= 0) {
                    onDisk.markNodeDeleted(ordinal);
                    found = true;
                }
            }
            if (!found) {
                group.putDeleteTombstone(objectId, deleteVs);
            }
        } catch (Exception e) {
            group.recordFailedOp(objectId, RetryEntry.delete(metadata, deleteVs, vectorIndexId, objectId));
            LOGGER.warn("Failed to delete vector node from graph indexes, objectId={}, vectorIndexId={}, versionstamp={}, recorded a retry entry: {}",
                    objectId, vectorIndexId, deleteVs, e.toString());
            LOGGER.debug("Stack trace for failed vector node delete, objectId={}", objectId, e);
        }
    }

    /**
     * Writes pending delete marks of the on-disk graphs to disk and advances the versionstamp of the active
     * on-heap graph to the newest delete in this batch. A flush failure is logged and does not stop the batch.
     */
    public void flush() {
        for (OnDiskVectorGraphIndex onDisk : group.getOnDiskIndexes()) {
            try {
                onDisk.flushMetadata();
            } catch (Exception exp) {
                LOGGER.error("Failed to flush on-disk vector index metadata, bucket={} vectorIndexId={}", metadata.uuid(), vectorIndexId, exp);
            }
        }

        if (maxVs != null) {
            List<OnHeapVectorGraphIndex> onHeaps = group.getOnHeapIndexes();
            if (!onHeaps.isEmpty()) {
                onHeaps.getLast().advanceVersionstamp(maxVs);
            }
        }
    }
}
