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

import java.util.concurrent.locks.Lock;

public class VectorNodeRemover extends BaseVectorNode {
    private final VectorGraphIndexGroup group;

    /**
     * Creates a remover for the given vector index and waits until its graph index group is ready.
     *
     * @param service       the bucket service
     * @param metadata      the bucket metadata
     * @param vectorIndexId the id of the vector index
     */
    public VectorNodeRemover(BucketService service, BucketMetadata metadata, long vectorIndexId) {
        super(service, metadata);
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
     * Marks the node of the given object as deleted in every on-heap and on-disk graph that holds it, unless the
     * node carries a newer versionstamp than the delete. If no graph holds the node yet, a delete tombstone is
     * stored.
     *
     * @param objectId the object id of the deleted vector
     * @param deleteVs the versionstamp of the delete operation
     */
    public void remove(ObjectId objectId, Versionstamp deleteVs) {
        Lock lock = group.lockFor(objectId);
        lock.lock();
        try {
            boolean[] foundOnHeap = {false};
            group.forEachOnHeap(onHeap -> {
                if (onHeap.getMetadata().findNodeRef(objectId) != null) {
                    foundOnHeap[0] = true;
                    // The versionstamp compare runs under the metadata lock, so a concurrent newer add
                    // cannot lose its mapping to this delete.
                    onHeap.markNodeDeleted(objectId, deleteVs);
                }
            });
            boolean foundOnDisk = group.markNodeDeletedOnDisk(objectId, deleteVs);
            boolean found = foundOnHeap[0] || foundOnDisk;

            // On-heap markNodeDeleted removes the ObjectId mapping. A second delete of the same object then
            // finds nothing and stores a tombstone for a node that is already gone.
            // This is harmless: a later add for the same ObjectId is either newer (tombstone is consumed,
            // node is added) or older (skipping it is correct). If no add comes, the tombstone stays in
            // the map. This is accepted.
            if (!found) {
                group.putDeleteTombstone(objectId, deleteVs);
            }
        } finally {
            lock.unlock();
        }
    }
}
