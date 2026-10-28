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

public class VectorNodeRemover extends BaseVectorNode {
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
        if (maxVs == null || deleteVs.compareTo(maxVs) > 0) {
            maxVs = deleteVs;
        }

        group.markNodeDeleted(objectId, deleteVs);
    }
}
