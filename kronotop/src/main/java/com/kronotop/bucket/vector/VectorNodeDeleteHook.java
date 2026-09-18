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

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Post-commit hook that marks deleted nodes on the on-heap vector graph index.
 * Records a delete tombstone with the transaction versionstamp when the node
 * hasn't been added to the graph yet, preventing ghost nodes from out-of-order
 * hook execution across concurrent sessions.
 */
public final class VectorNodeDeleteHook extends BaseVectorNode implements CommitHook {
    private final long vectorIndexId;
    private final List<DeletedVector> deletedVectors;
    private final CompletableFuture<byte[]> trVersionFuture;

    public VectorNodeDeleteHook(
            BucketService bucketService,
            BucketMetadata metadata,
            long vectorIndexId,
            List<DeletedVector> deletedVectors,
            CompletableFuture<byte[]> trVersionFuture
    ) {
        super(bucketService, metadata);
        this.vectorIndexId = vectorIndexId;
        this.deletedVectors = deletedVectors;
        this.trVersionFuture = trVersionFuture;
    }

    @Override
    public void run() {
        VectorGraphIndexGroup group = awaitReadyGroup(vectorIndexId);

        byte[] trVersion = trVersionFuture.join();
        Versionstamp maxVs = null;

        for (DeletedVector dv : deletedVectors) {
            Versionstamp deleteVs = Versionstamp.complete(trVersion, dv.userVersion());
            if (maxVs == null || deleteVs.compareTo(maxVs) > 0) {
                maxVs = deleteVs;
            }

            boolean found = false;
            for (OnHeapVectorGraphIndex onHeap : group.getOnHeapIndexes()) {
                int ordinal = onHeap.getMetadata().findOrdinal(dv.objectId());
                if (ordinal >= 0) {
                    onHeap.markNodeDeleted(dv.objectId(), ordinal);
                    found = true;
                }
            }
            for (OnDiskVectorGraphIndex onDisk : group.getOnDiskIndexes()) {
                int ordinal = onDisk.getMetadata().findOrdinal(dv.objectId());
                if (ordinal >= 0) {
                    onDisk.markNodeDeleted(ordinal);
                    found = true;
                }
            }
            if (!found) {
                group.putDeleteTombstone(dv.objectId(), deleteVs);
            }
        }

        for (OnDiskVectorGraphIndex onDisk : group.getOnDiskIndexes()) {
            onDisk.flushMetadata();
        }

        if (maxVs != null) {
            List<OnHeapVectorGraphIndex> onHeaps = group.getOnHeapIndexes();
            if (!onHeaps.isEmpty()) {
                onHeaps.getLast().advanceVersionstamp(maxVs);
            }
        }
    }
}
