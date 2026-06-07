/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.CommitHook;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.vector.OnDiskVectorGraphIndex;
import com.kronotop.bucket.vector.OnHeapVectorGraphIndex;
import com.kronotop.bucket.vector.VectorGraphIndexGroup;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Post-commit hook that marks deleted nodes on the on-heap vector graph index.
 * Records a delete tombstone with the transaction versionstamp when the node
 * hasn't been added to the graph yet, preventing ghost nodes from out-of-order
 * hook execution across concurrent sessions.
 */
record VectorNodeDeleteHook(
        BucketService bucketService, BucketMetadata metadata,
        long indexId, List<DeletedVector> deletedVectors,
        CompletableFuture<byte[]> trVersionFuture
) implements CommitHook {
    @Override
    public void run() {
        VectorGraphIndexGroup group = bucketService.getVectorGraphRegistry()
                .get(metadata.namespace(), metadata.name(), indexId);
        if (group == null) return;

        try {
            group.awaitReady();
        } catch (Exception e) {
            return;
        }

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
