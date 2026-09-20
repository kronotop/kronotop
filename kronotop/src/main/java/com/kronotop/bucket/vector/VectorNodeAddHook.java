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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Post-commit hook that adds new vector nodes to the on-heap vector graph index.
 */
public final class VectorNodeAddHook extends BaseVectorNode implements CommitHook {
    private static final Logger LOGGER = LoggerFactory.getLogger(VectorNodeAddHook.class);
    private final List<CollectedVector> collectedVectors;
    private final CompletableFuture<byte[]> trVersionFuture;

    public VectorNodeAddHook(
            BucketService service,
            BucketMetadata metadata,
            List<CollectedVector> collectedVectors,
            CompletableFuture<byte[]> trVersionFuture
    ) {
        super(service, metadata);
        this.collectedVectors = collectedVectors;
        this.trVersionFuture = trVersionFuture;
    }

    private void recordFailedAdd(Versionstamp addVs, CollectedVector cv) {
        VectorGraphIndexGroup group = awaitReadyGroup(cv.vectorIndexId());
        group.recordFailedOp(cv.objectId(), RetryEntry.add(metadata, addVs, cv));
    }

    @Override
    public void run() {
        if (service.isShuttingDown()) {
            return;
        }
        byte[] trVersion = trVersionFuture.join();
        VectorNodeWriter writer = new VectorNodeWriter(service, metadata);
        for (CollectedVector cv : collectedVectors) {
            Versionstamp addVs = Versionstamp.complete(trVersion, cv.userVersion());
            try {
                writer.write(cv, addVs);
            } catch (Exception e) {
                recordFailedAdd(addVs, cv);
                LOGGER.warn("Failed to add vector node to on-heap graph, objectId={}, vectorIndexId={}, versionstamp={}, recorded a retry entry: {}",
                        cv.objectId(), cv.vectorIndexId(), addVs, e.toString());
                LOGGER.debug("Stack trace for failed vector node add, objectId={}", cv.objectId(), e);
            }
        }
    }
}
