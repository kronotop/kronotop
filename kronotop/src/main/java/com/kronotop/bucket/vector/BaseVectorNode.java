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
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.VectorIndex;

class BaseVectorNode {
    final BucketService service;
    final BucketMetadata metadata;

    BaseVectorNode(BucketService service, BucketMetadata metadata) {
        this.service = service;
        this.metadata = metadata;
    }

    VectorGraphIndexGroup awaitReadyGroup(long vectorIndexId) {
        VectorIndex vectorIndex = metadata.vectorIndexes().getIndexById(vectorIndexId, IndexSelectionPolicy.ALL);
        VectorGraphIndexGroup group = service.getVectorGraphRegistry().computeIfAbsent(
                metadata,
                vectorIndex,
                () -> service.bootstrapVectorGroup(metadata, vectorIndex)
        );

        group.awaitReady();
        return group;
    }

    void recordFailedAdd(Versionstamp addVs, CollectedVector cv) {
        VectorGraphIndexGroup group = awaitReadyGroup(cv.vectorIndexId());
        RetryEntry retryEntry = new RetryEntry(metadata.namespace(), metadata.name(), metadata.uuid(), addVs, cv);
        group.recordFailedAdd(cv.objectId(), retryEntry);
    }
}
