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

import com.kronotop.CommitHook;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Post-commit hook that removes the deleted vectors of one transaction from the graph
 * indexes. Waits for the transaction version, then hands the batch to {@link VectorNodeRemover}.
 */
public final class VectorNodeDeleteHook extends BaseVectorNode implements CommitHook {
    private final long vectorIndexId;
    private final List<DeletedVector> deletedVectors;
    private final CompletableFuture<byte[]> trVersionFuture;

    public VectorNodeDeleteHook(
            BucketService service,
            BucketMetadata metadata,
            long vectorIndexId,
            List<DeletedVector> deletedVectors,
            CompletableFuture<byte[]> trVersionFuture
    ) {
        super(service, metadata);
        this.vectorIndexId = vectorIndexId;
        this.deletedVectors = deletedVectors;
        this.trVersionFuture = trVersionFuture;
    }

    @Override
    public void run() {
        byte[] trVersion = trVersionFuture.join();
        VectorNodeRemover remover = new VectorNodeRemover(service, metadata, vectorIndexId);
        for (DeletedVector dv: deletedVectors) {
            remover.remove(dv, trVersion);
        }
        remover.flush();
    }
}
