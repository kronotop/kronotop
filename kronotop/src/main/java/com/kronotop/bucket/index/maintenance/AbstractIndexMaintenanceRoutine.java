/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.index.maintenance;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;

public abstract class AbstractIndexMaintenanceRoutine implements IndexMaintenanceRoutine {
    protected final Context context;
    protected final Versionstamp taskId;
    protected final DirectorySubspace subspace;
    protected final IndexMaintenanceRoutineMetrics metrics;

    protected AbstractIndexMaintenanceRoutine(Context context,
                                              DirectorySubspace subspace,
                                              Versionstamp taskId) {
        this.context = context;
        this.taskId = taskId;
        this.subspace = subspace;
        this.metrics = new IndexMaintenanceRoutineMetrics();
    }

    protected void refreshBucketMetadata(String namespace, String bucket, long indexId) throws InterruptedException {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Open the BucketMetadata and refresh the caches
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, namespace, bucket);
            Index index = metadata.indexes().getIndexById(indexId, IndexSelectionPolicy.ALL);
            if (index == null) {
                throw new IndexMaintenanceRoutineException("index with id '" + indexId + "' could not be found");
            }

            /*
             * A potential stop-the-world pause (e.g., JVM GC) during the sleep interval
             * does not break the logic here. Once the transaction is created, it already
             * holds a stable read version from FoundationDB. If the pause extends beyond
             * the transaction lifetime, this transaction will simply fail with "too old"
             * and the task will be marked as failed. In that case, a manual or KCP trigger
             * is required to retry. This design ensures correctness is preserved even under
             * GC pauses; the worst case is a delayed or failed task, never inconsistent state.
             */
            String txLimitConfigPath = "__test__.index_maintenance.skip_wait_transaction_limit";
            boolean skipWaitTxLimit = context.getConfig().hasPath(txLimitConfigPath) && context.getConfig().getBoolean(txLimitConfigPath);
            if (!skipWaitTxLimit) {
                // FoundationDB transactions cannot live beyond 5s.
                // Sleeping 6s ensures that any previously opened transactions are expired.
                Thread.sleep(6000);
            }
            // Now all transactions either committed or died.
        }
    }

    @Override
    public IndexMaintenanceRoutineMetrics getMetrics() {
        return metrics;
    }
}
