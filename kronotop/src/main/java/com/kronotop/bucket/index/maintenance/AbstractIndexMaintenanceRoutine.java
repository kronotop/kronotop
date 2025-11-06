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
import com.kronotop.bucket.BucketMetadataVersionBarrier;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.internal.TransactionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public abstract class AbstractIndexMaintenanceRoutine implements IndexMaintenanceRoutine {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIndexMaintenanceRoutine.class);
    protected final Context context;
    protected final Versionstamp taskId;
    protected final DirectorySubspace subspace;
    protected final IndexMaintenanceRoutineMetrics metrics;
    protected volatile boolean stopped;

    protected AbstractIndexMaintenanceRoutine(Context context,
                                              DirectorySubspace subspace,
                                              Versionstamp taskId) {
        this.context = context;
        this.taskId = taskId;
        this.subspace = subspace;
        this.metrics = new IndexMaintenanceRoutineMetrics();
    }

    /**
     * Waits for bucket metadata propagation and establishes a stable transaction baseline for index maintenance.
     *
     * <p>Blocks until bucket metadata propagates to all cluster members (up to 60 seconds), then opens a transaction
     * with a stable read version. After validating the target index exists, sleeps for 10 seconds to guarantee all
     * pre-existing transactions have expired (FoundationDB 5s limit). This ensures subsequent index operations
     * see a consistent snapshot without conflicts from concurrent modifications.
     *
     * @param namespace the namespace containing the bucket
     * @param bucket the bucket name
     * @param indexId the index identifier to validate
     * @throws InterruptedException if interrupted during metadata propagation wait or transaction wait
     * @throws IndexMaintenanceRoutineException if metadata propagation fails or index not found
     */
    protected void refreshBucketMetadata(String namespace, String bucket, long indexId) throws InterruptedException {
        try {
            BucketMetadata metadata = TransactionUtils.execute(context, tr -> BucketMetadataUtil.forceOpen(context, tr, namespace, bucket));
            BucketMetadataVersionBarrier barrier = new BucketMetadataVersionBarrier(context, metadata);
            // Wait up to 60 seconds (120 attempts × 500ms intervals). Background task - no rush needed.
            barrier.await(metadata.version(), 120, Duration.ofMillis(500));
        } catch (Exception e) {
            LOGGER.debug("Bucket metadata synchronization failed for namespace={}, bucket={}, indexId={}", namespace, bucket, indexId, e);
            throw new IndexMaintenanceRoutineException(e);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Open the BucketMetadata and refresh the caches, forceOpen always loads all bucket and
            // index metadata from FDB using the given transaction.
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, namespace, bucket);
            Index index = metadata.indexes().getIndexById(indexId, IndexSelectionPolicy.ALL);
            if (index == null) {
                throw new IndexMaintenanceRoutineException("Index with id '" + indexId + "' could not be found");
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
                // Sleeping 10s ensures that any previously opened transactions are expired.
                Thread.sleep(10 * 1000);
            }
            // Now all transactions either committed or died.
        }
    }

    @Override
    public void stop() {
        stopped = true;
    }

    @Override
    public IndexMaintenanceRoutineMetrics getMetrics() {
        return metrics;
    }
}
