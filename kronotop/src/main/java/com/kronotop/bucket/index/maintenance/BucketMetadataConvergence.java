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
import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketMetadataVersionBarrier;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.internal.TransactionUtils;

import java.time.Duration;

/**
 * Utility for ensuring cluster-wide bucket metadata synchronization before index maintenance.
 *
 * <p>Provides a critical synchronization step that ensures all cluster nodes have observed
 * the latest bucket metadata version before index maintenance operations begin. This prevents
 * inconsistencies where some nodes might operate on stale metadata.
 *
 * <p><strong>Synchronization Strategy:</strong>
 * <ol>
 *   <li>Wait for all shards to observe target metadata version via {@link BucketMetadataVersionBarrier}</li>
 *   <li>Validate index exists after convergence</li>
 *   <li>Sleep 10 seconds to ensure all in-flight transactions expire</li>
 * </ol>
 *
 * <p><strong>Version Barrier:</strong> Uses {@link BucketMetadataVersionBarrier} to poll
 * all shards until they've witnessed the target version. Waits up to 60 seconds (120 attempts
 * × 500ms intervals) before failing.
 *
 * <p><strong>Transaction Expiration Window:</strong> After metadata convergence, sleeps 10
 * seconds to ensure any transactions opened before the metadata update have expired. FoundationDB
 * transactions have a 5-second default lifetime, so 10 seconds guarantees expiration.
 *
 * <p><strong>GC Pause Tolerance:</strong> If a stop-the-world GC pause occurs during the sleep,
 * the transaction will fail with "transaction too old" error. The task will be marked as FAILED,
 * requiring manual retry. This preserves correctness—never inconsistent state, only delayed tasks.
 *
 * <p><strong>Test Override:</strong> Config path {@code __test__.index_maintenance.skip_wait_transaction_limit}
 * allows skipping the 10-second sleep for faster test execution.
 *
 * @see BucketMetadataVersionBarrier
 * @see IndexBoundaryRoutine
 */
public class BucketMetadataConvergence {

    /**
     * Waits for cluster-wide metadata convergence and transaction expiration.
     *
     * <p>Ensures all cluster nodes have synchronized on the latest bucket metadata version
     * and that all transactions opened before the update have expired. Called by
     * {@link IndexBoundaryRoutine} before determining scan boundaries.
     *
     * <p><strong>Phase 1: Version Barrier (up to 60 seconds):</strong>
     * <ul>
     *   <li>Loads current bucket metadata and version</li>
     *   <li>Creates {@link BucketMetadataVersionBarrier} for the bucket</li>
     *   <li>Polls all shards until they observe the current version</li>
     *   <li>120 attempts × 500ms = 60 seconds maximum wait</li>
     *   <li>Throws {@link IndexMaintenanceRoutineException} on timeout or error</li>
     * </ul>
     *
     * <p><strong>Phase 2: Index Validation:</strong>
     * <ul>
     *   <li>Force-reloads metadata to refresh all caches</li>
     *   <li>Validates index exists with {@link IndexSelectionPolicy#ALL}</li>
     *   <li>Throws {@link IndexMaintenanceRoutineException} if index not found</li>
     * </ul>
     *
     * <p><strong>Phase 3: Transaction Expiration (10 seconds):</strong>
     * <ul>
     *   <li>Sleeps 10 seconds to ensure all pre-update transactions expire</li>
     *   <li>FoundationDB default transaction lifetime: 5 seconds</li>
     *   <li>10 seconds ensures 2× safety margin for expiration</li>
     *   <li>Skipped if {@code __test__.index_maintenance.skip_wait_transaction_limit} is true</li>
     * </ul>
     *
     * <p><strong>GC Pause Handling:</strong> If a GC pause occurs during sleep, the transaction
     * holding the stable read version will fail with "transaction too old" when it attempts
     * operations. This causes the index maintenance task to fail cleanly, preserving correctness
     * at the cost of requiring manual retry.
     *
     * @param context application context with cluster config and FoundationDB access
     * @param namespace bucket namespace
     * @param bucket bucket name
     * @param indexId index identifier for validation
     * @throws InterruptedException if thread interrupted during barrier wait or sleep
     * @throws IndexMaintenanceRoutineException if barrier fails, index not found, or other errors
     * @see BucketMetadataVersionBarrier#await(long, int, Duration)
     */
    public static void await(Context context, String namespace, String bucket, long indexId) throws InterruptedException {
        try {
            BucketMetadata metadata = TransactionUtils.execute(context,
                    tr -> BucketMetadataUtil.forceOpen(context, tr, namespace, bucket)
            );
            BucketMetadataVersionBarrier barrier = new BucketMetadataVersionBarrier(context, metadata);
            barrier.await(metadata.version(), 120, Duration.ofMillis(500));
        } catch (Exception e) {
            throw new IndexMaintenanceRoutineException(e);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.forceOpen(context, tr, namespace, bucket);
            Index index = metadata.indexes().getIndexById(indexId, IndexSelectionPolicy.ALL);
            if (index == null) {
                throw new IndexMaintenanceRoutineException("Index with id '" + indexId + "' could not be found");
            }

            /*
             * GC pause tolerance: If stop-the-world pause occurs during sleep, the transaction
             * fails with "too old" error. Task marked as FAILED, requires manual retry.
             * Preserves correctness—never inconsistent state, only delayed tasks.
             */
            String txLimitConfigPath = "__test__.index_maintenance.skip_wait_transaction_limit";
            boolean skipWaitTxLimit = context.getConfig().hasPath(txLimitConfigPath) && context.getConfig().getBoolean(txLimitConfigPath);
            if (!skipWaitTxLimit) {
                Thread.sleep(10 * 1000);
            }
        }
    }
}
