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

package com.kronotop.bucket;

import com.kronotop.Context;
import com.kronotop.internal.TransactionUtils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Utility for ensuring cluster-wide bucket metadata synchronization.
 *
 * <p>Provides a critical synchronization step that ensures all cluster nodes have observed
 * the latest bucket metadata version before operations that require consistent metadata views.
 * This prevents inconsistencies where some nodes might operate on stale metadata.
 *
 * <p><strong>Synchronization Strategy:</strong>
 * <ol>
 *   <li>Wait for all shards to observe target metadata version via {@link BucketMetadataVersionBarrier}</li>
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
 * <p><strong>Test Override:</strong> Config path {@code __test__.bucket_metadata_convergence.skip_wait_transaction_limit}
 * allows skipping the 10-second sleep for faster test execution.
 *
 * @see BucketMetadataVersionBarrier
 */
public class BucketMetadataConvergence {
    
    /**
     * Waits for cluster-wide metadata convergence and transaction expiration.
     *
     * <p><strong>Phase 1: Version Barrier (up to 60 seconds):</strong>
     * <ul>
     *   <li>Loads current bucket metadata and version</li>
     *   <li>Creates {@link BucketMetadataVersionBarrier} for the bucket</li>
     *   <li>Polls all shards until they observe the current version</li>
     * </ul>
     *
     * <p><strong>Phase 2: Transaction Expiration (10 seconds):</strong>
     * <ul>
     *   <li>Sleeps 10 seconds to ensure all pre-update transactions expire</li>
     *   <li>Skipped if {@code __test__.bucket_metadata_convergence.skip_wait_transaction_limit} is true</li>
     * </ul>
     *
     * @param context   application context with cluster config and FoundationDB access
     * @param namespace bucket namespace
     * @param bucket    bucket name
     * @throws BucketMetadataConvergenceException if barrier fails or other errors occur
     */
    public static void await(Context context, String namespace, String bucket) throws InterruptedException {
        try {
            BucketMetadata metadata = TransactionUtils.execute(context,
                    tr -> BucketMetadataUtil.openUncached(context, tr, namespace, bucket)
            );
            BucketMetadataVersionBarrier barrier = new BucketMetadataVersionBarrier(context, metadata);
            barrier.await(metadata.version(), 120, Duration.ofMillis(500));
        } catch (Exception exp) {
            throw new BucketMetadataConvergenceException(exp);
        }

        // Grace period
        String txLimitConfigPath = "__test__.bucket_metadata_convergence.skip_wait_transaction_limit";
        boolean skipWaitTxLimit = context.getConfig().hasPath(txLimitConfigPath) && context.getConfig().getBoolean(txLimitConfigPath);
        if (!skipWaitTxLimit) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
        }
    }
}
