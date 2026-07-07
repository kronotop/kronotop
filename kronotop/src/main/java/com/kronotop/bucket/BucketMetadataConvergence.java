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

package com.kronotop.bucket;

import com.kronotop.Context;
import com.kronotop.namespace.NamespaceBeingRemovedException;
import com.kronotop.transaction.TransactionUtil;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Blocks until every shard in the cluster has observed the latest bucket metadata version, then
 * waits out any transactions that started before the update.
 *
 * <p>This guards operations that must not run against stale metadata. It first drives a
 * {@link BucketMetadataVersionBarrier}, polling all shards for up to 60 seconds (120 attempts at
 * 500ms) until they report the target version. It then sleeps 10 seconds so that transactions
 * opened before the update, which have a 5-second FoundationDB lifetime, are guaranteed to have
 * expired.
 *
 * <p>The 10-second grace period can be skipped in tests by setting
 * {@code __test__.bucket_metadata_convergence.skip_wait_transaction_limit}.
 *
 * @see BucketMetadataVersionBarrier
 */
public class BucketMetadataConvergence {

    /**
     * Waits for cluster-wide metadata convergence, then for pre-update transactions to expire.
     *
     * @param context   application context with cluster config and FoundationDB access
     * @param namespace bucket namespace
     * @param bucket    bucket name
     * @throws BucketMetadataConvergenceException if the barrier fails or another error occurs
     */
    public static void await(Context context, String namespace, String bucket) throws InterruptedException {
        try {
            BucketMetadata metadata = TransactionUtil.execute(context,
                    tr -> BucketMetadataUtil.reload(context, tr, namespace, bucket)
            );
            BucketMetadataVersionBarrier barrier = new BucketMetadataVersionBarrier(context, metadata);
            barrier.await(metadata.version(), 120, Duration.ofMillis(500));
        } catch (NoSuchBucketException | BucketBeingRemovedException | NamespaceBeingRemovedException exp) {
            throw exp;
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
