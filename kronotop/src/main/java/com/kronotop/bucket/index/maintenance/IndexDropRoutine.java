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
import com.kronotop.bucket.RetryMethods;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.internal.task.TaskStorage;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexDropRoutine implements IndexMaintenanceRoutine {
    protected static final Logger LOGGER = LoggerFactory.getLogger(IndexDropRoutine.class);
    private final Context context;
    private final DirectorySubspace subspace;
    private final Versionstamp taskId;
    private final IndexDropTask task;
    private final IndexMaintenanceRoutineMetrics metrics;
    private volatile boolean stopped;

    public IndexDropRoutine(Context context,
                            DirectorySubspace subspace,
                            Versionstamp taskId,
                            IndexDropTask task) {
        this.context = context;
        this.subspace = subspace;
        this.taskId = taskId;
        this.task = task;
        this.metrics = new IndexMaintenanceRoutineMetrics();
    }

    private void refreshBucketMetadata() throws InterruptedException {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Open the BucketMetadata and refresh the caches
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
            Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
            if (index == null) {
                throw new IndexMaintenanceRoutineException("index with id '" + task.getIndexId() + "' could not be found");
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
            String txLimitConfigPath = "__test__.background_index_builder.skip_wait_transaction_limit";
            boolean skipWaitTxLimit = context.getConfig().hasPath(txLimitConfigPath) && context.getConfig().getBoolean(txLimitConfigPath);
            if (!skipWaitTxLimit) {
                // FoundationDB transactions cannot live beyond 5s.
                // Sleeping 6s ensures that any previously opened transactions are expired.
                Thread.sleep(6000);
            }
            // Now all transactions either committed or died.
        }
    }

    private void doStart() {
        if (stopped) {
            return;
        }
        try {
            refreshBucketMetadata();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
                if (definition == null) {
                    // task has dropped
                    return;
                }
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
                Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
                if (index == null) {
                    // index is gone
                    return;
                }
                IndexUtil.clear(tr, metadata.subspace(), index.definition().name());
                tr.commit().join();
            }
        } catch (InterruptedException e) {
            // Do not mark the task as failed. Program has stopped and this task
            // can be retried.
            Thread.currentThread().interrupt();
            throw new IndexMaintenanceRoutineShutdownException();
        }
    }

    @Override
    public void start() {
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(this::doStart);
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
