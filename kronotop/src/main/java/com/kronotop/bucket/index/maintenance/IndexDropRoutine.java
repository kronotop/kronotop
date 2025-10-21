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
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.internal.task.TaskStorage;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexDropRoutine extends AbstractIndexMaintenanceRoutine {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexDropRoutine.class);
    private final IndexDropTask task;

    public IndexDropRoutine(Context context,
                            DirectorySubspace subspace,
                            Versionstamp taskId,
                            IndexDropTask task) {
        super(context, subspace, taskId);
        this.task = task;
    }

    private void markIndexDropTaskFailed(Throwable th) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexDropTaskState.setError(tr, subspace, taskId, th.getMessage());
            IndexDropTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
            tr.commit().join();
        }
    }

    private void markIndexDropTaskCompleted(Transaction tr) {
        IndexDropTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.COMPLETED);
    }

    private void clearIndex(Transaction tr) {
        BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
        Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
        if (index == null) {
            // index is gone
            markIndexDropTaskCompleted(tr);
            return;
        }
        IndexUtil.clear(tr, metadata.subspace(), index.definition().name());
    }

    private void doStart() {
        if (stopped) {
            return;
        }

        IndexTaskStatus status = context.getFoundationDB().run(tr -> {
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            if (definition == null) {
                // task has dropped
                return IndexTaskStatus.COMPLETED;
            }
            IndexDropTaskState state = IndexDropTaskState.load(tr, subspace, taskId);
            if (state.status() == IndexTaskStatus.COMPLETED) {
                // Already completed
                markIndexDropTaskCompleted(tr);
                return IndexTaskStatus.COMPLETED;
            }
            IndexDropTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.RUNNING);
            return IndexTaskStatus.RUNNING;
        });

        if (status == IndexTaskStatus.COMPLETED) {
            return;
        }

        try {
            refreshBucketMetadata(task.getNamespace(), task.getBucket(), task.getIndexId());
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                clearIndex(tr);
                markIndexDropTaskCompleted(tr);
                tr.commit().join();
            }
            LOGGER.debug(
                    "Index={} on namespace={}, bucket={} has been dropped",
                    task.getIndexId(),
                    task.getNamespace(),
                    task.getBucket()
            );
        } catch (InterruptedException e) {
            // Do not mark the task as failed. Program has stopped and this task
            // can be retried.
            Thread.currentThread().interrupt();
            throw new IndexMaintenanceRoutineShutdownException();
        } catch (IndexMaintenanceRoutineException exp) {
            LOGGER.error("TaskId: {} has failed due to an error: '{}'",
                    VersionstampUtil.base32HexEncode(taskId),
                    exp.getMessage()
            );
            markIndexDropTaskFailed(exp);
        } finally {
            metrics.setLatestExecution(System.currentTimeMillis());
        }
    }

    @Override
    public void start() {
        LOGGER.debug(
                "Dropping index={} on namespace={}, bucket={}",
                task.getIndexId(),
                task.getNamespace(),
                task.getBucket()
        );
        stopped = false; // also means a restart
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(this::doStart);
    }
}
