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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import io.github.resilience4j.retry.Retry;

import java.util.HashMap;
import java.util.Map;

public class IndexMaintenanceTaskSweeper {
    private final Context context;
    private final int numShards;
    private final Map<Integer, DirectorySubspace> subspaces = new HashMap<>();

    public IndexMaintenanceTaskSweeper(Context context) {
        this.context = context;
        BucketService service = context.getService(BucketService.NAME);
        this.numShards = service.getNumberOfShards();
    }

    public void sweep(DirectorySubspace taskSubspace, Versionstamp taskId) {
        int numCompleted = 0;
        for (int shardId = 0; shardId < numShards; shardId++) {
            DirectorySubspace otherTaskSubspace = subspaces.computeIfAbsent(shardId,
                    (id) -> IndexTaskUtil.createOrOpenTasksSubspace(context, id));
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuilderTaskState state = IndexBuilderTaskState.load(tr, otherTaskSubspace, taskId);
                if (state.status() != IndexTaskStatus.COMPLETED) {
                    break;
                }
                numCompleted++;
            }
        }
        if (numCompleted != numShards) {
            return;
        }
        // READY
        Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
        retry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                byte[] taskDef = TaskStorage.getDefinition(tr, taskSubspace, taskId);
                if (taskDef == null) {
                    return;
                }
                IndexBuilderTask task = JSONUtil.readValue(taskDef, IndexBuilderTask.class);
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
                Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.READWRITE);
                if (index == null) {
                    dropIndexBuildingTaskPieces(tr, taskId);
                    tr.commit().join();
                } else {
                    IndexStatus status = index.definition().status();
                    if (status == IndexStatus.BUILDING) {
                        IndexDefinition definition = index.definition().updateStatus(IndexStatus.READY);
                        IndexUtil.saveIndexDefinition(tr, metadata, definition);
                        dropIndexBuildingTaskPieces(tr, taskId);
                        tr.commit().join();
                    }
                }
            }
        });
    }

    private void dropIndexBuildingTaskPieces(Transaction tr, Versionstamp taskId) {
        for (DirectorySubspace subspace : subspaces.values()) {
            TaskStorage.drop(tr, subspace, taskId);
        }
    }
}
