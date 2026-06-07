/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.bucket.index;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.bucket.index.statistics.IndexAnalyzeTask;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Shared utility for scheduling index analysis tasks, used by both
 * {@link SingleFieldIndexUtil} and {@link CompoundIndexUtil}.
 */
public class IndexAnalyzeUtil {

    /**
     * Schedules an asynchronous analysis task to collect cardinality and histogram
     * data for the given index.
     *
     * @param tx              the transactional context
     * @param metadata        the bucket metadata containing the index
     * @param indexSubspace   the directory subspace of the index
     * @param indexDefinition the index definition (single-field or compound)
     * @throws KronotopException if the index is not in READY state
     */
    public static void scheduleAnalyzeTask(TransactionalContext tx, BucketMetadata metadata,
                                           DirectorySubspace indexSubspace, IndexDefinition indexDefinition) {
        if (indexDefinition.status() != IndexStatus.READY) {
            throw new KronotopException(
                    String.format("Cannot analyze index: index is not in READY state (current=%s)", indexDefinition.status())
            );
        }

        int shardId = metadata.shards().get(ThreadLocalRandom.current().nextInt(metadata.shards().size()));

        IndexAnalyzeTask task = new IndexAnalyzeTask(metadata.namespace(), metadata.name(), indexDefinition.id(), shardId);
        byte[] encodedTask = JSONUtil.writeValueAsBytes(task);

        int userVersion = tx.getAndIncreaseUserVersion();
        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);
        TaskStorage.create(tx.tr(), userVersion, taskSubspace, encodedTask);
        IndexTaskUtil.setBackPointer(tx.tr(), indexSubspace, userVersion, shardId);

        TaskStorage.triggerWatchers(tx.tr(), taskSubspace);
    }
}
