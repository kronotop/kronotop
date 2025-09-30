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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.index.BackgroundIndexBuilder;
import com.kronotop.bucket.index.IndexBuildTask;
import com.kronotop.bucket.index.IndexBuildTaskState;
import com.kronotop.bucket.index.IndexTaskStatus;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;

public class IndexMaintenanceWorker implements Runnable {
    private final Context context;
    private final BackgroundIndexBuilder builder;
    private final DirectorySubspace subspace;
    private final Versionstamp taskId;
    private final IndexBuildTask task;
    private volatile boolean shutdown;

    public IndexMaintenanceWorker(Context context, DirectorySubspace subspace, int shardId, Versionstamp taskId) {
        this.context = context;
        this.subspace = subspace;
        this.taskId = taskId;
        byte[] raw = context.getFoundationDB().run(tr -> TaskStorage.getDefinition(tr, subspace, taskId));
        this.task = JSONUtil.readValue(raw, IndexBuildTask.class);
        this.builder = new BackgroundIndexBuilder(context, subspace, shardId, taskId, task);
    }


    @Override
    public void run() {
        while(!shutdown) {
            try {
                builder.run();
                IndexBuildTaskState state = context.getFoundationDB().run(tr -> IndexBuildTaskState.load(tr, subspace, taskId));
                if (state.status().equals(IndexTaskStatus.COMPLETED)) {

                }
            } catch (Exception e) {
                // TODO: LOG THIS
            }
        }
    }
}
