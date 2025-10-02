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
import com.kronotop.bucket.index.BackgroundIndexBuilderRoutine;
import com.kronotop.bucket.index.IndexBuilderTask;
import com.kronotop.bucket.index.IndexBuilderTaskState;
import com.kronotop.bucket.index.IndexMaintenanceRoutine;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class IndexMaintenanceWorker implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexMaintenanceWorker.class);
    private final Context context;
    private final IndexMaintenanceRoutine routine;
    private final DirectorySubspace subspace;
    private final Versionstamp taskId;
    private final Consumer<Versionstamp> completionHook;
    private final Metrics metrics = new Metrics();

    public IndexMaintenanceWorker(Context context, DirectorySubspace subspace, int shardId, Versionstamp taskId, Consumer<Versionstamp> completionHook) {
        this.context = context;
        this.subspace = subspace;
        this.taskId = taskId;
        this.completionHook = completionHook;
        byte[] raw = context.getFoundationDB().run(tr -> TaskStorage.getDefinition(tr, subspace, taskId));
        IndexBuilderTask task = JSONUtil.readValue(raw, IndexBuilderTask.class);
        this.routine = new BackgroundIndexBuilderRoutine(context, subspace, shardId, taskId, task);
    }

    @Override
    public void run() {
        RetryMethods.retry(RetryMethods.INDEX_MAINTENANCE_ROUTINE).executeRunnable(() -> {
            try {
                routine.start();
                IndexBuilderTaskState state = context.getFoundationDB().run(tr -> IndexBuilderTaskState.load(tr, subspace, taskId));
                if (IndexBuilderTaskState.isTerminal(state.status())) {
                    // Run a callback to remove this task from the watchdog thread.
                    completionHook.accept(taskId);
                    shutdown();
                }
            } catch (Exception e) {
                LOGGER.error("Failed to run the maintenance routine", e);
                throw e;
            } finally {
                getMetrics().setLatestExecution(System.currentTimeMillis());
            }
        });
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public void shutdown() {
        routine.stop();
    }

    public static class Metrics {
        private final long initiatedAt;
        private volatile long latestExecution;

        public Metrics() {
            this.initiatedAt = System.currentTimeMillis();
        }

        public long getInitiatedAt() {
            return initiatedAt;
        }

        public long getLatestExecution() {
            return latestExecution;
        }

        public void setLatestExecution(long latestExecution) {
            this.latestExecution = latestExecution;
        }
    }
}
