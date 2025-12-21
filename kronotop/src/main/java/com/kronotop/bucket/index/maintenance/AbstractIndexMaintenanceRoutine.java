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
import com.kronotop.KronotopException;

import java.util.concurrent.ExecutionException;

public abstract class AbstractIndexMaintenanceRoutine implements IndexMaintenanceRoutine {
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

    @Override
    public void stop() {
        stopped = true;
    }

    @Override
    public IndexMaintenanceRoutineMetrics getMetrics() {
        return metrics;
    }

    protected void commit(Transaction tr) {
        checkForShutdown();
        try {
            tr.commit().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IndexMaintenanceRoutineShutdownException(e);
        } catch (ExecutionException e) {
            throw new KronotopException(e.getCause());
        }
    }

    protected void checkForShutdown() {
        if (stopped || Thread.currentThread().isInterrupted()) {
            throw new IndexMaintenanceRoutineShutdownException();
        }
    }
}
