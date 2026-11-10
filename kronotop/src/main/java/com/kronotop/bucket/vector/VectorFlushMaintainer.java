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

package com.kronotop.bucket.vector;

import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.bucket.index.VectorIndexMaintainer;
import org.bson.types.ObjectId;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class VectorFlushMaintainer extends BaseVectorMaintainer<VectorFlushTask> {
    private final long idleMillis;

    public VectorFlushMaintainer(Context context) {
        super(context, "flush", "kr.vector-flush-maintainer-%d",
                context.getConfig().getLong("bucket.vector.flush_drain_timeout_seconds"));
        this.idleMillis = context.getConfig().getLong("bucket.vector.flush_idle_millis");
    }

    @Override
    protected BlockingQueue<VectorFlushTask> queue() {
        return service.getVectorFlushQueue();
    }

    private VectorGraphIndexGroup loadGroup(VectorFlushTask task) {
        VectorFlushKey key = task.key();
        return service.getVectorGraphRegistry().get(key.namespace(), key.bucket(), key.indexId());
    }

    private void truncateGenerationId(VectorGraphIndexGroup group, ObjectId generationId) {
        retry().executeRunnable(() -> {
            try (Transaction tr = context.getTransactionManager().create()) {
                VectorIndexMaintainer.truncateMutationLog(tr, group.getVectorIndex().subspace(), generationId.toByteArray());
                tr.commit().join();
            }
        });
    }

    private void reschedule(VectorFlushTask task) {
        // In drain mode a task that still has data is dropped, so the drain can finish. The next
        // start rebuilds it from the mutation log.
        if (drainMode) {
            return;
        }
        long newDue = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(idleMillis);
        service.getVectorFlushQueue().add(new VectorFlushTask(task.key(), newDue));
    }

    @Override
    protected void process(VectorFlushTask task) {
        VectorGraphIndexGroup group = loadGroup(task);
        if (group == null) {
            return;
        }
        final VectorFlushKey key = task.key();
        group.forEachOnHeap(key.generationId(), (onHeap) -> {
            if (drainMode || System.currentTimeMillis() - onHeap.getLastMutation() >= idleMillis) {
                boolean flushed = group.flush(service.getDataDir(), onHeap);
                if (flushed) {
                    VectorSyncTask syncTask = new VectorSyncTask(
                            key.namespace(),
                            key.bucket(),
                            key.indexId(),
                            onHeap.getId()
                    );
                    service.getVectorSyncQueue().add(syncTask);
                }
            }
        });
        if (!group.hasNoOnHeapIndexes(key.generationId())) {
            // Still have some data, reschedule it.
            reschedule(task);
        } else {
            truncateGenerationId(group, key.generationId());
        }
    }

    @Override
    protected void onFailure(VectorFlushTask task, Exception exp) {
        LOGGER.error("Vector flush task failed", exp);
        service.getMetrics().incrementVectorFlushFailures();
        if (task != null) {
            reschedule(task);
        }
    }
}
