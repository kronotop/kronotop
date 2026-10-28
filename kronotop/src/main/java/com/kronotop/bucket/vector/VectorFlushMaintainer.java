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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.VectorIndexMaintainer;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.transaction.TransactionUtil;
import io.github.resilience4j.retry.Retry;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class VectorFlushMaintainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(VectorFlushMaintainer.class);
    private static final long DRAIN_LOG_INTERVAL_SECONDS = 10;
    private final Context context;
    private final ExecutorService executor;
    private final long drainTimeoutSeconds;
    private final long idleMillis;
    private BucketService service;
    private volatile boolean drainMode;
    private volatile boolean started;

    public VectorFlushMaintainer(Context context) {
        this.context = context;
        this.drainTimeoutSeconds = context.getConfig().getLong("bucket.vector.flush_drain_timeout_seconds");
        this.idleMillis = context.getConfig().getLong("bucket.vector.flush_idle_millis");
        this.executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("kr.vector-flush-maintainer-%d").build()
        );
    }

    public void setDrainMode() {
        drainMode = true;
    }

    public void start() {
        if (started) {
            return;
        }
        service = context.getService(BucketService.NAME);
        executor.submit(new FlushMaintainer());
        started = true;
    }

    /**
     * Waits for the drain to finish. The maintainer thread exits on its own once the flush
     * queue is empty in drain mode. The wait is bounded by {@code bucket.vector.flush_drain_timeout_seconds}.
     */
    public void shutdown() throws InterruptedException {
        executor.shutdown();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(drainTimeoutSeconds);
        while (!executor.awaitTermination(DRAIN_LOG_INTERVAL_SECONDS, TimeUnit.SECONDS)) {
            if (System.nanoTime() >= deadline) {
                LOGGER.warn("Vector flush drain exceeded {} seconds, aborting.", drainTimeoutSeconds);
                executor.shutdownNow();
                if (!executor.awaitTermination(
                        ExecutorServiceUtil.DEFAULT_TIMEOUT,
                        ExecutorServiceUtil.DEFAULT_TIMEOUT_TIMEUNIT
                )) {
                    LOGGER.warn("Vector flush maintainer cannot be stopped gracefully.");
                }
                return;
            }
            LOGGER.info("Vector flush drain still in progress.");
        }
    }

    private class FlushMaintainer implements Runnable {

        private VectorGraphIndexGroup loadGroup(VectorFlushTask task) {
            VectorFlushKey key = task.key();
            return service.getVectorGraphRegistry().get(key.namespace(), key.bucket(), key.indexId());
        }

        private void truncateGenerationId(VectorGraphIndexGroup group, ObjectId generationId) {
            Retry retry = TransactionUtil.retry(10, Duration.ofMillis(100));
            retry.executeRunnable(() -> {
                try (Transaction tr = context.getTransactionManager().create()) {
                    VectorIndexMaintainer.truncateMutationLog(tr, group.getVectorIndex().subspace(), generationId.toByteArray());
                    tr.commit().join();
                }
            });
        }

        private void reschedule(VectorFlushTask task) {
            long newDue = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(idleMillis);
            service.getVectorFlushQueue().add(new VectorFlushTask(task.key(), newDue));
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                VectorFlushTask task = null;
                try {
                    task = service.getVectorFlushQueue().poll(1, TimeUnit.SECONDS);
                    if (task == null && drainMode) {
                        break;
                    }
                    if (task == null) {
                        continue;
                    }
                    VectorGraphIndexGroup group = loadGroup(task);
                    if (group == null) {
                        continue;
                    }
                    group.forEachOnHeap(task.key().generationId(), (onHeap) -> {
                        if (drainMode || System.currentTimeMillis() - onHeap.getLastMutation() >= idleMillis) {
                            group.flush(service.getBucketDataDir(), onHeap);
                        }
                    });
                    if (!group.hasNoOnHeapIndexes(task.key().generationId())) {
                        // Still have some data, reschedule it.
                        reschedule(task);
                    } else {
                        truncateGenerationId(group, task.key().generationId());
                    }
                } catch (InterruptedException exp) {
                    Thread.currentThread().interrupt();
                    throw new KronotopException(exp);
                } catch (Exception exp) {
                    LOGGER.error("Vector flush task failed", exp);
                    if (task != null) {
                        reschedule(task);
                    }
                }
            }
        }
    }
}
