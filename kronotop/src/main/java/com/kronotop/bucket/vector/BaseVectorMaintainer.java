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

import com.apple.foundationdb.FDBException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketService;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.transaction.TransactionUtil;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Single-threaded queue consumer shared by the vector maintainers. Subclasses supply the queue and the work per task.
 *
 * @param <T> the task type
 */
public abstract class BaseVectorMaintainer<T> {
    private static final long DRAIN_LOG_INTERVAL_SECONDS = 10;
    protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
    protected final Context context;
    protected BucketService service;
    protected volatile boolean drainMode;
    private final ExecutorService executor;
    private final long drainTimeoutSeconds;
    private final String label;
    private volatile boolean started;

    protected BaseVectorMaintainer(
            Context context,
            String label,
            String threadNameFormat,
            long drainTimeoutSeconds
    ) {
        this.context = context;
        this.label = label;
        this.drainTimeoutSeconds = drainTimeoutSeconds;
        this.executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build()
        );
    }

    static Retry retry() {
        return TransactionUtil.retry(10, Duration.ofMillis(100));
    }

    protected abstract BlockingQueue<T> queue();

    protected abstract void process(T task) throws Exception;

    protected abstract void onFailure(T task, Exception exp);

    public void setDrainMode() {
        drainMode = true;
    }

    public void start() {
        if (started) {
            return;
        }
        service = context.getService(BucketService.NAME);
        executor.submit(this::runLoop);
        started = true;
    }

    /**
     * Waits for the drain to finish. The maintainer thread exits on its own once the queue
     * is empty in drain mode. The wait is bounded by the drain timeout given by the subclass.
     */
    public void shutdown() throws InterruptedException {
        executor.shutdown();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(drainTimeoutSeconds);
        while (!executor.awaitTermination(DRAIN_LOG_INTERVAL_SECONDS, TimeUnit.SECONDS)) {
            if (System.nanoTime() >= deadline) {
                LOGGER.warn("Vector {} drain exceeded {} seconds, aborting.", label, drainTimeoutSeconds);
                executor.shutdownNow();
                if (!executor.awaitTermination(
                        ExecutorServiceUtil.DEFAULT_TIMEOUT,
                        ExecutorServiceUtil.DEFAULT_TIMEOUT_TIMEUNIT
                )) {
                    LOGGER.warn("Vector {} maintainer cannot be stopped gracefully.", label);
                }
                return;
            }
            LOGGER.info("Vector {} drain still in progress.", label);
        }
    }

    private void runLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            T task = null;
            try {
                task = queue().poll(1, TimeUnit.SECONDS);
                if (task == null && drainMode) {
                    break;
                }
                if (task == null) {
                    continue;
                }
                process(task);
            } catch (InterruptedException exp) {
                Thread.currentThread().interrupt();
                throw new KronotopException(exp);
            } catch (Exception exp) {
                if (exp instanceof IOException || exp instanceof UncheckedIOException || exp instanceof FDBException) {
                    onFailure(task, exp);
                    continue;
                }
                LOGGER.error("Vector maintainer task failed with an unexpected error, task dropped: {}", task, exp);
            }
        }
    }
}
