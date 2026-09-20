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

package com.kronotop.bucket;

import com.kronotop.Context;
import com.kronotop.internal.ExecutorServiceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Retries the failed vector node adds and deletes on a fixed period. A single virtual thread walks all
 * vector graph index groups and applies the recorded failed operations to the graph indexes again.
 */
public class FailedVectorNodeOpRetrier {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailedVectorNodeOpRetrier.class);
    private final Context context;
    private final ReentrantLock lock = new ReentrantLock();
    private final Duration period;
    private final AtomicBoolean started = new AtomicBoolean();
    private volatile Thread worker;
    private volatile  boolean shutdown;

    private void retryVectorNodeOps() {
        BucketService service = context.getService(BucketService.NAME);
        service.getVectorGraphRegistry().forEachGroup((group) -> {
            int number = group.retryFailedOps();
            if (number > 0) {
                LOGGER.debug("Retried {} failed vector node ops, bucketId={}", number, group.getBucketId());
            }
        });
    }

    public FailedVectorNodeOpRetrier(Context context, Duration period) {
        this.context = context;
        this.period = period;
    }

    /**
     * Starts the worker thread. Can be called only once.
     *
     * @throws IllegalStateException if the retrier is already started
     */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("FailedVectorNodeOpRetrier is already started");
        }
        this.worker = Thread.ofVirtual().name("failed-vector-node-op-retrier").start(() -> periodicRetrier(period));
    }

    private void periodicRetrier(Duration period) {
        long next = System.nanoTime() + period.toNanos();
        while (!shutdown) {
            try {
                long wait = next - System.nanoTime();
                if (wait > 0) Thread.sleep(Duration.ofNanos(wait));
                // fixed-rate, no drift
                next += period.toNanos();

                // do not pile up ticks after a slow pass
                if (next < System.nanoTime()) next = System.nanoTime();

                long start = System.nanoTime();
                // exit while waiting for the lock on shutdown
                lock.lockInterruptibly();
                try {
                    retryVectorNodeOps();
                } finally {
                    lock.unlock();
                }
                LOGGER.debug("Retry pass took {} ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            } catch (InterruptedException exp) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception exp) {
                // Keep the worker alive, the next tick will retry.
                LOGGER.error("Failed to retry vector node adds, will try again in the next period", exp);
            }
        }
    }

    /**
     * Stops the worker thread and waits for it to exit, up to the default timeout.
     * Does nothing if the retrier was never started.
     */
    public void shutdown() throws InterruptedException {
        shutdown = true;
        if (worker != null) {
            worker.interrupt();
            worker.join(Duration.ofSeconds(ExecutorServiceUtil.DEFAULT_TIMEOUT));
        }
    }
}
