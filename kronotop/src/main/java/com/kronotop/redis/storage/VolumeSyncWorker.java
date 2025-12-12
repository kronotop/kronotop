/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.redis.storage;

import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.ServiceContext;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.storage.syncer.VolumeSyncer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.Semaphore;

/**
 * The VolumeSyncWorker class is responsible for synchronizing volume in Redis shards.
 * It implements the Runnable interface, allowing its execution to be managed in a separate thread.
 */
public class VolumeSyncWorker implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(VolumeSyncWorker.class);
    private final Context context;
    private final int workerId;
    private final int numWorkers;
    private final HashMap<Integer, VolumeSyncer> cache = new HashMap<>();
    private final Semaphore semaphore = new Semaphore(1);
    private final ServiceContext<RedisShard> redisContext;
    private volatile boolean shutdown = false;

    public VolumeSyncWorker(Context context, int workerId) {
        this.workerId = workerId;
        this.context = context;
        this.redisContext = context.getServiceContext(RedisService.NAME);
        this.numWorkers = context.getConfig().getInt("redis.volume_syncer.workers");
    }

    public void pause() {
        try {
            LOGGER.info("{}: {} has been paused", this.getClass().getSimpleName(), workerId);
            semaphore.acquire();
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException(exp);
        }
    }

    public void resume() {
        LOGGER.info("{}: {} has been resumed", this.getClass().getSimpleName(), workerId);
        semaphore.release();
    }

    /**
     * Executes the volume synchronization operation for the provided Redis shard.
     * <p>
     * If the volume sync queue for the shard is empty, the method returns immediately.
     * Otherwise, it fetches or creates a {@link VolumeSyncer} for the shard and runs it.
     * Any exceptions thrown during the execution of the {@link VolumeSyncer} are caught and logged.
     * </p>
     *
     * @param shard the Redis shard for which to execute the volume synchronization
     */
    private void runVolumeSyncer(RedisShard shard) {
        if (shard.volumeSyncQueue().isEmpty()) {
            return;
        }
        VolumeSyncer volumeSyncer = cache.compute(shard.id(),
                (k, value) ->
                        Objects.requireNonNullElseGet(value,
                                () -> new VolumeSyncer(context, shard)));
        try {
            volumeSyncer.run();
        } catch (Exception e) {
            LOGGER.error("Error while running volume syncer for Redis shard: {}", shard.id(), e);
        }
    }

    @Override
    public void run() {
        try {
            semaphore.acquire();
            if (shutdown) {
                return;
            }
            try {
                redisContext.shards().forEach((shardId, shard) -> {
                    if (shardId % numWorkers != workerId) return;

                    shard.index().flush();
                    runVolumeSyncer(shard);
                });
            } catch (Exception e) {
                LOGGER.error("Error while running Redis shard maintenance worker", e);
                throw e;
            }
        } catch (InterruptedException exp) {
            LOGGER.debug("Redis shard maintenance worker: {} is interrupted while running", workerId, exp);
            Thread.currentThread().interrupt();
            throw new KronotopException(exp);
        } finally {
            semaphore.release();
        }
    }

    private void drainVolumeSyncQueue(RedisShard shard) {
        if (shard.volumeSyncQueue().isEmpty()) {
            return;
        }
        LOGGER.info("Draining volume sync queue on Redis shard: {}", shard.id());
        while (!shard.volumeSyncQueue().isEmpty()) {
            runVolumeSyncer(shard);
        }
    }

    /**
     * Drains the volume synchronization queues for the Redis shards assigned to this worker.
     * <p>
     * Each shard is processed only if its shard ID modulo the number of workers equals the worker ID.
     * This ensures that the work is distributed evenly among the workers.
     * </p>
     * <p>
     * The method acquires a semaphore before processing to ensure thread safety and releases it after processing.
     * </p>
     * <p>
     * If the thread is interrupted while waiting to acquire the semaphore or during processing,
     * the interruption is logged.
     * </p>
     */
    public void drainVolumeSyncQueues() {
        try {
            semaphore.acquire();
            redisContext.shards().forEach((shardId, shard) -> {
                if (shardId % numWorkers != workerId) return;
                drainVolumeSyncQueue(shard);
            });
        } catch (InterruptedException exp) {
            LOGGER.debug("Redis shard maintenance worker: {} is interrupted while draining the volume sync queues", workerId, exp);
            Thread.currentThread().interrupt();
            throw new KronotopException(exp);
        } finally {
            semaphore.release();
        }
    }

    public void shutdown() {
        try {
            semaphore.acquire(); // wait until all syncers are done
            shutdown = true;
        } catch (InterruptedException exp) {
            LOGGER.debug("Failed to shutdown Redis shard maintenance worker", exp);
            Thread.currentThread().interrupt();
            throw new KronotopException(exp);
        } finally {
            semaphore.release();
        }
    }
}
