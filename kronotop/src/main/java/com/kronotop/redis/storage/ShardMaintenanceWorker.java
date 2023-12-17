/*
 * Copyright (c) 2023 Kronotop
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

import com.kronotop.core.Context;
import com.kronotop.redis.storage.persistence.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.Semaphore;

/**
 * The ShardMaintenanceWorker class represents a task that performs shard maintenance in a multi-worker environment.
 * It runs periodically to flush shard indexes and persist data from the persistence queue for a specific worker.
 * It implements the Runnable interface to be executed by a thread pool.
 */
public class ShardMaintenanceWorker implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShardMaintenanceWorker.class);
    private final Context context;
    private final int workerId;
    private final int numWorkers;
    private final HashMap<Integer, Persistence> cache = new HashMap<>();
    private final Semaphore semaphore = new Semaphore(1);

    public ShardMaintenanceWorker(Context context, int workerId) {
        this.workerId = workerId;
        this.context = context;
        this.numWorkers = context.getConfig().getInt("persistence.num_workers");
    }

    public void pause() {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void unpause() {
        semaphore.release();
    }

    @Override
    public void run() {
        try {
            semaphore.acquire();
            try {
                context.getLogicalDatabase().getShards().forEach((shardId, shard) -> {
                    if (shardId % numWorkers != workerId) return;

                    shard.getIndex().flush();
                    if (shard.getPersistenceQueue().size() > 0) {
                        Persistence persistence = cache.compute(shardId,
                                (k, value) ->
                                        Objects.requireNonNullElseGet(value,
                                                () -> new Persistence(context, shard)));
                        persistence.run();
                    }
                });
            } catch (Exception e) {
                LOGGER.error("Error while running shard maintenance worker {}", e.getMessage());
                throw e;
            }
        } catch (InterruptedException e) {
            LOGGER.debug("Shard maintenance worker is interrupted while waiting for a permit", e);
        } finally {
            semaphore.release();
        }
    }
}
