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

package com.kronotop.redis;

import com.kronotop.Context;
import com.kronotop.MemberAttributes;
import com.kronotop.instance.KronotopInstanceStatus;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.RedisValueContainer;
import com.kronotop.redis.storage.RedisValueKind;
import com.kronotop.redis.storage.syncer.jobs.DeleteByVersionstampJob;
import io.netty.util.Attribute;

import java.util.NoSuchElementException;
import java.util.concurrent.Phaser;
import java.util.concurrent.locks.ReadWriteLock;

/*
 * Redis algorithm:
 *
 * Periodically Redis tests a few keys at random among keys with an expire set. All the keys that are already expired are deleted from the keyspace.
 *
 * Specifically, this is what Redis does 10 times per second:
 *
 * Test 20 random keys from the set of keys with an associated expire.
 * Delete all the keys found expired.
 * If more than 25% of keys were expired, start again from step 1.
 * This is a trivial probabilistic algorithm, basically the assumption is that our sample is representative of the whole key space, and we continue to expire until the percentage of keys that are likely to be expired is under 25%
 */

/**
 * The EvictionWorker class is responsible for managing the eviction process of expired keys
 * in Redis shards within a Kronotop instance. The eviction process is executed using
 * virtual threads to handle the workload efficiently.
 * <p>
 * This class implements the Runnable interface and works in conjunction with
 * the Kronotop service context to identify and interact with the Redis shards.
 * It employs the {@link ShardEvictionWorker} inner class for shard-specific eviction logic.
 * Eviction is handled to ensure the removal of expired keys efficiently while respecting
 * operational constraints.
 */
public class EvictionWorker implements Runnable {
    private static final int NUMBER_OF_SAMPLES = 20;
    private static final int CONTINUATION_THRESHOLD = NUMBER_OF_SAMPLES / 4;
    private final Context context;
    private final RedisService service;
    private final Phaser phaser = new Phaser(1);

    public EvictionWorker(Context context) {
        this.context = context;
        this.service = context.getService(RedisService.NAME);
    }

    /**
     * Submits a shard eviction worker task for the given Redis shard to an executor service.
     * Registers the task with a synchronization mechanism to ensure proper task lifecycle management.
     *
     * @param shard the Redis shard for which the eviction worker is to be submitted.
     *              It represents a partition of the Redis storage and contains its data and index.
     */
    private void submitShardEvictionWorker(RedisShard shard) {
        ShardEvictionWorker worker = new ShardEvictionWorker(shard);
        phaser.register();
        context.getVirtualThreadPerTaskExecutor().submit(worker);
    }

    private boolean isInstanceRunning() {
        Attribute<KronotopInstanceStatus> attr = context.getMemberAttributes().attr(MemberAttributes.INSTANCE_STATUS);
        KronotopInstanceStatus status = attr.get();
        if (status == null) {
            return false;
        }
        return status.equals(KronotopInstanceStatus.RUNNING);
    }

    @Override
    public void run() {
        for (RedisShard shard : service.getServiceContext().shards().values()) {
            if (!shard.isOperable() || !isInstanceRunning()) {
                break;
            }
            submitShardEvictionWorker(shard);
        }
        phaser.arriveAndAwaitAdvance();
    }

    /**
     * The ShardEvictionWorker is responsible for performing periodic eviction of expired keys
     * from a specific Redis shard. It implements the {@link Runnable} interface, enabling it to
     * be executed by an ExecutorService in a multithreaded environment.
     * <p>
     * The eviction process involves checking the TTL (time to live) of string keys in the shard's storage,
     * removing expired keys, and performing associated cleanup operations such as removing keys
     * from the index and processing versionstamp-based deletion jobs.
     * <p>
     * This worker supports continued eviction if the number of evicted keys meets or exceeds a
     * specified threshold and the shard remains operable.
     * <p>
     * The worker operates within the following constraints:
     * - Limits eviction attempts to a fixed number of random samples per execution.
     * - Synchronizes access to shard data using ReadWriteLocks provided by the shard's striped lock mechanism.
     * - Re-submits itself for further eviction tasks when needed.
     * <p>
     * Exceptions encountered during its operation, such as {@link NoSuchElementException}, are handled silently.
     * <p>
     * Key Behaviors:
     * - Evicts expired string keys by removing them from shard storage and the index,
     * and enqueues versionstamp-based delete jobs where applicable.
     * - Re-submits itself for continued eviction when required.
     * - Deregisters itself from a synchronization mechanism upon completion.
     */
    class ShardEvictionWorker implements Runnable {
        private final RedisShard shard;
        private int numberOfEvicted;

        public ShardEvictionWorker(RedisShard shard) {
            this.shard = shard;
        }

        private void evictStringKeys(String key, RedisValueContainer container) {
            long current = service.getCurrentTimeInMilliseconds();
            if (container.string().ttl() != 0 && container.string().ttl() <= current) {
                RedisValueContainer previous = shard.storage().remove(key);
                shard.index().remove(key);
                if (previous.baseRedisValue().versionstamp() != null) {
                    shard.volumeSyncQueue().add(new DeleteByVersionstampJob(previous.baseRedisValue().versionstamp()));
                }
                numberOfEvicted++;
            }
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < NUMBER_OF_SAMPLES; i++) {
                    if (!shard.isOperable() || shard.index().size() == 0) {
                        break;
                    }
                    String key = shard.index().random();
                    ReadWriteLock lock = shard.striped().get(key);
                    lock.readLock().lock();
                    try {
                        RedisValueContainer container = shard.storage().get(key);
                        if (container == null) {
                            // deleted
                            continue;
                        }
                        if (container.kind().equals(RedisValueKind.STRING)) {
                            evictStringKeys(key, container);
                        }
                    } finally {
                        lock.readLock().unlock();
                    }
                }
                if (numberOfEvicted >= CONTINUATION_THRESHOLD && isInstanceRunning() && shard.isOperable()) {
                    submitShardEvictionWorker(shard);
                }
            } catch (NoSuchElementException e) {
                // Ignore
            } finally {
                phaser.arriveAndDeregister();
            }
        }
    }
}
