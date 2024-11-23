/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.volume.replication;

import com.kronotop.Context;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.client.InternalClient;
import com.kronotop.cluster.client.StatefulInternalConnection;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class ReplicationClient {
    private final Context context;
    private final ReplicationConfig config;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicReference<StatefulInternalConnection<byte[], byte[]>> connection = new AtomicReference<>();
    private RedisClient client = null;

    ReplicationClient(Context context, ReplicationConfig config) {
        this.context = context;
        this.config = config;
    }

    /**
     * Establishes a connection to the primary member of a specified shard.
     * <p>
     * This method locks the object to ensure thread safety and retrieves the primary
     * member information for the shard specified in the configuration.
     * It then creates a new Redis client using the member's address
     * and updates the current client reference. If a previous Redis client exists,
     * it shuts it down before setting the new client.
     *
     * @throws IllegalArgumentException if the route for the specified shard kind
     *                                  and shard id is not found.
     */
    public void connect() {
        lock.lock();
        try {
            RoutingService routing = context.getService(RoutingService.NAME);
            Route route = routing.findRoute(config.shardKind(), config.shardId());
            if (route == null) {
                throw new IllegalArgumentException("Route not found: " + config.shardKind() + " " + config.shardId());
            }

            Member member = route.primary();
            RedisClient redisClient = RedisClient.create(String.format("redis://%s:%d", member.getInternalAddress().getHost(), member.getInternalAddress().getPort()));
            if (client != null) {
                client.shutdown();
            }
            client = redisClient;
            connection.set(InternalClient.connect(redisClient, ByteArrayCodec.INSTANCE));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves the stateful internal connection to Redis.
     * <p>
     * This method checks if the connection has already been established. If not, it throws an
     * IllegalStateException indicating that the Redis connection is not available.
     *
     * @return an instance of {@link StatefulInternalConnection<byte[], byte[]>} representing the stateful
     *         connection to the Redis database.
     * @throws IllegalStateException if the Redis connection has not been established yet.
     */
    public StatefulInternalConnection<byte[], byte[]> connection() {
        StatefulInternalConnection<byte[], byte[]> conn = connection.get();
        if (conn == null) {
            throw new IllegalStateException("Redis connection not established yet");
        }
        return conn;
    }

    /**
     * Shuts down the current Redis client and releases its connection.
     *
     * <p>This method ensures thread safety by locking the relevant resources before
     * discarding the Redis client. If a Redis client is currently active, it will be
     * shut down and the connection will be cleared. The method guarantees that the
     * Redis client will not be used after being set to null.
     *
     * <p>Note: It is important to call this method to properly release resources
     * associated with the Redis client to avoid potential memory leaks or resource
     * exhaustion.
     */
    public void shutdown() {
        lock.lock();
        try {
            // The client should be discarded after calling shutdown.
            if (client != null) {
                client.shutdown();
                connection.set(null);
                client = null;
            }
        } finally {
            lock.unlock();
        }
    }
}
