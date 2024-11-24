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
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.codec.ByteArrayCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * ReplicationClient is responsible for managing the connection to a primary Kronotop instance,
 * handling the connection lifecycle including establishing and shutting down the connection.
 * It ensures thread-safety using a read-write lock.
 */
public class ReplicationClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationClient.class);
    private final Context context;
    private final ReplicationConfig config;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private StatefulInternalConnection<byte[], byte[]> connection;
    private RedisClient client;

    ReplicationClient(Context context, ReplicationConfig config) {
        this.context = context;
        this.config = config;
    }

    /**
     * Establishes a connection to the primary Redis instance based on the provided
     * shard configuration and routing information. This method is thread-safe,
     * utilizing a write lock to ensure exclusive access during the connection process.
     *
     * @throws IllegalArgumentException if the route for the specified shard kind
     *         and shard ID is not found
     */
    public void connect() {
        lock.writeLock().lock();
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
            connection = InternalClient.connect(redisClient, ByteArrayCodec.INSTANCE);
        } catch (RedisConnectionException e) {
            LOGGER.error("Failed to connect", e);
            // Shutdown the client if it's active, because we don't want to use a wrong client to replicate data.
            shutdown_internal();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Retrieves the current stateful connection to the Redis database.
     * This method ensures thread safety by acquiring a read lock before accessing the connection.
     *
     * @return the current stateful connection to the Redis database
     * @throws IllegalStateException if the Redis connection is not established yet
     */
    public StatefulInternalConnection<byte[], byte[]> connection() {
        lock.readLock().lock();
        try {
            if (connection == null) {
                throw new IllegalStateException("Redis connection not established yet");
            }
            return connection;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void shutdown_internal() {
        if (client == null) {
            return;
        }
        // The client should be discarded after calling shutdown.
        client.shutdown();
        connection = null;
        client = null;
    }

    /**
     * Shuts down the current client and releases any associated resources.
     * <p>
     * This method is thread-safe, acquiring a write lock to ensure exclusive access.
     * Upon invocation, it shuts down the existing client if one is present, and removes
     * references to the client and its connection.
     */
    public void shutdown() {
        lock.writeLock().lock();
        try {
            shutdown_internal();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
