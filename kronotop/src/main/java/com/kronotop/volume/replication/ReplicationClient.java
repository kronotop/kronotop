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

package com.kronotop.volume.replication;

import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.client.InternalClient;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.cluster.sharding.ShardKind;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.lettuce.core.*;
import io.lettuce.core.codec.ByteArrayCodec;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.locks.ReentrantLock;

public class ReplicationClient {
    private final Context context;
    private final ShardKind shardKind;
    private final int shardId;

    // Using ReentrantLock to avoid Virtual Thread pinning.
    private final ReentrantLock lock = new ReentrantLock();

    private Member server;
    private RedisClient client;

    // Store options for reconnection.
    private ClientOptions options;

    // connection and connected are kept volatile (since conn() method is lock-free)
    private volatile StatefulInternalConnection<byte[], byte[]> connection;
    private volatile boolean connected;
    private volatile boolean shutdown;

    ReplicationClient(Context context, ShardKind shardKind, int shardId) {
        this.shardKind = shardKind;
        this.shardId = shardId;
        this.context = context;
    }

    public void reconnect() {
        if (options == null) {
            throw new KronotopException("Cannot reconnect: no previous ClientOptions available");
        }
        connect(options);
    }

    public void connect(ClientOptions options) {
        lock.lock();
        try {
            this.options = options;
            // Reject connection if the client has been shutdown externally
            if (shutdown) {
                throw new ReplicationClientShutdownException();
            }

            RoutingService routing = context.getService(RoutingService.NAME);
            Route route = routing.findRoute(shardKind, shardId);
            if (route == null) {
                throw new IllegalArgumentException("Route not found: " + shardKind + " " + shardId);
            }

            Member member = route.primary();

            // If already connected and target server hasn't changed (no topology change),
            // Lettuce handles network blips internally via auto-reconnect.
            // No need to create a new client.
            if (connected && member.equals(server)) {
                return;
            }

            // Reset state if the target server changed or this is the first connection
            connected = false;

            // Clean up existing client if any
            cleanupResources();

            RedisURI.Builder builder = RedisURI.Builder.
                    redis(member.getInternalAddress().getHost(), member.getInternalAddress().getPort());
            RedisClient redisClient = RedisClient.create(builder.build());
            redisClient.setOptions(options);

            // Create a Lettuce connection with retry for transient failures
            client = redisClient;
            Retry retry = connectionRetry();
            connection = Retry.decorateSupplier(retry,
                    () -> InternalClient.connect(redisClient, ByteArrayCodec.INSTANCE)).get();

            server = member;
            // Safe publication: set to true after all assignments are complete
            connected = true;
        } finally {
            lock.unlock();
        }
    }

    public StatefulInternalConnection<byte[], byte[]> conn() {
        // Fast-path: volatile read without acquiring lock
        if (shutdown) {
            throw new ReplicationClientShutdownException();
        }
        if (!connected || connection == null) {
            throw new ReplicationClientNotConnectedException();
        }
        return connection;
    }

    public void shutdown() {
        lock.lock(); // Acquire lock to prevent race condition with connect()
        try {
            if (shutdown) {
                return;
            }
            shutdown = true;
            connected = false;
            cleanupResources();
        } finally {
            lock.unlock();
        }
    }

    // Helper method that must be called while holding the lock
    private void cleanupResources() {
        if (client != null) {
            client.getOptions().mutate().autoReconnect(false);

            if (connection != null) {
                connection.close();
                connection = null;
            }

            try {
                // RedisClient is a heavy resource, must be closed.
                client.shutdown();
                client.getResources().shutdown();
            } catch (Exception ignored) {
                // We can swallow errors during shutdown
            }
            client = null;
        }
        // Server info is now invalid
        server = null;
    }

    private Throwable getRootCause(Throwable t) {
        Throwable result = t;
        while (result.getCause() != null && result.getCause() != result) {
            result = result.getCause();
        }
        return result;
    }

    private Retry connectionRetry() {
        return Retry.of("replication-client-connect", RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofSeconds(1))
                .retryOnException(throwable -> {
                    if (shutdown) {
                        return false;
                    }
                    Throwable root = getRootCause(throwable);
                    return root instanceof ConnectException ||
                            root instanceof SocketTimeoutException ||
                            root instanceof UnknownHostException ||
                            root instanceof RedisConnectionException ||
                            root instanceof RedisCommandTimeoutException;
                }).build());
    }
}