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

package com.kronotop.cluster.client;

import com.google.common.util.concurrent.Striped;
import com.kronotop.cluster.Member;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.RedisCodec;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * InternalConnectionPool manages a pool of connections to Kronotop servers identified by members.
 * It utilizes RedisCodec for encoding and decoding keys and values. The connection
 * pool maintains thread-safe access with the use of striped read-write locks.
 *
 * @param <K> the type of keys managed by the connection pool
 * @param <V> the type of values managed by the connection pool
 */
public class InternalConnectionPool<K, V> {
    private final RedisCodec<K, V> codec;
    private final ConcurrentHashMap<Member, RedisClient> clients = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Member, StatefulInternalConnection<K, V>> connections = new ConcurrentHashMap<>();
    private final Striped<ReadWriteLock> striped = Striped.readWriteLock(7);
    private volatile boolean shutdown;

    public InternalConnectionPool(RedisCodec<K, V> codec) {
        this.codec = codec;
    }

    /**
     * Retrieves a stateful internal connection associated with the given member. If no
     * existing connection is found, a new connection is established and returned.
     * The method ensures thread-safe access using read-write locks.
     *
     * @param member the member for which the connection is requested
     * @return a stateful internal connection associated with the specified member
     * @throws IllegalStateException if the connection pool is shut down
     */
    public StatefulInternalConnection<K, V> get(Member member) {
        if (shutdown) {
            throw new IllegalStateException("Connection pool is shut down");
        }

        ReadWriteLock lock = striped.get(member);

        lock.readLock().lock();
        try {
            StatefulInternalConnection<K, V> connection = connections.get(member);
            if (connection != null) {
                return connection;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            RedisClient client = clients.get(member);
            if (client == null) {
                String uri = String.format("redis://%s:%d", member.getInternalAddress().getHost(), member.getInternalAddress().getPort());
                client = RedisClient.create(uri);
                clients.put(member, client);
            }
            StatefulInternalConnection<K, V> connection = InternalClient.connect(client, codec);
            connections.put(member, connection);
            return connection;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Shuts down the connection and client associated with the specified member.
     * This method acquires a write lock for the member to ensure thread-safe operation.
     * If the member has no associated client, the method returns immediately without any action.
     * Otherwise, it shuts down the RedisClient, then removes the member's connection and client
     * from their respective maps.
     *
     * @param member the member whose associated Redis client and connection should be shut down
     */
    public void shutdown(Member member) {
        ReadWriteLock lock = striped.get(member);

        lock.writeLock().lock();
        try {
            RedisClient client = clients.get(member);
            if (client == null) {
                return;
            }
            client.shutdown();
            connections.remove(member);
            clients.remove(member);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Initiates the shutdown process for the InternalConnectionPool. Once initiated,
     * the pool enters a state where no new connections can be retrieved or
     * created, and the existing connections associated with all members
     * are systematically shut down.
     * <p>
     * This method sets an internal flag indicating the shutdown process
     * and iterates over all current members in the clients map,
     * invoking a shutdown on each individual member.
     * <p>
     * Note: This action is irreversible, and once the shutdown process
     * begins, the pool cannot be restarted.
     */
    public void shutdown() {
        shutdown = true;
        for (Member member : clients.keySet()) {
            shutdown(member);
        }
    }
}
