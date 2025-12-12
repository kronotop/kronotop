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

import com.kronotop.cluster.client.protocol.InternalAsyncCommands;
import com.kronotop.cluster.client.protocol.InternalAsyncCommandsImpl;
import com.kronotop.cluster.client.protocol.InternalCommands;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.NodeSelection;
import io.lettuce.core.cluster.api.sync.NodeSelectionCommands;
import io.lettuce.core.codec.RedisCodec;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

/**
 * StatefulInternalConnection encapsulates a stateful connection to a Redis database with both synchronous and asynchronous command interfaces.
 *
 * @param <K> the type of keys maintained by this connection
 * @param <V> the type of values maintained by this connection
 */
public class StatefulInternalConnection<K, V> {
    private final StatefulRedisConnection<K, V> connection;
    private final InternalAsyncCommands<K, V> async;
    private final InternalCommands<K, V> sync;

    public StatefulInternalConnection(StatefulRedisConnection<K, V> connection, RedisCodec<K, V> codec) {
        this.connection = connection;
        this.async = new InternalAsyncCommandsImpl<>(connection, codec);
        this.sync = newKronotopCommandsImpl();
    }

    public static StatefulInternalConnection<String, String> connect(RedisClient redisClient) {
        return InternalClient.connect(redisClient);
    }

    public static <K, V> StatefulInternalConnection<K, V> connect(RedisClient redisClient, RedisCodec<K, V> codec) {
        StatefulRedisConnection<K, V> connection = redisClient.connect(codec);
        return new StatefulInternalConnection<>(connection, codec);
    }

    public InternalAsyncCommands<K, V> async() {
        return async;
    }

    public InternalCommands<K, V> sync() {
        return sync;
    }

    private InternalCommands<K, V> newKronotopCommandsImpl() {
        return clusterSyncHandler(InternalCommands.class);
    }

    @SuppressWarnings("unchecked")
    private <T> T clusterSyncHandler(Class<?>... interfaces) {
        return (T) Proxy.newProxyInstance(AbstractRedisClient.class.getClassLoader(), interfaces, syncInvocationHandler());
    }

    private InvocationHandler syncInvocationHandler() {
        return new ClusterFutureSyncInvocationHandler<>(connection, RedisClusterAsyncCommands.class, NodeSelection.class,
                NodeSelectionCommands.class, async());
    }

    public void close() {
        connection.close();
    }
}
