package com.kronotop.client;

import com.kronotop.protocol.KronotopAsyncCommands;
import com.kronotop.protocol.KronotopAsyncCommandsImpl;
import com.kronotop.protocol.KronotopCommands;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.NodeSelection;
import io.lettuce.core.cluster.api.sync.NodeSelectionCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

/**
 * Represents a stateful connection to Kronotop using Redis Cluster Protocol.
 *
 * @param <K> the type for Redis keys
 * @param <V> the type for Redis values
 */
public class StatefulKronotopConnection<K, V> {
    private final StatefulRedisClusterConnection<K, V> connection;
    private final KronotopAsyncCommands<K, V> async;
    private final KronotopCommands<K, V> sync;

    public StatefulKronotopConnection(StatefulRedisClusterConnection<K, V> connection, RedisCodec<K, V> codec) {
        this.connection = connection;
        this.async = new KronotopAsyncCommandsImpl<>(connection, codec);
        this.sync = newKronotopCommandsImpl();

    }

    public static StatefulKronotopConnection<String, String> connect(RedisClusterClient redisClusterClient) {
        StatefulRedisClusterConnection<String, String> connection = redisClusterClient.connect();
        return new StatefulKronotopConnection<>(connection, StringCodec.UTF8);
    }

    public static <K, V> StatefulKronotopConnection<K, V> connect(RedisClusterClient redisClusterClient, RedisCodec<K, V> codec) {
        StatefulRedisClusterConnection<K, V> connection = redisClusterClient.connect(codec);
        return new StatefulKronotopConnection<>(connection, codec);
    }

    public KronotopAsyncCommands<K, V> async() {
        return async;
    }

    public KronotopCommands<K, V> sync() {
        return sync;
    }

    private KronotopCommands<K, V> newKronotopCommandsImpl() {
        return clusterSyncHandler(KronotopCommands.class);
    }

    @SuppressWarnings("unchecked")
    private <T> T clusterSyncHandler(Class<?>... interfaces) {
        return (T) Proxy.newProxyInstance(AbstractRedisClient.class.getClassLoader(), interfaces, syncInvocationHandler());
    }

    private InvocationHandler syncInvocationHandler() {
        return new ClusterFutureSyncInvocationHandler<>(connection, RedisClusterAsyncCommands.class, NodeSelection.class,
                NodeSelectionCommands.class, async());
    }
}
