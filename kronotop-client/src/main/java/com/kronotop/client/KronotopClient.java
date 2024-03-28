package com.kronotop.client;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

/**
 * The KronotopClient class provides methods to connect to a Kronotop cluster and retrieve a StatefulKronotopConnection.
 * It supports connecting with a RedisCodec or using the default StringCodec.
 */
public class KronotopClient {

    public static StatefulKronotopConnection<String, String> connect(RedisClusterClient redisClusterClient) {
        StatefulRedisClusterConnection<String, String> connection = redisClusterClient.connect();
        return new StatefulKronotopConnection<>(connection, StringCodec.UTF8);
    }

    public static <K, V> StatefulKronotopConnection<K, V> connect(RedisClusterClient redisClusterClient, RedisCodec<K, V> codec) {
        StatefulRedisClusterConnection<K, V> connection = redisClusterClient.connect(codec);
        return new StatefulKronotopConnection<>(connection, codec);
    }
}
