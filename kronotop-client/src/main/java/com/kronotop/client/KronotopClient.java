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
