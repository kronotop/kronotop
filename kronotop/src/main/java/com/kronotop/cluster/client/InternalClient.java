/*
 * Copyright (c) 2023-2025 Kronotop
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

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class InternalClient {

    public static StatefulInternalConnection<String, String> connect(RedisClient client) {
        StatefulRedisConnection<String, String> connection = client.connect();
        return new StatefulInternalConnection<>(connection, StringCodec.UTF8);
    }

    public static <K, V> StatefulInternalConnection<K, V> connect(RedisClient client, RedisCodec<K, V> codec) {
        StatefulRedisConnection<K, V> connection = client.connect(codec);
        return new StatefulInternalConnection<>(connection, codec);
    }
}
