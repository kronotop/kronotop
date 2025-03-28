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

package com.kronotop.protocol;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;

public class KronotopAsyncCommandsImpl<K, V> extends AbstractKronotopAsyncCommands<K, V> implements KronotopAsyncCommands<K, V> {
    public KronotopAsyncCommandsImpl(StatefulConnection<K, V> connection, RedisCodec<K, V> codec) {
        super(connection, codec);
    }
}
