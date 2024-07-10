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


package com.kronotop.cluster.client.protocol;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.RedisCommand;

import java.util.List;

public abstract class AbstractInternalAsyncCommands<K, V> implements InternalAsyncCommands<K, V> {
    private final InternalCommandBuilder<K, V> commandBuilder;
    private final StatefulConnection<K, V> connection;

    protected AbstractInternalAsyncCommands(StatefulConnection<K, V> connection, RedisCodec<K, V> codec) {
        this.commandBuilder = new InternalCommandBuilder<>(codec);
        this.connection = connection;
    }

    @Override
    public RedisFuture<List<Object>> segmentRange(String volume, String segment, SegmentRange... ranges) {
        return dispatch(commandBuilder.segmentrange(volume, segment, ranges));
    }

    private <T> AsyncCommand<K, V, T> dispatch(RedisCommand<K, V, T> cmd) {
        AsyncCommand<K, V, T> asyncCommand = new AsyncCommand<>(cmd);
        RedisCommand<K, V, T> dispatched = connection.dispatch(asyncCommand);
        if (dispatched instanceof AsyncCommand) {
            return (AsyncCommand<K, V, T>) dispatched;
        }
        return asyncCommand;
    }

}