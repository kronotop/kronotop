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

package com.kronotop.protocol;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

public class BaseKronotopCommandBuilder<K, V> {

    protected final RedisCodec<K, V> codec;

    public BaseKronotopCommandBuilder(RedisCodec<K, V> codec) {
        this.codec = codec;
    }

    protected <T> Command<K, V, T> createCommand(ProtocolKeyword type, CommandOutput<K, V, T> output) {
        return createCommand(type, output, (CommandArgs<K, V>) null);
    }

    protected <T> Command<K, V, T> createCommand(ProtocolKeyword type, CommandOutput<K, V, T> output, K key) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key);
        return createCommand(type, output, args);
    }

    protected <T> Command<K, V, T> createCommand(ProtocolKeyword type, CommandOutput<K, V, T> output, K key, V value) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addValue(value);
        return createCommand(type, output, args);
    }

    protected <T> Command<K, V, T> createCommand(ProtocolKeyword type, CommandOutput<K, V, T> output, K key, V[] values) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addValues(values);
        return createCommand(type, output, args);
    }

    protected <T> Command<K, V, T> createCommand(ProtocolKeyword type, CommandOutput<K, V, T> output, CommandArgs<K, V> args) {
        return new Command<>(type, output, args);
    }
}