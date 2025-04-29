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

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

/**
 * BaseKronotopCommandBuilder is a utility class used to construct Redis commands
 * with the support of a specified {@link RedisCodec}. It provides a series of
 * methods to create commands with varying levels of arguments, such as keys
 * and values, for flexible protocol interactions.
 *
 * @param <K> the type of keys used in the Redis commands
 * @param <V> the type of values used in the Redis commands
 */
public class BaseKronotopCommandBuilder<K, V> {

    protected final RedisCodec<K, V> codec;

    public BaseKronotopCommandBuilder(RedisCodec<K, V> codec) {
        this.codec = codec;
    }

    /**
     * Creates a new command for the specified protocol keyword and command output.
     * This is a simplified command creation that does not include any additional arguments.
     *
     * @param <T> the type of the result returned by the command
     * @param type the protocol keyword that defines the type of command to create
     * @param output the output handler for processing the result of the command
     * @return a newly created command instance with the specified parameters
     */
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