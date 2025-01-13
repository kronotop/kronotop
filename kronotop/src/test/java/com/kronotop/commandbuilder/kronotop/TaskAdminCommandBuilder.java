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

package com.kronotop.commandbuilder.kronotop;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.MapOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TaskAdminCommandBuilder<K, V> extends BaseKronotopCommandBuilder<K, V> {

    public TaskAdminCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, Map<String, Object>> list() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.LIST);
        return createCommand(CommandType.TASK_ADMIN, (MapOutput) new MapOutput<String, Object>((RedisCodec) codec), args);
    }

    enum CommandType implements ProtocolKeyword {
        TASK_ADMIN("TASK.ADMIN");

        public final byte[] bytes;

        CommandType(String name) {
            bytes = name.getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }
    }

    enum CommandKeyword implements ProtocolKeyword {
        LIST("LIST");

        public final byte[] bytes;

        CommandKeyword(String name) {
            bytes = name.getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }
    }
}
