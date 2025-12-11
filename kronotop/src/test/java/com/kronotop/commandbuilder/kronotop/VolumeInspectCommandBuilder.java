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

public class VolumeInspectCommandBuilder<K, V> extends BaseKronotopCommandBuilder<K, V> {

    public VolumeInspectCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, Map<String, Object>> cursor(String volumeName) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(CommandKeyword.CURSOR).
                add(volumeName);
        return createCommand(CommandType.VOLUME_INSPECT, (MapOutput) new MapOutput<String, Object>((RedisCodec) codec), args);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, Map<String, Object>> replication(String shardKind, int shardId, String standbyId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(CommandKeyword.REPLICATION).
                add(shardKind).
                add(shardId).
                add(standbyId);
        return createCommand(CommandType.VOLUME_INSPECT, (MapOutput) new MapOutput<String, Object>((RedisCodec) codec), args);
    }

    enum CommandType implements ProtocolKeyword {
        VOLUME_INSPECT("VOLUME.INSPECT");

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
        CURSOR("CURSOR"),
        REPLICATION("REPLICATION");

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
