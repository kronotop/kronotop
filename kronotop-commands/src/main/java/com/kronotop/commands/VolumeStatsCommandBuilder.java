/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.commands;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.MapOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class VolumeStatsCommandBuilder<K, V> extends BaseKronotopCommandBuilder<K, V> {

    public VolumeStatsCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, Map<String, Object>> overview(String volumeName) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(volumeName);
        return createCommand(CommandType.VOLUME_STATS, (MapOutput) new MapOutput<String, Object>((RedisCodec) codec), args);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, Map<String, Object>> opcounters(String volumeName) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(volumeName).add(CommandKeyword.OPCOUNTERS);
        return createCommand(CommandType.VOLUME_STATS, (MapOutput) new MapOutput<String, Object>((RedisCodec) codec), args);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, Map<String, Object>> replication(String volumeName, String shardKind, int shardId, String standbyId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec)
                .add(volumeName)
                .add(CommandKeyword.REPLICATION)
                .add(shardKind)
                .add(shardId)
                .add(standbyId);
        return createCommand(CommandType.VOLUME_STATS, (MapOutput) new MapOutput<String, Object>((RedisCodec) codec), args);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, List<Object>> segments(String volumeName) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(volumeName).add(CommandKeyword.SEGMENTS);
        return createCommand(CommandType.VOLUME_STATS, (ArrayOutput) new ArrayOutput<>((RedisCodec) codec), args);
    }

    public Command<K, V, String> reset(String volumeName) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(volumeName).add(CommandKeyword.RESET);
        return createCommand(CommandType.VOLUME_STATS, new StatusOutput<>(codec), args);
    }

    enum CommandType implements ProtocolKeyword {
        VOLUME_STATS("VOLUME.STATS");

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
        OPCOUNTERS("OPCOUNTERS"),
        REPLICATION("REPLICATION"),
        SEGMENTS("SEGMENTS"),
        RESET("RESET");

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
