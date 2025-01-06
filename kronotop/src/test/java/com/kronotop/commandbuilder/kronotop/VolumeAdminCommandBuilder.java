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

package com.kronotop.commandbuilder.kronotop;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.MapOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.StringListOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class VolumeAdminCommandBuilder<K, V> extends BaseKronotopCommandBuilder<K, V> {

    public VolumeAdminCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    public Command<K, V, List<String>> list() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.LIST);
        return createCommand(CommandType.VOLUME_ADMIN, new StringListOutput<>(codec), args);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, Map<String, Object>> describe(String name) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.DESCRIBE).add(name);
        return createCommand(CommandType.VOLUME_ADMIN, (MapOutput) new MapOutput<String, Object>((RedisCodec) codec), args);
    }

    public Command<K, V, String> setStatus(String name, String status) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.SET_STATUS).add(name).add(status);
        return createCommand(CommandType.VOLUME_ADMIN, new StatusOutput<>(codec), args);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, Map<String, Object>> replications() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.REPLICATIONS);
        return createCommand(CommandType.VOLUME_ADMIN, (MapOutput) new MapOutput<String, Object>((RedisCodec) codec), args);
    }

    public Command<K, V, String> vacuum(String volumeName, double allowedGarbageRatio) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.VACUUM).add(volumeName).add(allowedGarbageRatio);
        return createCommand(CommandType.VOLUME_ADMIN, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> stopVacuum(String volumeName) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.STOP_VACUUM).add(volumeName);
        return createCommand(CommandType.VOLUME_ADMIN, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> cleanupOrphanFiles(String volumeName) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.CLEANUP_ORPHAN_FILES).add(volumeName);
        return createCommand(CommandType.VOLUME_ADMIN, new StatusOutput<>(codec), args);
    }

    enum CommandType implements ProtocolKeyword {
        VOLUME_ADMIN("VOLUME.ADMIN");

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
        LIST("LIST"),
        DESCRIBE("DESCRIBE"),
        SET_STATUS("SET-STATUS"),
        REPLICATIONS("REPLICATIONS"),
        VACUUM("VACUUM"),
        STOP_VACUUM("STOP-VACUUM"),
        CLEANUP_ORPHAN_FILES("CLEANUP-ORPHAN-FILES");

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
