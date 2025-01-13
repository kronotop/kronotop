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
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.StringListOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class KrAdminCommandBuilder<K, V> extends BaseKronotopCommandBuilder<K, V> {

    public KrAdminCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    public Command<K, V, String> initializeCluster() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.INITIALIZE_CLUSTER);
        return createCommand(CommandType.KR_ADMIN, new StatusOutput<>(codec), args);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, Map<String, Object>> listMembers() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.LIST_MEMBERS);
        return createCommand(CommandType.KR_ADMIN, (MapOutput) new MapOutput<String, Object>((RedisCodec) codec), args);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, Map<String, Object>> findMember(String memberId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.FIND_MEMBER).add(memberId);
        return createCommand(CommandType.KR_ADMIN, (MapOutput) new MapOutput<String, Object>((RedisCodec) codec), args);
    }

    public Command<K, V, String> setMemberStatus(String memberID, String status) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.SET_MEMBER_STATUS).add(memberID).add(status);
        return createCommand(CommandType.KR_ADMIN, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> removeMember(String memberID) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.REMOVE_MEMBER).add(memberID);
        return createCommand(CommandType.KR_ADMIN, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<String>> listSilentMembers() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.LIST_SILENT_MEMBERS);
        return createCommand(CommandType.KR_ADMIN, new StringListOutput<>(codec), args);
    }

    public Command<K, V, String> setShardStatus(String shardKind, int sharId, String status) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.SET_SHARD_STATUS).add(shardKind).add(sharId).add(status);
        return createCommand(CommandType.KR_ADMIN, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> setShardStatus(String shardKind, String status) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.SET_SHARD_STATUS).add(shardKind).add("*").add(status);
        return createCommand(CommandType.KR_ADMIN, new StatusOutput<>(codec), args);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, Map<String, Object>> describeShard(String shardKind, int shardId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.DESCRIBE_SHARD).add(shardKind).add(shardId);
        return createCommand(CommandType.KR_ADMIN, (MapOutput) new MapOutput<String, Object>((RedisCodec) codec), args);
    }

    public Command<K, V, String> route(String operationKind, String routeKind, String shardKind, int shardId, String memberId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.ROUTE).
                add(operationKind).
                add(routeKind).
                add(shardKind).
                add(shardId).
                add(memberId);
        return createCommand(CommandType.KR_ADMIN, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> route(String operationKind, String routeKind, String shardKind, String memberId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.ROUTE).
                add(operationKind).
                add(routeKind).
                add(shardKind).
                add("*").
                add(memberId);
        return createCommand(CommandType.KR_ADMIN, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> syncStandby(String operationKind, String shardKind, int shardId, String memberId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.SYNC_STANDBY).
                add(operationKind).
                add(shardKind).
                add(shardId).
                add(memberId);
        return createCommand(CommandType.KR_ADMIN, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> syncStandby(String operationKind, String shardKind, String memberId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(CommandKeyword.SYNC_STANDBY).
                add(operationKind).
                add(shardKind).
                add("*").
                add(memberId);
        return createCommand(CommandType.KR_ADMIN, new StatusOutput<>(codec), args);
    }

    enum CommandType implements ProtocolKeyword {
        KR_ADMIN("KR.ADMIN");

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
        INITIALIZE_CLUSTER("INITIALIZE-CLUSTER"),
        LIST_MEMBERS("LIST-MEMBERS"),
        FIND_MEMBER("FIND-MEMBER"),
        SET_MEMBER_STATUS("SET-MEMBER-STATUS"),
        REMOVE_MEMBER("REMOVE-MEMBER"),
        LIST_SILENT_MEMBERS("LIST-SILENT-MEMBERS"),
        SET_SHARD_STATUS("SET-SHARD-STATUS"),
        DESCRIBE_SHARD("DESCRIBE-SHARD"),
        ROUTE("ROUTE"),
        SYNC_STANDBY("SYNC-STANDBY");

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
