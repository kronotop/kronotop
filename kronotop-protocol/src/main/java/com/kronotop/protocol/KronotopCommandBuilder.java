/*
 * Copyright (c) 2023 Kronotop
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
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;

public class KronotopCommandBuilder<K, V> extends BaseKronotopCommandBuilder<K, V> {

    public KronotopCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    public Command<K, V, String> auth(K key, V value) {
        return createCommand(CommandType.AUTH, new StatusOutput<>(codec), key, value);
    }

    public Command<K, V, String> begin() {
        return createCommand(CommandType.BEGIN, new StatusOutput<>(codec));
    }

    public Command<K, V, String> rollback() {
        return createCommand(CommandType.ROLLBACK, new StatusOutput<>(codec));
    }

    public Command<K, V, String> commit() {
        return createCommand(CommandType.COMMIT, new StatusOutput<>(codec));
    }

    public Command<K, V, Long> commitAndGetCommittedVersion() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(CommitKeywords.GET_COMMITTED_VERSION);
        return createCommand(CommandType.COMMIT, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, V> commitAndGetVersionstamp() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(CommitKeywords.GET_VERSIONSTAMP);
        return createCommand(CommandType.COMMIT, new ValueOutput<>(codec), args);
    }

    public Command<K, V, String> namespaceOpen(K namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.OPEN).
                addKey(namespace);
        return createCommand(CommandType.NAMESPACE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> namespaceRemove(K namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.REMOVE).
                addKey(namespace);
        return createCommand(CommandType.NAMESPACE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, Long> namespaceExists(K namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.EXISTS).
                addKey(namespace);
        return createCommand(CommandType.NAMESPACE, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, List<Object>> namespaceList(K namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.LIST);
        if (namespace != null) {
            args.addKey(namespace);
        }
        return createCommand(CommandType.NAMESPACE, new ArrayOutput<>(codec), args);
    }

    public Command<K, V, List<Object>> namespaceListOpen(K namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.LIST_OPEN).
                addKey(namespace);
        return createCommand(CommandType.NAMESPACE, new ArrayOutput<>(codec), args);
    }

    public Command<K, V, String> namespaceCreate(K namespace, NamespaceArgs namespaceArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.CREATE).
                addKey(namespace);
        if (namespaceArgs != null) {
            namespaceArgs.build(args);
        }
        return createCommand(CommandType.NAMESPACE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> namespaceMove(K oldNamespace, K newNamespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.MOVE).
                addKeys(oldNamespace, newNamespace);
        return createCommand(CommandType.NAMESPACE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> namespaceCreateOrOpen(K namespace, NamespaceArgs namespaceArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.CREATE_OR_OPEN).
                addKey(namespace);
        if (namespaceArgs != null) {
            namespaceArgs.build(args);
        }
        return createCommand(CommandType.NAMESPACE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> zput(String namespace, K key, V value) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(namespace).
                addKey(key).
                addValue(value);
        return createCommand(CommandType.ZPUT, new StatusOutput<>(codec), args);
    }

    public Command<K, V, V> zget(String namespace, K key) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(namespace).
                addKey(key);
        return createCommand(CommandType.ZGET, new ValueOutput<>(codec), args);
    }

    public Command<K, V, String> zdel(String namespace, K key) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(namespace).
                addKey(key);
        return createCommand(CommandType.ZDEL, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> zdelprefix(byte[] key) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(key);
        return createCommand(CommandType.ZDELPREFIX, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> zdelrange(String namespace, ZDelRangeArgs zDelRangeArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(namespace);
        if (zDelRangeArgs != null) {
            zDelRangeArgs.build(args);
        }
        return createCommand(CommandType.ZDELRANGE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<Object>> zgetrange(String namespace, ZGetRangeArgs zGetRangeArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(namespace);
        if (zGetRangeArgs != null) {
            zGetRangeArgs.build(args);
        }
        return createCommand(CommandType.ZGETRANGE, new ArrayOutput<>(codec), args);
    }

    public Command<K, V, V> zgetkey(String namespace, ZGetKeyArgs zGetKeyArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(namespace);
        if (zGetKeyArgs != null) {
            zGetKeyArgs.build(args);
        }
        return createCommand(CommandType.ZGETKEY, new ValueOutput<>(codec), args);
    }

    public Command<K, V, String> snapshotRead(SnapshotReadArgs snapshotReadArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        if (snapshotReadArgs != null) {
            snapshotReadArgs.build(args);
        }
        return createCommand(CommandType.SNAPSHOT_READ, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> zmutate(String namespace, K key, V param, ZMutateArgs zMutateArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(namespace).
                addKey(key).
                addValue(param);
        if (zMutateArgs != null) {
            zMutateArgs.build(args);
        }
        return createCommand(CommandType.ZMUTATE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, Long> zgetrangesize(String namespace, ZGetRangeSizeArgs zGetRangeSizeArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(namespace);
        if (zGetRangeSizeArgs != null) {
            zGetRangeSizeArgs.build(args);
        }
        return createCommand(CommandType.ZGETRANGESIZE, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, Long> getapproximatesize() {
        return createCommand(CommandType.GETAPPROXIMATESIZE, new IntegerOutput<>(codec));
    }

    public Command<K, V, Long> getreadversion() {
        return createCommand(CommandType.GETREADVERSION, new IntegerOutput<>(codec));
    }
}
