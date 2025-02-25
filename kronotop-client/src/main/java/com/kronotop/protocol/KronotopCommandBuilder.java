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

    public Command<K, V, List<Object>> commit(CommitArgs commitArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        commitArgs.build(args);
        return createCommand(CommandType.COMMIT, new ArrayOutput<>(codec), args);
    }

    public Command<K, V, String> namespaceRemove(K namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.REMOVE).
                addKey(namespace);
        return createCommand(CommandType.NAMESPACE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> namespaceUse(K namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.USE).
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

    public Command<K, V, String> namespaceCreate(K namespace) {
        return namespaceCreate(namespace, null);
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

    public Command<K, V, String> namespaceCurrent() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(NamespaceKeywords.CURRENT);
        return createCommand(CommandType.NAMESPACE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> zset(K key, V value) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key).
                addValue(value);
        return createCommand(CommandType.ZSET, new StatusOutput<>(codec), args);
    }

    public Command<K, V, V> zget(K key) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key);
        return createCommand(CommandType.ZGET, new ValueOutput<>(codec), args);
    }

    public Command<K, V, String> zdel(K key) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key);
        return createCommand(CommandType.ZDEL, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> zdelprefix(byte[] key) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(key);
        return createCommand(CommandType.ZDELPREFIX, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> zdelrange(ZDelRangeArgs zDelRangeArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        if (zDelRangeArgs != null) {
            zDelRangeArgs.build(args);
        }
        return createCommand(CommandType.ZDELRANGE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<Object>> zgetrange(ZGetRangeArgs zGetRangeArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        if (zGetRangeArgs != null) {
            zGetRangeArgs.build(args);
        }
        return createCommand(CommandType.ZGETRANGE, new ArrayOutput<>(codec), args);
    }

    public Command<K, V, V> zgetkey(ZGetKeyArgs zGetKeyArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
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
        return createCommand(CommandType.SNAPSHOTREAD, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> zmutate(K key, V param, ZMutateArgs zMutateArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                addKey(key).
                addValue(param);
        if (zMutateArgs != null) {
            zMutateArgs.build(args);
        }
        return createCommand(CommandType.ZMUTATE, new StatusOutput<>(codec), args);
    }

    public Command<K, V, Long> zgetrangesize(ZGetRangeSizeArgs zGetRangeSizeArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        if (zGetRangeSizeArgs != null) {
            zGetRangeSizeArgs.build(args);
        }
        return createCommand(CommandType.ZGETRANGESIZE, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, Long> getApproximateSize() {
        return createCommand(CommandType.GETAPPROXIMATESIZE, new IntegerOutput<>(codec));
    }

    public Command<K, V, Long> getreadversion() {
        return createCommand(CommandType.GETREADVERSION, new IntegerOutput<>(codec));
    }
}
