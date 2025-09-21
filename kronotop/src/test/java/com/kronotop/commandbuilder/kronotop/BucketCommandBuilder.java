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
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.*;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.kronotop.bucket.handlers.protocol.InsertArgumentKey.DOCS;
import static io.lettuce.core.protocol.CommandType.HELLO;

public class BucketCommandBuilder<K, V> extends BaseKronotopCommandBuilder<K, V> {
    public BucketCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    public final Command<K, V, List<String>> insert(String bucket, List<V> documents) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(DOCS.toString()).addValues(documents);
        return createCommand(CommandType.BUCKET_INSERT, new StringListOutput<>(codec), args);
    }

    @SafeVarargs
    public final Command<K, V, List<String>> insert(String bucket, V... documents) {
        return insert(bucket, List.of(documents));
    }

    public final Command<K, V, List<String>> insert(String bucket, BucketInsertArgs bucketInsertArgs, List<V> documents) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket);
        if (bucketInsertArgs != null) {
            bucketInsertArgs.build(args);
        }
        args.add(DOCS.toString()).addValues(documents);
        return createCommand(CommandType.BUCKET_INSERT, new StringListOutput<>(codec), args);
    }

    @SafeVarargs
    public final Command<K, V, List<String>> insert(String bucket, BucketInsertArgs bucketInsertArgs, V... documents) {
        return insert(bucket, bucketInsertArgs, List.of(documents));
    }

    public final Command<K, V, List<String>> insert(String bucket, V document) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(DOCS.toString()).addValue(document);
        return createCommand(CommandType.BUCKET_INSERT, new StringListOutput<>(codec), args);
    }

    public final Command<K, V, List<String>> insert(String bucket, BucketInsertArgs bucketInsertArgs, V document) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket);
        if (bucketInsertArgs != null) {
            bucketInsertArgs.build(args);
        }
        args.add("DOCS").addValue(document);
        return createCommand(CommandType.BUCKET_INSERT, new StringListOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> query(String bucket, String query) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(query);
        return createCommand(CommandType.QUERY, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> query(String bucket, String query, BucketQueryArgs bucketQueryArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(query);
        if (bucketQueryArgs != null) {
            bucketQueryArgs.build(args);
        }
        return createCommand(CommandType.QUERY, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> advanceQuery(int cursorId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add("QUERY").add(cursorId);
        return createCommand(CommandType.BUCKET_ADVANCE, new MapOutput<>(codec), args);
    }

    public final Command<K, V, List<K>> advanceDelete(int cursorId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add("DELETE").add(cursorId);
        return createCommand(CommandType.BUCKET_ADVANCE, new KeyListOutput<>(codec), args);
    }

    public final Command<K, V, List<K>> advanceUpdate(int cursorId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add("UPDATE").add(cursorId);
        return createCommand(CommandType.BUCKET_ADVANCE, new KeyListOutput<>(codec), args);
    }

    public Command<String, String, Map<String, Object>> hello(int protocolVersion) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.ASCII).add(protocolVersion);
        return new Command<>(HELLO, new GenericMapOutput<>(StringCodec.ASCII), args);
    }

    public final Command<K, V, String> createIndex(String bucket, String definitions) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(definitions);
        return createCommand(CommandType.BUCKET_CREATE_INDEX, new StatusOutput<>(codec), args);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, List<Map<String, Object>>> listIndexes(String bucket) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.add(bucket);
        return new Command(CommandType.BUCKET_LIST_INDEXES, new ListOfGenericMapsOutput<>(StringCodec.ASCII), args);
    }

    public Command<String, String, Map<String, Object>> describeIndex(String bucket, String index) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).add(bucket).add(index);
        return new Command<>(CommandType.BUCKET_DESCRIBE_INDEX, new GenericMapOutput<>(StringCodec.ASCII), args);
    }

    public Command<K, V, String> dropIndex(String bucket, String index) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(index);
        return createCommand(CommandType.BUCKET_DROP_INDEX, new StatusOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> delete(String bucket, String query) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(query);
        return createCommand(CommandType.BUCKET_DELETE, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> delete(String bucket, String query, BucketQueryArgs bucketQueryArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(query);
        if (bucketQueryArgs != null) {
            bucketQueryArgs.build(args);
        }
        return createCommand(CommandType.BUCKET_DELETE, new MapOutput<>(codec), args);
    }

    enum CommandType implements ProtocolKeyword {
        BUCKET_INSERT("BUCKET.INSERT"),
        BUCKET_QUERY("BUCKET.QUERY"),
        BUCKET_DELETE("BUCKET.DELETE"),
        QUERY("QUERY"),
        BUCKET_ADVANCE("BUCKET.ADVANCE"),
        BUCKET_CREATE_INDEX("BUCKET.CREATE-INDEX"),
        BUCKET_LIST_INDEXES("BUCKET.LIST-INDEXES"),
        BUCKET_DESCRIBE_INDEX("BUCKET.DESCRIBE-INDEX"),
        BUCKET_DROP_INDEX("BUCKET.DROP-INDEX");

        public final byte[] bytes;

        CommandType(String name) {
            bytes = name.getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }
    }
}
