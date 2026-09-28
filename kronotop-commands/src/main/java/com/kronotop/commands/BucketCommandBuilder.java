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
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.*;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static io.lettuce.core.protocol.CommandType.HELLO;

public class BucketCommandBuilder<K, V> extends BaseKronotopCommandBuilder<K, V> {
    public BucketCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    private static byte[] encodeVector(float[] floats) {
        ByteBuffer buf = ByteBuffer.allocate(floats.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (float f : floats) buf.putFloat(f);
        return buf.array();
    }

    private static void addNamespace(CommandArgs<?, ?> args, String namespace) {
        if (namespace != null) {
            args.add("NAMESPACE").add(namespace);
        }
    }

    public final Command<K, V, List<String>> insert(String bucket, String namespace, List<V> documents) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket);
        if (namespace != null) {
            args.add("NAMESPACE").add(namespace);
        }
        args.add("DOCS").addValues(documents);
        return createCommand(CommandType.BUCKET_INSERT, new StringListOutput<>(codec), args);
    }

    public final Command<K, V, List<String>> insert(String bucket, List<V> documents) {
        return insert(bucket, null, documents);
    }

    @SafeVarargs
    public final Command<K, V, List<String>> insert(String bucket, V... documents) {
        return insert(bucket, List.of(documents));
    }

    public final Command<K, V, List<String>> insert(String bucket, V document) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add("DOCS").addValue(document);
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

    public final Command<K, V, Map<K, V>> query(String bucket, byte[] query) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(query);
        return createCommand(CommandType.QUERY, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> query(String bucket, byte[] query, BucketQueryArgs bucketQueryArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(query);
        if (bucketQueryArgs != null) {
            bucketQueryArgs.build(args);
        }
        return createCommand(CommandType.QUERY, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> explain(String bucket, String query) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(query);
        return createCommand(CommandType.BUCKET_EXPLAIN, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> explain(String bucket, String query, BucketQueryArgs bucketQueryArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(query);
        if (bucketQueryArgs != null) {
            bucketQueryArgs.build(args);
        }
        return createCommand(CommandType.BUCKET_EXPLAIN, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> advanceQuery(int cursorId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add("QUERY").add(cursorId);
        return createCommand(CommandType.BUCKET_ADVANCE, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> advanceDelete(int cursorId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add("DELETE").add(cursorId);
        return createCommand(CommandType.BUCKET_ADVANCE, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> advanceUpdate(int cursorId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add("UPDATE").add(cursorId);
        return createCommand(CommandType.BUCKET_ADVANCE, new MapOutput<>(codec), args);
    }

    public final Command<K, V, String> close(String operation, int cursorId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(operation).add(cursorId);
        return createCommand(CommandType.BUCKET_CLOSE, new StatusOutput<>(codec), args);
    }

    public Command<String, String, Map<String, Object>> hello(int protocolVersion) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.ASCII).add(protocolVersion);
        return new Command<>(HELLO, new GenericMapOutput<>(StringCodec.ASCII), args);
    }

    public final Command<K, V, String> indexCreate(String bucket, String schemas) {
        return indexCreate(bucket, schemas, null);
    }

    /**
     * Same as {@code indexCreate} but runs the command in the given namespace instead of the session's current one.
     *
     * @param bucket    the bucket name
     * @param schemas   the index definitions
     * @param namespace the namespace to run the command in, or null for the session's current one
     */
    public final Command<K, V, String> indexCreate(String bucket, String schemas, String namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(BucketIndex.CREATE).
                add(bucket).
                add(schemas);
        addNamespace(args, namespace);
        return createCommand(CommandType.BUCKET_INDEX, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<String>> indexList(String bucket) {
        return indexList(bucket, null);
    }

    /**
     * Same as {@code indexList} but runs the command in the given namespace instead of the session's current one.
     *
     * @param bucket    the bucket name
     * @param namespace the namespace to run the command in, or null for the session's current one
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Command<K, V, List<String>> indexList(String bucket, String namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(BucketIndex.LIST).
                add(bucket);
        addNamespace(args, namespace);
        return new Command(CommandType.BUCKET_INDEX, new StringListOutput<>(StringCodec.ASCII), args);
    }

    public Command<String, String, Map<String, Object>> indexDescribe(String bucket, String index) {
        return indexDescribe(bucket, index, null);
    }

    /**
     * Same as {@code indexDescribe} but runs the command in the given namespace instead of the session's current one.
     *
     * @param bucket    the bucket name
     * @param index     the index name
     * @param namespace the namespace to run the command in, or null for the session's current one
     */
    public Command<String, String, Map<String, Object>> indexDescribe(String bucket, String index, String namespace) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).
                add(BucketIndex.DESCRIBE).
                add(bucket).
                add(index);
        addNamespace(args, namespace);
        return new Command<>(CommandType.BUCKET_INDEX, new GenericMapOutput<>(StringCodec.ASCII), args);
    }

    public Command<K, V, String> indexDrop(String bucket, String index) {
        return indexDrop(bucket, index, null);
    }

    /**
     * Same as {@code indexDrop} but runs the command in the given namespace instead of the session's current one.
     *
     * @param bucket    the bucket name
     * @param index     the index name
     * @param namespace the namespace to run the command in, or null for the session's current one
     */
    public Command<K, V, String> indexDrop(String bucket, String index, String namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(BucketIndex.DROP).
                add(bucket).
                add(index);
        addNamespace(args, namespace);
        return createCommand(CommandType.BUCKET_INDEX, new StatusOutput<>(codec), args);
    }

    public Command<K, V, String> indexAnalyze(String bucket, String index) {
        return indexAnalyze(bucket, index, null);
    }

    /**
     * Same as {@code indexAnalyze} but runs the command in the given namespace instead of the session's current one.
     *
     * @param bucket    the bucket name
     * @param index     the index name
     * @param namespace the namespace to run the command in, or null for the session's current one
     */
    public Command<K, V, String> indexAnalyze(String bucket, String index, String namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(BucketIndex.ANALYZE).
                add(bucket).
                add(index);
        addNamespace(args, namespace);
        return createCommand(CommandType.BUCKET_INDEX, new StatusOutput<>(codec), args);
    }

    public Command<String, String, Map<String, Object>> indexTasks(String bucket, String index) {
        return indexTasks(bucket, index, null);
    }

    /**
     * Same as {@code indexTasks} but runs the command in the given namespace instead of the session's current one.
     *
     * @param bucket    the bucket name
     * @param index     the index name
     * @param namespace the namespace to run the command in, or null for the session's current one
     */
    public Command<String, String, Map<String, Object>> indexTasks(String bucket, String index, String namespace) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).
                add(BucketIndex.TASKS).
                add(bucket).
                add(index);
        addNamespace(args, namespace);
        return new Command<>(CommandType.BUCKET_INDEX, new GenericMapOutput<>(StringCodec.ASCII), args);
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

    public final Command<K, V, Map<K, V>> delete(String bucket, byte[] query, BucketQueryArgs bucketQueryArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(query);
        if (bucketQueryArgs != null) {
            bucketQueryArgs.build(args);
        }
        return createCommand(CommandType.BUCKET_DELETE, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> update(String bucket, String query, String update) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(query).add(update);
        return createCommand(CommandType.BUCKET_UPDATE, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> update(String bucket, String query, byte[] update) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(query).add(update);
        return createCommand(CommandType.BUCKET_UPDATE, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> update(String bucket, String query, String update, BucketQueryArgs bucketQueryArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(query).add(update);
        if (bucketQueryArgs != null) {
            bucketQueryArgs.build(args);
        }
        return createCommand(CommandType.BUCKET_UPDATE, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> update(String bucket, byte[] query, byte[] update,
                                                 BucketQueryArgs bucketQueryArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(query).add(update);
        if (bucketQueryArgs != null) {
            bucketQueryArgs.build(args);
        }
        return createCommand(CommandType.BUCKET_UPDATE, new MapOutput<>(codec), args);
    }

    public final Command<K, V, String> remove(String bucket) {
        return remove(bucket, null);
    }

    /**
     * Same as {@code remove} but runs the command in the given namespace instead of the session's current one.
     *
     * @param bucket    the bucket name
     * @param namespace the namespace to run the command in, or null for the session's current one
     */
    public final Command<K, V, String> remove(String bucket, String namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket);
        addNamespace(args, namespace);
        return createCommand(CommandType.BUCKET_REMOVE, new StatusOutput<>(codec), args);
    }

    public final Command<K, V, List<Object>> vector(String bucket, String selector, float[] vector) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(selector).add(encodeVector(vector));
        return createCommand(CommandType.BUCKET_VECTOR, new ArrayOutput<>(codec), args);
    }

    public final Command<K, V, List<Object>> vector(String bucket, String selector, float[] vector, BucketVectorArgs bucketVectorArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).add(selector).add(encodeVector(vector));
        bucketVectorArgs.build(args);
        return createCommand(CommandType.BUCKET_VECTOR, new ArrayOutput<>(codec), args);
    }

    public final Command<K, V, String> purge(String bucket) {
        return purge(bucket, null);
    }

    /**
     * Same as {@code purge} but runs the command in the given namespace instead of the session's current one.
     *
     * @param bucket    the bucket name
     * @param namespace the namespace to run the command in, or null for the session's current one
     */
    public final Command<K, V, String> purge(String bucket, String namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket);
        addNamespace(args, namespace);
        return createCommand(CommandType.BUCKET_PURGE, new StatusOutput<>(codec), args);
    }

    public final Command<K, V, List<Object>> locate(String bucket) {
        return locate(bucket, null);
    }

    /**
     * Same as {@code locate} but runs the command in the given namespace instead of the session's current one.
     *
     * @param bucket    the bucket name
     * @param namespace the namespace to run the command in, or null for the session's current one
     */
    public final Command<K, V, List<Object>> locate(String bucket, String namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket);
        addNamespace(args, namespace);
        return createCommand(CommandType.BUCKET_LOCATE, new ArrayOutput<>(codec), args);
    }

    public final Command<K, V, List<Object>> list() {
        return list(null);
    }

    /**
     * Same as {@code list} but runs the command in the given namespace instead of the session's current one.
     *
     * @param namespace the namespace to run the command in, or null for the session's current one
     */
    public final Command<K, V, List<Object>> list(String namespace) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        addNamespace(args, namespace);
        return createCommand(CommandType.BUCKET_LIST, new ArrayOutput<>(codec), args);
    }

    public final Command<K, V, String> create(String bucket) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket);
        return createCommand(CommandType.BUCKET_CREATE, new StatusOutput<>(codec), args);
    }

    public final Command<K, V, String> create(String bucket, BucketCreateArgs bucketCreateArgs) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket);
        bucketCreateArgs.build(args);
        return createCommand(CommandType.BUCKET_CREATE, new StatusOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> cursors() {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        return createCommand(CommandType.BUCKET_CURSORS, new MapOutput<>(codec), args);
    }

    public final Command<K, V, Map<K, V>> cursors(String operation) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(operation);
        return createCommand(CommandType.BUCKET_CURSORS, new MapOutput<>(codec), args);
    }

    public enum CommandType implements ProtocolKeyword {
        QUERY("QUERY"),
        BUCKET_INSERT("BUCKET.INSERT"),
        BUCKET_QUERY("BUCKET.QUERY"),
        BUCKET_DELETE("BUCKET.DELETE"),
        BUCKET_UPDATE("BUCKET.UPDATE"),
        BUCKET_ADVANCE("BUCKET.ADVANCE"),
        BUCKET_CLOSE("BUCKET.CLOSE"),
        BUCKET_INDEX("BUCKET.INDEX"),
        BUCKET_REMOVE("BUCKET.REMOVE"),
        BUCKET_PURGE("BUCKET.PURGE"),
        BUCKET_EXPLAIN("BUCKET.EXPLAIN"),
        BUCKET_CREATE("BUCKET.CREATE"),
        BUCKET_CURSORS("BUCKET.CURSORS"),
        BUCKET_VECTOR("BUCKET.VECTOR"),
        BUCKET_LOCATE("BUCKET.LOCATE"),
        BUCKET_LIST("BUCKET.LIST");

        public final byte[] bytes;

        CommandType(String name) {
            bytes = name.getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }
    }

    enum BucketIndex implements ProtocolKeyword {
        CREATE("CREATE"),
        LIST("LIST"),
        DESCRIBE("DESCRIBE"),
        DROP("DROP"),
        ANALYZE("ANALYZE"),
        TASKS("TASKS");

        public final byte[] bytes;

        BucketIndex(String name) {
            bytes = name.getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }
    }
}
