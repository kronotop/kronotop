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


package com.kronotop.bucket.handlers;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.CommitHook;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.handlers.protocol.BucketOperation;
import com.kronotop.bucket.handlers.protocol.QueryArguments;
import com.kronotop.bucket.pipeline.PipelineNode;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.bucket.pipeline.QueryOptions;
import com.kronotop.bucket.pipeline.UpdateOptions;
import com.kronotop.cluster.Route;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.network.Address;
import com.kronotop.server.*;
import com.kronotop.server.resp3.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.Document;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public abstract class AbstractBucketHandler implements Handler {
    private final static RedisMessage CURSOR_ID_MESSAGE_KEY = new SimpleStringRedisMessage("cursor_id");
    private final static RedisMessage ENTRIES_MESSAGE_KEY = new SimpleStringRedisMessage("entries");
    private final static RedisMessage VERSIONSTAMPS_MESSAGE_KEY = new SimpleStringRedisMessage("versionstamps");
    protected final BucketService service;
    protected final Context context;

    public AbstractBucketHandler(BucketService service) {
        this.service = service;
        this.context = service.getContext();
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    /**
     * Retrieves the input type associated with the given request's session.
     *
     * @param request the request object containing the session from which the input type is retrieved
     * @return the input type associated with the session, typically either JSON or BSON
     */
    InputType getInputType(Request request) {
        return request.getSession().attr(com.kronotop.server.SessionAttributes.INPUT_TYPE).get();
    }

    /**
     * Retrieves the reply type associated with the given request's session.
     *
     * @param request the request object containing the session from which the reply type is retrieved
     * @return the reply type associated with the session, typically either JSON or BSON
     */
    private ReplyType getReplyType(Request request) {
        return request.getSession().attr(com.kronotop.server.SessionAttributes.REPLY_TYPE).get();
    }

    /**
     * Prepares a {@code ByteBuf} value based on the reply type derived from the given request.
     * The method generates either a BSON or JSON formatted {@code ByteBuf} depending on the reply type
     * of the session associated with the request. In case of an unsupported reply type, an exception is thrown.
     *
     * @param request the {@code Request} object containing the session information and other relevant data
     * @param entry   a {@code Map.Entry} containing a {@code Versionstamp} as the key and a {@code ByteBuffer} as the value,
     *                where the value represents the data to be transformed into the {@code ByteBuf}
     * @return a {@code ByteBuf} object containing the serialized data in the format specified by the reply type
     */
    protected ByteBuf prepareValue(Request request, Map.Entry<Versionstamp, ByteBuffer> entry) {
        ReplyType replyType = getReplyType(request);

        return switch (replyType) {
            case ReplyType.BSON -> {
                ByteBuffer bb = entry.getValue();
                yield Unpooled.wrappedBuffer(bb.slice());
            }
            case ReplyType.JSON -> {
                Document document = BSONUtil.toDocument(entry.getValue().array());
                byte[] data = document.toJson().getBytes(StandardCharsets.UTF_8);
                yield Unpooled.wrappedBuffer(data);
            }
        };
    }

    /**
     * Processes and writes a RESP3-compliant response to the client based on the provided {@code ReadResponse}.
     * Converts the supplied {@code ReadResponse} into a Redis-compatible data format and sends the resulting
     * data structure to the client using the {@code Response} object. Handles both scenarios where the response
     * contains entries and where it is empty.
     *
     * @param request      the {@code Request} object containing session and command-related information
     * @param response     the {@code Response} object used to send the processed results back to the client
     * @param readResponse the {@code ReadResponse} object containing the cursor ID and entries to be converted
     *                     into a Redis-compliant map response
     */
    protected void resp3Response(Request request, Response response, BucketEntriesMapResponse readResponse) {
        Map<RedisMessage, RedisMessage> root = new LinkedHashMap<>();
        root.put(CURSOR_ID_MESSAGE_KEY, new IntegerRedisMessage(readResponse.cursorId()));
        if (readResponse.entries() == null || readResponse.entries().isEmpty()) {
            root.put(ENTRIES_MESSAGE_KEY, MapRedisMessage.EMPTY_INSTANCE);
            response.writeMap(root);
            return;
        }
        Map<RedisMessage, RedisMessage> entries = new LinkedHashMap<>();
        for (Map.Entry<Versionstamp, ByteBuffer> entry : readResponse.entries().entrySet()) {
            ByteBuf value = prepareValue(request, entry);
            entries.put(
                    new SimpleStringRedisMessage(VersionstampUtil.base32HexEncode(entry.getKey())),
                    new FullBulkStringRedisMessage(value)
            );
        }
        root.put(ENTRIES_MESSAGE_KEY, new MapRedisMessage(entries));
        response.writeMap(root);
    }

    /**
     * Processes and writes a RESP2-compliant response to the client based on the given {@code ReadResponse}.
     * Converts the provided {@code ReadResponse} into a Redis-compatible data format and sends the resulting
     * data structure to the client using the {@code Response} object.
     *
     * @param request      the {@code Request} object containing the session and command information
     * @param response     the {@code Response} object used to send the result back to the client
     * @param readResponse the {@code ReadResponse} object containing the cursor ID and entries
     *                     to be processed and sent as a Redis-compliant response
     */
    protected void resp2Response(Request request, Response response, BucketEntriesMapResponse readResponse) {
        List<RedisMessage> root = new LinkedList<>();
        root.add(new IntegerRedisMessage(readResponse.cursorId()));
        if (readResponse.entries() == null || readResponse.entries().isEmpty()) {
            root.add(ArrayRedisMessage.EMPTY_INSTANCE);
            response.writeArray(root);
            return;
        }
        List<RedisMessage> result = new LinkedList<>();
        for (Map.Entry<Versionstamp, ByteBuffer> entry : readResponse.entries().entrySet()) {
            ByteBuf value = prepareValue(request, entry);
            result.add(new SimpleStringRedisMessage(VersionstampUtil.base32HexEncode(entry.getKey())));
            result.add(new FullBulkStringRedisMessage(value));
        }
        root.add(new ArrayRedisMessage(result));
        response.writeArray(root);
    }

    /**
     * Retrieves a {@code BucketShard} object based on the specified shard ID.
     * If the shard ID is negative, the method selects the next available shard using the shard selector.
     * If the shard ID is out of the valid range or the shard is not owned by the current member,
     * a {@code KronotopException} is thrown.
     *
     * @param shardId the ID of the shard to retrieve. If negative, the next shard is selected automatically.
     * @return the {@code BucketShard} corresponding to the specified shard ID or the next shard if shard ID is negative.
     * @throws KronotopException if the shard ID is out of the valid range or the shard is not owned by the member.
     */
    protected BucketShard getOrSelectBucketShardId(int shardId) {
        if (shardId < 0) {
            return service.getShardSelector().next();
        }
        if (shardId >= service.getNumberOfShards()) {
            throw new KronotopException("Invalid shard id");
        }
        BucketShard shard = service.getShard(shardId);
        if (Objects.isNull(shard)) {
            Route route = service.findRoute(shardId);
            if (Objects.isNull(route)) {
                throw new KronotopException("No routing information found for Bucket Shard: " + shardId);
            }
            Address address = route.primary().getExternalAddress();
            throw new RedirectException(shardId, address.getHost(), address.getPort());
        }
        return shard;
    }

    protected void resp3VersionstampArrayResponse(Response response, BucketVersionstampArrayResponse deleteResponse) {
        Map<RedisMessage, RedisMessage> root = new LinkedHashMap<>();
        root.put(CURSOR_ID_MESSAGE_KEY, new IntegerRedisMessage(deleteResponse.cursorId()));
        if (deleteResponse.versionstamps() == null || deleteResponse.versionstamps().isEmpty()) {
            root.put(VERSIONSTAMPS_MESSAGE_KEY, ArrayRedisMessage.EMPTY_INSTANCE);
            response.writeMap(root);
            return;
        }
        List<RedisMessage> versionstamps = new ArrayList<>();
        for (Versionstamp versionstamp : deleteResponse.versionstamps()) {
            versionstamps.add(
                    new SimpleStringRedisMessage(VersionstampUtil.base32HexEncode(versionstamp))
            );
        }
        root.put(VERSIONSTAMPS_MESSAGE_KEY, new ArrayRedisMessage(versionstamps));
        response.writeMap(root);
    }

    protected void resp2VersionstampArrayResponse(Response response, BucketVersionstampArrayResponse deleteResponse) {
        List<RedisMessage> root = new LinkedList<>();
        root.add(new IntegerRedisMessage(deleteResponse.cursorId()));
        if (deleteResponse.versionstamps() == null || deleteResponse.versionstamps().isEmpty()) {
            root.add(ArrayRedisMessage.EMPTY_INSTANCE);
            response.writeArray(root);
            return;
        }
        List<RedisMessage> versionstamps = new LinkedList<>();
        for (Versionstamp versionstamp : deleteResponse.versionstamps()) {
            versionstamps.add(new SimpleStringRedisMessage(VersionstampUtil.base32HexEncode(versionstamp)));
        }
        root.add(new ArrayRedisMessage(versionstamps));
        response.writeArray(root);
    }

    QueryOptions buildQueryOptions(Session session, UpdateOptions updateOptions, QueryArguments arguments) {
        QueryOptions.Builder builder = QueryOptions.builder();
        if (arguments.limit() == 0) {
            builder.limit(session.attr(SessionAttributes.LIMIT).get());
        } else {
            builder.limit(arguments.limit());
        }
        builder.reverse(arguments.reverse());
        if (updateOptions != null) {
            builder.update(updateOptions);
        }
        return builder.build();
    }

    QueryContext buildQueryContext(Request request, String bucket, byte[] query, QueryArguments arguments, UpdateOptions updateOptions) {
        Session session = request.getSession();
        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, bucket);
        QueryOptions options = buildQueryOptions(session, updateOptions, arguments);
        PipelineNode plan = service.getPlanner().plan(metadata, query);
        return new QueryContext(metadata, options, plan);
    }

    QueryContext buildQueryContext(Request request, String bucket, byte[] query, QueryArguments arguments) {
        return buildQueryContext(request, bucket, query, arguments, null);
    }

    Document parseDocument(InputType inputType, byte[] data) {
        if (inputType.equals(InputType.JSON)) {
            return BSONUtil.fromJson(data);
        } else if (inputType.equals(InputType.BSON)) {
            return BSONUtil.fromBson(data);
        } else {
            throw new KronotopException("Invalid input type: " + inputType);
        }
    }

    Map<Integer, QueryContext> findQueryContext(Session session, BucketOperation subcommand) {
        return switch (subcommand) {
            case QUERY -> session.attr(SessionAttributes.BUCKET_READ_QUERY_CONTEXTS).get();
            case DELETE -> session.attr(SessionAttributes.BUCKET_DELETE_QUERY_CONTEXTS).get();
            case UPDATE -> session.attr(SessionAttributes.BUCKET_UPDATE_QUERY_CONTEXTS).get();
        };
    }

    /**
     * A commit hook implementation that executes post-commit operations for query contexts.
     * This hook is triggered after a FoundationDB transaction commits successfully and is responsible
     * for running any deferred operations that should only execute once the transaction is guaranteed
     * to be committed.
     *
     * <p>The hook integrates with the Kronotop query execution pipeline to ensure that
     * post-commit operations (such as cache invalidations, index updates, or cleanup tasks)
     * are properly executed after successful transaction commits.</p>
     *
     * <p>This is particularly important for maintaining consistency between FoundationDB state
     * and other components like the Volume storage layer or caching mechanisms.</p>
     *
     * @see CommitHook
     * @see QueryContext#runPostCommitHooks()
     */
    static class QueryContextCommitHook implements CommitHook {
        private final QueryContext ctx;

        /**
         * Constructs a new QueryContextCommitHook with the specified query context.
         *
         * @param ctx the query context containing post-commit operations to execute
         */
        QueryContextCommitHook(QueryContext ctx) {
            this.ctx = ctx;
        }

        /**
         * Executes the post-commit operations associated with the query context.
         * This method is called automatically by the Kronotop framework after
         * a FoundationDB transaction commits successfully.
         */
        @Override
        public void run() {
            ctx.runPostCommitHooks();
        }
    }
}
