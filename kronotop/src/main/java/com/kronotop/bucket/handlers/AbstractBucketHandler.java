/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ParameterExtractor;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.handlers.protocol.BucketOperation;
import com.kronotop.bucket.handlers.protocol.QueryArguments;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.VectorIndex;
import com.kronotop.bucket.pipeline.PipelineNode;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.bucket.pipeline.QueryOptions;
import com.kronotop.bucket.pipeline.UpdateOptions;
import com.kronotop.bucket.vector.VectorGraphIndexGroup;
import com.kronotop.bucket.vector.VectorIndexNotReadyException;
import com.kronotop.cluster.Route;
import com.kronotop.network.Address;
import com.kronotop.server.*;
import com.kronotop.server.resp3.*;
import com.kronotop.transaction.TransactionUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Base class for bucket command handlers providing common functionality for query execution,
 * response formatting, and shard management.
 *
 * <p>Subclasses implement specific bucket operations (QUERY, DELETE, UPDATE, INSERT, EXPLAIN) while
 * inheriting shared utilities for RESP2/RESP3 response formatting, query context building,
 * and document parsing.</p>
 */
public abstract class AbstractBucketHandler implements Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBucketHandler.class);
    private static final byte[] CURSOR_ID_BYTES = "cursor_id".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ENTRIES_BYTES = "entries".getBytes(StandardCharsets.UTF_8);
    private static final byte[] OBJECT_IDS_BYTES = "object_ids".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SCORE_BYTES = "score".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ENTRY_BYTES = "entry".getBytes(StandardCharsets.UTF_8);

    protected final BucketService service;
    protected final Context context;
    private final boolean planCacheEnabled;
    private final int planCacheMaxTtl;

    /**
     * Constructs a handler with the given bucket service and loads plan cache configuration.
     *
     * @param service the bucket service providing shard access and query planning
     */
    public AbstractBucketHandler(BucketService service) {
        this.service = service;
        this.context = service.getContext();

        planCacheEnabled = this.context.getConfig().getBoolean("bucket.plan_cache.enabled");
        planCacheMaxTtl = this.context.getConfig().getInt("bucket.plan_cache.max_ttl");
    }

    protected static FullBulkStringRedisMessage bulkString(String input) {
        return new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Bucket handlers are not Redis-compatible as they use Kronotop-specific protocol extensions.
     */
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
     *
     * @param request  the {@code Request} object containing the session information
     * @param document a {@code ByteBuffer} containing the document data
     * @return a {@code ByteBuf} object containing the serialized data in the format specified by the reply type
     */
    protected ByteBuf prepareValue(Request request, ByteBuffer document) {
        ReplyType replyType = getReplyType(request);

        return switch (replyType) {
            case ReplyType.BSON -> Unpooled.wrappedBuffer(document.slice());
            case ReplyType.JSON -> {
                byte[] bytes = new byte[document.remaining()];
                document.get(bytes);
                document.rewind();
                BsonDocument bsonDocument = BSONUtil.toBsonDocument(bytes);
                byte[] data = BSONUtil.toJson(bsonDocument).getBytes(StandardCharsets.UTF_8);
                yield Unpooled.wrappedBuffer(data);
            }
        };
    }

    protected RedisMessage encodeObjectId(ObjectIdFormat format, ObjectId objectId) {
        return switch (format) {
            case BYTES -> new FullBulkStringRedisMessage(
                    Unpooled.wrappedBuffer(objectId.toByteArray())
            );
            case HEX -> new FullBulkStringRedisMessage(
                    Unpooled.copiedBuffer(objectId.toHexString(), StandardCharsets.US_ASCII)
            );
        };
    }

    /**
     * Processes and writes a RESP3-compliant response to the client based on the provided {@code ReadResponse}.
     *
     * @param request      the {@code Request} object containing session and command-related information
     * @param response     the {@code Response} object used to send the processed results back to the client
     * @param readResponse the {@code BucketEntriesMapResponse} containing the cursor ID and entries
     */
    protected void resp3Response(Request request, Response response, BucketEntriesMapResponse readResponse) {
        Map<RedisMessage, RedisMessage> root = new LinkedHashMap<>();
        root.put(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(CURSOR_ID_BYTES)), new IntegerRedisMessage(readResponse.cursorId()));
        if (readResponse.entries() == null || readResponse.entries().isEmpty()) {
            root.put(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(ENTRIES_BYTES)), ArrayRedisMessage.EMPTY_INSTANCE);
            response.writeMap(root);
            return;
        }
        List<RedisMessage> entries = new ArrayList<>();
        for (ByteBuffer document : readResponse.entries()) {
            ByteBuf value = prepareValue(request, document);
            entries.add(
                    new FullBulkStringRedisMessage(value)
            );
        }
        root.put(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(ENTRIES_BYTES)), new ArrayRedisMessage(entries));
        response.writeMap(root);
    }

    /**
     * Processes and writes a RESP2-compliant response to the client based on the given {@code ReadResponse}.
     *
     * @param request      the {@code Request} object containing the session and command information
     * @param response     the {@code Response} object used to send the result back to the client
     * @param readResponse the {@code BucketEntriesMapResponse} containing the cursor ID and entries
     */
    protected void resp2Response(Request request, Response response, BucketEntriesMapResponse readResponse) {
        List<RedisMessage> root = new ArrayList<>();
        root.add(new IntegerRedisMessage(readResponse.cursorId()));
        if (readResponse.entries() == null || readResponse.entries().isEmpty()) {
            root.add(ArrayRedisMessage.EMPTY_INSTANCE);
            response.writeArray(root);
            return;
        }
        List<RedisMessage> result = new ArrayList<>();
        for (ByteBuffer document : readResponse.entries()) {
            ByteBuf value = prepareValue(request, document);
            result.add(new FullBulkStringRedisMessage(value));
        }
        root.add(new ArrayRedisMessage(result));
        response.writeArray(root);
    }

    /**
     * Writes a RESP3 map response containing cursor ID and ObjectId array.
     * Used for DELETE and UPDATE operations that return affected document identifiers.
     *
     * @param response         the response writer
     * @param format           the ObjectId encoding format (BYTES or HEX)
     * @param objectIdResponse the result containing cursor ID and affected ObjectIds
     */
    protected void resp3ObjectIdArrayResponse(Response response, ObjectIdFormat format, BucketObjectIdArrayResponse objectIdResponse) {
        Map<RedisMessage, RedisMessage> root = new LinkedHashMap<>();
        root.put(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(CURSOR_ID_BYTES)), new IntegerRedisMessage(objectIdResponse.cursorId()));
        if (objectIdResponse.objectIds() == null || objectIdResponse.objectIds().isEmpty()) {
            root.put(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(OBJECT_IDS_BYTES)), ArrayRedisMessage.EMPTY_INSTANCE);
            response.writeMap(root);
            return;
        }
        List<RedisMessage> objectIds = new ArrayList<>();
        for (ObjectId objectId : objectIdResponse.objectIds()) {
            objectIds.add(encodeObjectId(format, objectId));
        }
        root.put(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(OBJECT_IDS_BYTES)), new ArrayRedisMessage(objectIds));
        response.writeMap(root);
    }

    /**
     * Writes a RESP2 array response containing cursor ID and ObjectId array.
     * Used for DELETE and UPDATE operations with RESP2 clients.
     *
     * @param response         the response writer
     * @param format           the ObjectId encoding format (BYTES or HEX)
     * @param objectIdResponse the result containing cursor ID and affected ObjectIds
     */
    protected void resp2ObjectIdArrayResponse(Response response, ObjectIdFormat format, BucketObjectIdArrayResponse objectIdResponse) {
        List<RedisMessage> root = new ArrayList<>();
        root.add(new IntegerRedisMessage(objectIdResponse.cursorId()));
        if (objectIdResponse.objectIds() == null || objectIdResponse.objectIds().isEmpty()) {
            root.add(ArrayRedisMessage.EMPTY_INSTANCE);
            response.writeArray(root);
            return;
        }
        List<RedisMessage> objectIds = new ArrayList<>();
        for (ObjectId objectId : objectIdResponse.objectIds()) {
            objectIds.add(encodeObjectId(format, objectId));
        }
        root.add(new ArrayRedisMessage(objectIds));
        response.writeArray(root);
    }

    /**
     * Writes a RESP3 response for vector search: array of maps, each with score and document.
     *
     * @param request  the request for reply type conversion
     * @param response the response writer
     * @param results  vector search results with scores and documents
     */
    protected void resp3VectorResponse(Request request, Response response, List<VectorSearchResult> results) {
        if (results.isEmpty()) {
            response.writeArray(List.of());
            return;
        }
        List<RedisMessage> array = new ArrayList<>(results.size());
        for (VectorSearchResult result : results) {
            Map<RedisMessage, RedisMessage> entry = new LinkedHashMap<>();
            entry.put(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(SCORE_BYTES)),
                    new DoubleRedisMessage(result.score()));
            entry.put(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(ENTRY_BYTES)),
                    new FullBulkStringRedisMessage(prepareValue(request, result.document())));
            array.add(new MapRedisMessage(entry));
        }
        response.writeArray(array);
    }

    /**
     * Writes a RESP2 response for vector search: array of [score, document] pairs.
     *
     * @param request  the request for reply type conversion
     * @param response the response writer
     * @param results  vector search results with scores and documents
     */
    protected void resp2VectorResponse(Request request, Response response, List<VectorSearchResult> results) {
        if (results.isEmpty()) {
            response.writeArray(List.of());
            return;
        }
        List<RedisMessage> array = new ArrayList<>(results.size());
        for (VectorSearchResult result : results) {
            List<RedisMessage> pair = new ArrayList<>(2);
            pair.add(bulkString(String.valueOf(result.score())));
            pair.add(new FullBulkStringRedisMessage(prepareValue(request, result.document())));
            array.add(new ArrayRedisMessage(pair));
        }
        response.writeArray(array);
    }

    /**
     * Applies field-level projection to query results if a projection spec is present in the context.
     * <p>
     * This method deserializes each ByteBuffer into a BsonDocument, applies the projection, and serializes back.
     * A BsonBinaryReader-based approach that operates directly on raw bytes was considered but rejected:
     * projection runs on already-limited result sets, the real bottleneck is FDB/Volume I/O, and the Projector
     * logic (nested fields, array traversal, positional operator) would require a complex state machine
     * for marginal gain.
     *
     * @param entries the raw document entries from the query executor
     * @param ctx     the query context potentially containing a projection spec
     * @return projected entries, or the original entries if no projection is specified
     */
    protected List<ByteBuffer> applyProjection(List<ByteBuffer> entries, QueryContext ctx) {
        if (ctx.getProjectionSpec() == null || entries == null || entries.isEmpty()) {
            return entries;
        }

        List<BsonDocument> documents = new ArrayList<>(entries.size());
        for (ByteBuffer entry : entries) {
            documents.add(BSONUtil.fromBson(entry.slice()));
        }

        List<BsonDocument> projected = Projector.project(documents, ctx.getProjectionSpec(), ctx.getParsedQuery());

        List<ByteBuffer> result = new ArrayList<>(projected.size());
        for (BsonDocument doc : projected) {
            result.add(BSONUtil.toByteBuffer(doc));
        }
        return result;
    }

    /**
     * Applies field-level projection to vector search results.
     * <p>
     * Same ByteBuffer-to-BsonDocument round-trip rationale as {@link #applyProjection}:
     * the result set is already bounded, and a raw-bytes approach would add significant
     * complexity for negligible performance improvement.
     */
    protected List<VectorSearchResult> applyVectorProjection(
            List<VectorSearchResult> results,
            BsonDocument projectionSpec,
            BqlExpr parsedQuery) {
        if (projectionSpec == null || results == null || results.isEmpty()) {
            return results;
        }

        List<BsonDocument> documents = new ArrayList<>(results.size());
        for (VectorSearchResult result : results) {
            documents.add(BSONUtil.fromBson(result.document().slice()));
        }

        List<BsonDocument> projected = Projector.project(documents, projectionSpec, parsedQuery);

        List<VectorSearchResult> projectedResults = new ArrayList<>(results.size());
        for (int i = 0; i < results.size(); i++) {
            projectedResults.add(new VectorSearchResult(results.get(i).score(), BSONUtil.toByteBuffer(projected.get(i))));
        }
        return projectedResults;
    }

    /**
     * Builds query options from session defaults and command arguments.
     *
     * @param session       the client session containing default limit
     * @param updateOptions optional update configuration for UPDATE operations
     * @param arguments     parsed command arguments containing limit and sort options
     * @return configured query options
     */
    QueryOptions buildQueryOptions(Session session, UpdateOptions updateOptions, QueryArguments arguments) {
        QueryOptions.Builder builder = QueryOptions.builder();
        if (arguments.getLimit() == 0) {
            builder.limit(session.attr(SessionAttributes.LIMIT).get());
        } else {
            builder.limit(arguments.getLimit());
        }
        if (arguments.getSortBy() != null) {
            builder.sortByField(arguments.getSortBy());
            builder.sortDirection(arguments.getSortDirection());
        }
        if (arguments.getResultSortBy() != null) {
            if (arguments.getResultSortBy().equals(arguments.getSortBy())) {
                throw new KronotopException("SORTBY and RESULTSORT cannot use the same field: '" + arguments.getSortBy() + "'");
            }
            builder.resultSortField(arguments.getResultSortBy());
            builder.resultSortDirection(arguments.getResultSortDirection());
        }
        if (updateOptions != null) {
            builder.update(updateOptions);
        }
        if (arguments.getCollation() != null) {
            builder.collation(arguments.getCollation());
        }
        return builder.build();
    }

    /**
     * Returns the first shard from the bucket's shard list that is owned by this cluster member.
     * If no local shard exists, redirects the client to the primary owner of the first shard.
     *
     * @param metadata the bucket metadata containing the shard list
     * @return the local shard
     * @throws KronotopException if the bucket has no shards assigned, or no route is found
     * @throws RejectException   if none of the bucket's shards are local
     */
    BucketShard findLocalShard(BucketMetadata metadata) {
        // Potential optimization: cache the selected shardId in BucketMetadata cache or somewhere else.
        if (metadata.shards() == null || metadata.shards().isEmpty()) {
            throw new KronotopException("No shards assigned to bucket: '" + metadata.name() + "'");
        }
        for (int shardId : metadata.shards()) {
            BucketShard shard = service.getShard(shardId);
            if (shard == null) {
                continue;
            }
            return shard;
        }

        int shardId = metadata.shards().getFirst();
        Route route = service.findRoute(shardId);
        if (Objects.isNull(route)) {
            throw new KronotopException("No route found for Bucket shard: " + shardId);
        }
        Address address = route.primary().getExternalAddress();
        throw new RejectException(shardId, address.getHost(), address.getPort());
    }

    /**
     * Validates that the current node owns at least one shard assigned to the given bucket.
     * Rejects the request with a redirect if the bucket's shards are all remote.
     *
     * @param metadata the bucket metadata containing the shard list
     * @throws KronotopException if the bucket has no shards assigned, or no route is found
     * @throws RejectException   if none of the bucket's shards are local
     */
    void checkBucketOwnership(BucketMetadata metadata) {
        findLocalShard(metadata);
    }

    /**
     * Creates a query context by parsing the BQL query, building an execution plan, and
     * assembling query options from the request arguments.
     *
     * @param request          the client request
     * @param metadata         the bucket metadata
     * @param query            the BQL query as bytes
     * @param arguments        parsed command arguments
     * @param updateOptions    optional update configuration
     * @param disablePlanCache whether to bypass plan cache
     * @return initialized query context ready for execution
     */
    QueryContext buildQueryContext(Request request, BucketMetadata metadata, @Nonnull byte[] query, QueryArguments arguments, UpdateOptions updateOptions, boolean disablePlanCache) {
        Session session = request.getSession();
        QueryOptions options = buildQueryOptions(session, updateOptions, arguments);

        BqlExpr expr = BqlParser.parse(query);
        List<BqlValue> parameters = ParameterExtractor.extract(expr);

        boolean usePlanCache = !disablePlanCache && planCacheEnabled;
        PipelineNode plan = service.getPlanner().plan(context, metadata, expr, parameters, usePlanCache, planCacheMaxTtl, arguments.getSortBy(), arguments.getCollation());
        QueryContext ctx = new QueryContext(session, metadata, options, plan, parameters);

        ctx.setQueryBytes(query);
        ctx.setUserVersionSupplier(() -> TransactionUtil.getUserVersion(session));

        if (arguments.getProjection() != null) {
            ctx.setProjectionSpec(BSONUtil.fromJson(arguments.getProjection()));
            ctx.setParsedQuery(expr);
        }

        return ctx;
    }

    /**
     * Creates a query context with plan caching enabled and no update options.
     */
    QueryContext buildQueryContext(Request request, BucketMetadata metadata, byte[] query, QueryArguments arguments) {
        return buildQueryContext(request, metadata, query, arguments, null, false);
    }

    /**
     * Creates a query context with configurable plan caching and no update options.
     */
    QueryContext buildQueryContext(Request request, BucketMetadata metadata, byte[] query, QueryArguments arguments, boolean disablePlanCache) {
        return buildQueryContext(request, metadata, query, arguments, null, disablePlanCache);
    }

    /**
     * Retrieves the query context map for cursor-based pagination from the session.
     *
     * @param session    the client session
     * @param subcommand the operation type (QUERY, DELETE, or UPDATE)
     * @return map of cursor IDs to query contexts
     */
    Map<Integer, QueryContext> findQueryContext(Session session, BucketOperation subcommand) {
        return switch (subcommand) {
            case QUERY -> session.attr(SessionAttributes.BUCKET_READ_QUERY_CONTEXTS).get();
            case DELETE -> session.attr(SessionAttributes.BUCKET_DELETE_QUERY_CONTEXTS).get();
            case UPDATE -> session.attr(SessionAttributes.BUCKET_UPDATE_QUERY_CONTEXTS).get();
        };
    }

    /**
     * Ensures all vector indexes on the bucket are bootstrapped and ready before a write operation proceeds.
     * Triggers bootstrap via computeIfAbsent if the group has not been created yet.
     *
     * @throws VectorIndexNotReadyException if any vector index is still bootstrapping
     */
    protected void checkVectorIndexRecoveryState(BucketMetadata metadata) {
        String namespace = metadata.namespace();
        String bucket = metadata.name();
        for (VectorIndex vectorIndex : metadata.vectorIndexes().getIndexes(IndexSelectionPolicy.READWRITE)) {
            long indexId = vectorIndex.definition().id();
            VectorGraphIndexGroup group = service.getVectorGraphRegistry().computeIfAbsent(
                    metadata,
                    vectorIndex,
                    () -> service.bootstrapVectorGroup(metadata, vectorIndex)
            );
            if (!group.isReady()) {
                throw new VectorIndexNotReadyException(namespace, bucket, indexId);
            }
        }
    }
}
