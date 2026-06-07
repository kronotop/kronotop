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

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BaseHandlerTest;
import com.kronotop.TestUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollatorCache;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ParameterExtractor;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.transaction.InstrumentedTransaction;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonBinaryReader;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class BasePipelineTest extends BaseHandlerTest {
    protected static final int SHARD_ID = 0;

    private final CursorManager cursorManager = new CursorManager();
    protected ReadExecutor readExecutor;
    protected DeleteExecutor deleteExecutor;
    protected UpdateExecutor updateExecutor;
    protected BucketService bucketService;
    protected boolean planCacheEnabled;
    protected int planCacheMaxTtl;

    @BeforeEach
    public void setupPipelineExecutor() {
        bucketService = context.getService(BucketService.NAME);
        planCacheEnabled = context.getConfig().getBoolean("bucket.plan_cache.enabled");
        planCacheMaxTtl = context.getConfig().getInt("bucket.plan_cache.max_ttl");

        DocumentRetriever documentRetriever = new DocumentRetriever(bucketService);
        CollatorCache collatorCache = new CollatorCache();
        PipelineEnv env = new PipelineEnv(bucketService, documentRetriever, cursorManager, collatorCache);
        PipelineExecutor executor = new PipelineExecutor(env);
        readExecutor = new ReadExecutor(executor);
        deleteExecutor = new DeleteExecutor(bucketService.getContext(), executor);
        updateExecutor = new UpdateExecutor(bucketService.getContext(), executor);
    }

    protected List<ObjectId> insertDocumentsAndGetObjectIds(String bucketName, List<byte[]> documents) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(bucketName, documents).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        assertEquals(documents.size(), actualMessage.children().size(),
                String.format("Should have %d ObjectIds for %d documents", documents.size(), documents.size()));

        List<ObjectId> objectIds = new ArrayList<>();
        for (int i = 0; i < documents.size(); i++) {
            FullBulkStringRedisMessage message = (FullBulkStringRedisMessage) actualMessage.children().get(i);
            assertNotNull(message.content());
            objectIds.add(TestUtil.bulkStringToObjectId(message));
        }
        return objectIds;
    }

    protected BucketMetadata createIndexesAndLoadBucketMetadata(String bucket, SingleFieldIndexDefinition... indexes) {
        createBucket(bucket);

        // Create indexes
        for (SingleFieldIndexDefinition definition : indexes) {
            createIndexThenWaitForReadiness(TEST_NAMESPACE, bucket, definition);
        }

        // Load and return metadata
        return getBucketMetadata(bucket);
    }

    protected BucketMetadata createIndexesAndLoadBucketMetadata(
            String bucket,
            SingleFieldIndexDefinition[] singleFieldIndexes,
            CompoundIndexDefinition[] compoundIndexes) {
        createBucket(bucket);
        for (SingleFieldIndexDefinition def : singleFieldIndexes) {
            createIndexThenWaitForReadiness(TEST_NAMESPACE, bucket, def);
        }
        createIndexThenWaitForReadiness(TEST_NAMESPACE, bucket, compoundIndexes);
        return getBucketMetadata(bucket);
    }

    protected PlanWithParams createPlanWithParams(BucketMetadata metadata, @Nonnull String query) {
        return createPlanWithParams(metadata, query.getBytes(StandardCharsets.UTF_8), null);
    }

    protected PlanWithParams createPlanWithParams(BucketMetadata metadata, @Nonnull String query, String sortByField) {
        return createPlanWithParams(metadata, query.getBytes(StandardCharsets.UTF_8), sortByField);
    }

    protected PlanWithParams createPlanWithParams(BucketMetadata metadata, @Nonnull String query, String sortByField, Collation collation) {
        return createPlanWithParams(metadata, query.getBytes(StandardCharsets.UTF_8), sortByField, collation);
    }

    protected PlanWithParams createPlanWithParams(BucketMetadata metadata, @Nonnull byte[] query) {
        return createPlanWithParams(metadata, query, null);
    }

    protected PlanWithParams createPlanWithParams(BucketMetadata metadata, @Nonnull byte[] query, String sortByField) {
        BqlExpr expr = BqlParser.parse(query);
        List<BqlValue> parameters = ParameterExtractor.extract(expr);

        PipelineNode plan = bucketService.getPlanner().plan(context, metadata, expr, parameters, planCacheEnabled, planCacheMaxTtl, sortByField);
        return new PlanWithParams(plan, parameters);
    }

    protected PlanWithParams createPlanWithParams(BucketMetadata metadata, @Nonnull byte[] query, String sortByField, Collation collation) {
        BqlExpr expr = BqlParser.parse(query);
        List<BqlValue> parameters = ParameterExtractor.extract(expr);

        PipelineNode plan = bucketService.getPlanner().plan(context, metadata, expr, parameters, planCacheEnabled, planCacheMaxTtl, sortByField, collation);
        return new PlanWithParams(plan, parameters);
    }

    // Helper method to extract names from results
    Set<String> extractNamesFromResults(List<ByteBuffer> results) {
        Set<String> names = new HashSet<>();
        for (ByteBuffer documentBuffer : results) {
            documentBuffer.rewind();
            try (BsonBinaryReader reader = new BsonBinaryReader(documentBuffer)) {
                reader.readStartDocument();
                while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if ("name".equals(fieldName)) {
                        names.add(reader.readString());
                    } else {
                        reader.skipValue();
                    }
                }
                reader.readEndDocument();
            }
        }
        return names;
    }

    Set<Integer> extractIntegerFieldFromResults(List<ByteBuffer> results, String field) {
        Set<Integer> ages = new LinkedHashSet<>();

        for (ByteBuffer documentBuffer : results) {
            documentBuffer.rewind();
            try (BsonReader reader = new BsonBinaryReader(documentBuffer)) {
                reader.readStartDocument();

                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if (field.equals(fieldName)) {
                        ages.add(reader.readInt32());
                    } else {
                        reader.skipValue();
                    }
                }
                reader.readEndDocument();
            }
        }

        return ages;
    }

    List<KeyValue> fetchAllIndexedEntries(DirectorySubspace indexSubspace) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = createTransaction()) {
            return tr.getRange(begin, end).asList().join();
        }
    }

    List<KeyValue> fetchAllIndexBackPointers(DirectorySubspace indexSubspace) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        try (Transaction tr = createTransaction()) {
            return tr.getRange(begin, end).asList().join();
        }
    }

    List<String> runQueryOnBucket(BucketMetadata metadata, String query) {
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        List<String> actualResult = new ArrayList<>();
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        return actualResult;
    }

    protected Session getSession() {
        return channel.attr(SessionAttributes.SESSION).get();
    }

    protected Transaction createTransaction() {
        return new InstrumentedTransaction(context.getFoundationDB().createTransaction());
    }

    protected BucketMetadata createBucketWithCollation(String bucketName, String collationJson) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.create(bucketName, BucketCreateArgs.Builder
                .collation(collationJson)
                .shards(List.of(TEST_SHARD_ID))
                .ifNotExists()).encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        return getBucketMetadata(bucketName);
    }

    protected record PlanWithParams(PipelineNode plan, List<BqlValue> parameters) {
    }
}
