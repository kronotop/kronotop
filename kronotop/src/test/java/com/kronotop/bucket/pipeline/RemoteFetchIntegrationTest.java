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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseClusterTestWithTCPServer;
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.*;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ParameterExtractor;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.index.SingleFieldIndexUtil;
import com.kronotop.bucket.optimizer.Optimizer;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PhysicalPlanner;
import com.kronotop.bucket.planner.physical.PlannerContext;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.transaction.InstrumentedTransaction;
import com.kronotop.transaction.TransactionUtil;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.bson.BsonBinaryReader;
import org.bson.BsonType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for pipeline execution with remote document fetching.
 * Tests that queries executed from a non-primary node correctly fetch documents via fetchRemote.
 */
class RemoteFetchIntegrationTest extends BaseClusterTestWithTCPServer {

    private static final String BUCKET_NAME = "test-bucket";
    private static final int SHARD_ID = 0;
    private final LogicalPlanner logicalPlanner = new LogicalPlanner();
    private final PhysicalPlanner physicalPlanner = new PhysicalPlanner();
    private final Optimizer optimizer = new Optimizer();
    private Context primaryContext;
    private EmbeddedChannel primaryChannel;

    @BeforeEach
    void setupInstances() {
        KronotopTestInstance primaryInstance = getInstances().getFirst();
        primaryContext = primaryInstance.getContext();
        primaryChannel = primaryInstance.getChannel();
    }

    private Session getSession() {
        return primaryChannel.attr(SessionAttributes.SESSION).get();
    }

    @Test
    void shouldExecuteQueryWithRemoteFetch() {
        createBucket(BUCKET_NAME);
        // Insert documents on primary node
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 35}")
        );
        insertDocuments(BUCKET_NAME, documents, primaryChannel);
        BucketMetadata metadata = openBucketMetadata(BUCKET_NAME);

        // Add a second node that will query remotely
        KronotopTestInstance secondInstance = addNewInstance(true);
        Context secondContext = secondInstance.getContext();
        BucketService secondBucketService = secondContext.getService(BucketService.NAME);

        // Verify shard is remote from the second node's perspective
        RoutingService routing = secondContext.getService(RoutingService.NAME);
        Route route = routing.findRoute(ShardKind.BUCKET, SHARD_ID);
        assertNotEquals(secondContext.getMember(), route.primary(),
                "Shard should be owned by primary, not second instance");

        // Execute the query from the second node (triggers fetchRemote)
        ReadExecutor readExecutor = createReadExecutor(secondBucketService);
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> names = new ArrayList<>();
        try (Transaction tr = new InstrumentedTransaction(secondContext.getFoundationDB().createTransaction())) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(3, results.size());
            for (ByteBuffer buffer : results) {
                names.add(extractName(buffer));
            }
        }

        assertTrue(names.contains("Alice"));
        assertTrue(names.contains("Bob"));
        assertTrue(names.contains("Charlie"));
    }

    @Test
    void shouldExecuteFilteredQueryWithRemoteFetch() {
        createBucket(BUCKET_NAME);
        // Insert documents on primary
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 35}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'age': 28}")
        );
        insertDocuments(BUCKET_NAME, documents, primaryChannel);
        BucketMetadata metadata = openBucketMetadata(BUCKET_NAME);

        // Second node queries with filter
        KronotopTestInstance secondInstance = addNewInstance(true);
        Context secondContext = secondInstance.getContext();
        BucketService secondBucketService = secondContext.getService(BucketService.NAME);

        ReadExecutor readExecutor = createReadExecutor(secondBucketService);
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$gte': 30}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> names = new ArrayList<>();
        try (Transaction tr = new InstrumentedTransaction(secondContext.getFoundationDB().createTransaction())) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            for (ByteBuffer buffer : results) {
                names.add(extractName(buffer));
            }
        }

        assertTrue(names.contains("Alice"));
        assertTrue(names.contains("Charlie"));
        assertFalse(names.contains("Bob"));
        assertFalse(names.contains("Diana"));
    }

    @Test
    void shouldExecuteIndexedQueryWithRemoteFetch() {
        createBucket(BUCKET_NAME);
        // Create an index and insert documents on primary
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", org.bson.BsonType.INT32, false, IndexStatus.WAITING);
        createIndex(BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 35}")
        );
        insertDocuments(BUCKET_NAME, documents, primaryChannel);

        // Refresh metadata to include the index
        BucketMetadata metadata = openBucketMetadata(BUCKET_NAME);

        // Second node queries using index
        KronotopTestInstance secondInstance = addNewInstance(true);
        Context secondContext = secondInstance.getContext();
        BucketService secondBucketService = secondContext.getService(BucketService.NAME);

        ReadExecutor readExecutor = createReadExecutor(secondBucketService);
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': 30}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = new InstrumentedTransaction(secondContext.getFoundationDB().createTransaction())) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(1, results.size());
            assertEquals("Alice", extractName(results.iterator().next()));
        }
    }

    @Test
    void shouldPreserveOrderWithRemoteFetch() {
        createBucket(BUCKET_NAME);
        // Insert documents on primary
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'First', 'seq': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Second', 'seq': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Third', 'seq': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Fourth', 'seq': 4}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Fifth', 'seq': 5}")
        );
        insertDocuments(BUCKET_NAME, documents, primaryChannel);
        BucketMetadata metadata = openBucketMetadata(BUCKET_NAME);

        // Second node queries
        KronotopTestInstance secondInstance = addNewInstance(true);
        Context secondContext = secondInstance.getContext();
        BucketService secondBucketService = secondContext.getService(BucketService.NAME);

        ReadExecutor readExecutor = createReadExecutor(secondBucketService);
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> names = new ArrayList<>();
        try (Transaction tr = new InstrumentedTransaction(secondContext.getFoundationDB().createTransaction())) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(5, results.size());
            for (ByteBuffer buffer : results) {
                names.add(extractName(buffer));
            }
        }

        assertEquals(List.of("First", "Second", "Third", "Fourth", "Fifth"), names);
    }

    @Test
    void shouldHandleManyDocumentsWithRemoteFetch() {
        createBucket(BUCKET_NAME);
        // Insert many documents on primary
        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{'name': 'Doc%d', 'index': %d}", i, i)
            ));
        }
        insertDocuments(BUCKET_NAME, documents, primaryChannel);
        BucketMetadata metadata = openBucketMetadata(BUCKET_NAME);

        // Second node queries
        KronotopTestInstance secondInstance = addNewInstance(true);
        Context secondContext = secondInstance.getContext();
        BucketService secondBucketService = secondContext.getService(BucketService.NAME);

        ReadExecutor readExecutor = createReadExecutor(secondBucketService);
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = new InstrumentedTransaction(secondContext.getFoundationDB().createTransaction())) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(50, results.size());

            int index = 0;
            for (ByteBuffer buffer : results) {
                assertEquals("Doc" + index, extractName(buffer));
                index++;
            }
        }
    }

    private ReadExecutor createReadExecutor(BucketService bucketService) {
        DocumentRetriever documentRetriever = new DocumentRetriever(bucketService);
        CursorManager cursorManager = new CursorManager();
        CollatorCache collatorCache = new CollatorCache();
        PipelineEnv env = new PipelineEnv(bucketService, documentRetriever, cursorManager, collatorCache);
        PipelineExecutor executor = new PipelineExecutor(env);
        return new ReadExecutor(executor);
    }

    private PlanWithParams createPlanWithParams(BucketMetadata metadata, String query) {
        PlannerContext ctx = new PlannerContext(metadata);
        BqlExpr parsedQuery = BqlParser.parse(query);
        List<BqlValue> parameters = ParameterExtractor.extract(parsedQuery);
        LogicalNode logicalPlan = logicalPlanner.planAndValidate(parsedQuery);
        PhysicalNode physicalPlan = physicalPlanner.plan(ctx, logicalPlan);
        PhysicalNode optimizedPlan = optimizer.optimize(ctx, physicalPlan);
        PipelineNode plan = PipelineRewriter.rewrite(ctx, optimizedPlan);
        return new PlanWithParams(plan, parameters);
    }

    private void createBucket(String bucketName) {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.create(bucketName).encode(buf);
        Object response = runCommand(primaryChannel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
    }

    private BucketMetadata openBucketMetadata(String bucketName) {
        String namespace = primaryContext.getConfig().getString("default_namespace");
        try (Transaction tr = primaryContext.getFoundationDB().createTransaction()) {
            return BucketMetadataUtil.open(primaryContext, tr, namespace, bucketName);
        }
    }

    private void createIndex(String bucketName, SingleFieldIndexDefinition definition) {
        String namespace = primaryContext.getConfig().getString("default_namespace");
        BucketMetadata metadata = openBucketMetadata(bucketName);

        DirectorySubspace subspace;
        try (Transaction tr = primaryContext.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(primaryContext, tr);
            subspace = SingleFieldIndexUtil.create(tx, namespace, bucketName, definition);
            tr.commit().join();
        }

        // Wait for index to be ready
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = primaryContext.getFoundationDB().createTransaction()) {
                SingleFieldIndexDefinition loaded = SingleFieldIndexUtil.loadIndexDefinition(tr, subspace);
                return loaded.status() == IndexStatus.READY;
            }
        });

        // Wait for metadata to be updated
        long targetVersion = TransactionUtil.execute(primaryContext,
                tr -> BucketMetadataUtil.readVersion(tr, metadata.subspace()));
        BucketMetadataVersionBarrier barrier = new BucketMetadataVersionBarrier(primaryContext, metadata);
        barrier.await(targetVersion, 30, Duration.ofMillis(500));
    }

    private void insertDocuments(String bucketName, List<byte[]> documents, EmbeddedChannel channel) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(bucketName, documents).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage response = (ArrayRedisMessage) msg;
        assertEquals(documents.size(), response.children().size());
    }

    private String extractName(ByteBuffer buffer) {
        buffer.rewind();
        try (BsonBinaryReader reader = new BsonBinaryReader(buffer)) {
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                String fieldName = reader.readName();
                if ("name".equals(fieldName)) {
                    return reader.readString();
                } else {
                    reader.skipValue();
                }
            }
            reader.readEndDocument();
        }
        fail("Document does not contain 'name' field");
        return null;
    }

    private record PlanWithParams(PipelineNode plan, List<BqlValue> parameters) {
    }
}
