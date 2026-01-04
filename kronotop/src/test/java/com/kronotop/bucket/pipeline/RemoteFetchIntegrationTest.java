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

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseClusterTestWithTCPServer;
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.*;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.bucket.optimizer.Optimizer;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PhysicalPlanner;
import com.kronotop.bucket.planner.physical.PlannerContext;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RouteKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketInsertArgs;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.MockChannelHandlerContext;
import com.kronotop.server.Session;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.bson.BsonBinaryReader;
import org.bson.BsonType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    private KronotopTestInstance primaryInstance;
    private Context primaryContext;
    private EmbeddedChannel primaryChannel;

    @BeforeEach
    void setupInstances() {
        primaryInstance = getInstances().getFirst();
        primaryContext = primaryInstance.getContext();
        primaryChannel = primaryInstance.getChannel();
    }

    @Test
    void shouldExecuteQueryWithRemoteFetch() {
        // Insert documents on primary node
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 35}")
        );
        insertDocuments(BUCKET_NAME, documents);
        BucketMetadata metadata = createBucket(BUCKET_NAME);

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
        PipelineNode plan = createExecutionPlan(metadata, "{}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        List<String> names = new ArrayList<>();
        try (Transaction tr = secondContext.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(3, results.size());
            for (ByteBuffer buffer : results.values()) {
                names.add(extractName(buffer));
            }
        }

        assertTrue(names.contains("Alice"));
        assertTrue(names.contains("Bob"));
        assertTrue(names.contains("Charlie"));
    }

    @Test
    void shouldExecuteFilteredQueryWithRemoteFetch() {
        // Insert documents on primary
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 35}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'age': 28}")
        );
        insertDocuments(BUCKET_NAME, documents);
        BucketMetadata metadata = createBucket(BUCKET_NAME);

        // Second node queries with filter
        KronotopTestInstance secondInstance = addNewInstance(true);
        Context secondContext = secondInstance.getContext();
        BucketService secondBucketService = secondContext.getService(BucketService.NAME);

        ReadExecutor readExecutor = createReadExecutor(secondBucketService);
        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gte': 30}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        List<String> names = new ArrayList<>();
        try (Transaction tr = secondContext.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            for (ByteBuffer buffer : results.values()) {
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
        // Create an index and insert documents on primary
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", org.bson.BsonType.INT32);
        createIndex(BUCKET_NAME, ageIndex);
        BucketMetadata metadata = createBucket(BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 35}")
        );
        insertDocuments(BUCKET_NAME, documents);

        // Refresh metadata to include the index
        metadata = createBucket(BUCKET_NAME);

        // Second node queries using index
        KronotopTestInstance secondInstance = addNewInstance(true);
        Context secondContext = secondInstance.getContext();
        BucketService secondBucketService = secondContext.getService(BucketService.NAME);

        ReadExecutor readExecutor = createReadExecutor(secondBucketService);
        PipelineNode plan = createExecutionPlan(metadata, "{'age': 30}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = secondContext.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(1, results.size());
            assertEquals("Alice", extractName(results.values().iterator().next()));
        }
    }

    @Test
    void shouldPreserveOrderWithRemoteFetch() {
        // Insert documents on primary
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'First', 'seq': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Second', 'seq': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Third', 'seq': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Fourth', 'seq': 4}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Fifth', 'seq': 5}")
        );
        insertDocuments(BUCKET_NAME, documents);
        BucketMetadata metadata = createBucket(BUCKET_NAME);

        // Second node queries
        KronotopTestInstance secondInstance = addNewInstance(true);
        Context secondContext = secondInstance.getContext();
        BucketService secondBucketService = secondContext.getService(BucketService.NAME);

        ReadExecutor readExecutor = createReadExecutor(secondBucketService);
        PipelineNode plan = createExecutionPlan(metadata, "{}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        List<String> names = new ArrayList<>();
        try (Transaction tr = secondContext.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(5, results.size());
            for (ByteBuffer buffer : results.values()) {
                names.add(extractName(buffer));
            }
        }

        assertEquals(List.of("First", "Second", "Third", "Fourth", "Fifth"), names);
    }

    @Test
    void shouldHandleManyDocumentsWithRemoteFetch() {
        // Insert many documents on primary
        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{'name': 'Doc%d', 'index': %d}", i, i)
            ));
        }
        insertDocuments(BUCKET_NAME, documents);
        BucketMetadata metadata = createBucket(BUCKET_NAME);

        // Second node queries
        KronotopTestInstance secondInstance = addNewInstance(true);
        Context secondContext = secondInstance.getContext();
        BucketService secondBucketService = secondContext.getService(BucketService.NAME);

        ReadExecutor readExecutor = createReadExecutor(secondBucketService);
        PipelineNode plan = createExecutionPlan(metadata, "{}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = secondContext.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(50, results.size());

            int index = 0;
            for (ByteBuffer buffer : results.values()) {
                assertEquals("Doc" + index, extractName(buffer));
                index++;
            }
        }
    }

    @Test
    @Disabled
    void shouldFetchDocumentsFromAllShardsWithDistributedOwnership() {
        int numberOfShards = primaryContext.getConfig().getInt("bucket.shards");

        // Add second instance
        KronotopTestInstance secondInstance = addNewInstance(true);
        Context secondContext = secondInstance.getContext();

        // Reassign half the shards to the second instance
        int midpoint = numberOfShards / 2;
        for (int shardId = midpoint; shardId < numberOfShards; shardId++) {
            setShardStatus(shardId, "READONLY");
            reassignShardPrimary(shardId, secondInstance.getMember().getId());
            setShardStatus(shardId, "READWRITE");
        }

        // Wait for routing to converge on both instances
        waitForShardOwnership(primaryContext, midpoint, numberOfShards, secondInstance.getMember());
        waitForShardOwnership(secondContext, midpoint, numberOfShards, secondInstance.getMember());

        BucketMetadata metadata = createBucket(BUCKET_NAME);

        // Insert documents to all shards
        int docsPerShard = 3;
        int totalDocs = 0;
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            List<byte[]> documents = new ArrayList<>();
            for (int i = 0; i < docsPerShard; i++) {
                documents.add(BSONUtil.jsonToDocumentThenBytes(
                        String.format("{'name': 'Shard%d_Doc%d', 'shardId': %d, 'docIndex': %d}", shardId, i, shardId, i)
                ));
            }
            // Insert to appropriate instance based on shard ownership
            if (shardId < midpoint) {
                insertDocumentsToShard(BUCKET_NAME, shardId, documents, primaryChannel);
            } else {
                insertDocumentsToShard(BUCKET_NAME, shardId, documents, secondInstance.getChannel());
            }
            totalDocs += documents.size();
        }

        // Verify shard distribution
        RoutingService routing = primaryContext.getService(RoutingService.NAME);
        for (int shardId = 0; shardId < midpoint; shardId++) {
            Route route = routing.findRoute(ShardKind.BUCKET, shardId);
            assertEquals(primaryContext.getMember(), route.primary(),
                    "Shard " + shardId + " should be owned by first instance");
        }
        for (int shardId = midpoint; shardId < numberOfShards; shardId++) {
            Route route = routing.findRoute(ShardKind.BUCKET, shardId);
            assertEquals(secondContext.getMember(), route.primary(),
                    "Shard " + shardId + " should be owned by second instance");
        }

        // Execute empty query {} from first instance - should fetch some docs locally, some remotely
        BucketService primaryBucketService = primaryContext.getService(BucketService.NAME);
        ReadExecutor readExecutor = createReadExecutor(primaryBucketService);
        PipelineNode plan = createExecutionPlan(metadata, "{}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = primaryContext.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(totalDocs, results.size(), "Should fetch all documents from all shards");

            // Verify we got documents from each shard
            int[] docsFromShard = new int[numberOfShards];
            for (ByteBuffer buffer : results.values()) {
                int shardId = extractShardId(buffer);
                docsFromShard[shardId]++;
            }

            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                assertEquals(docsPerShard, docsFromShard[shardId],
                        "Should have " + docsPerShard + " documents from shard " + shardId);
            }
        }
    }

    private void setShardStatus(int shardId, String status) {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus(ShardKind.BUCKET.name(), shardId, status).encode(buf);

        Object raw = runCommand(primaryChannel, buf);
        if (raw instanceof ErrorRedisMessage message) {
            fail("Failed to set shard " + shardId + " status to " + status + ": " + message.content());
        }
        assertInstanceOf(SimpleStringRedisMessage.class, raw);
    }

    private void reassignShardPrimary(int shardId, String memberId) {
        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", RouteKind.PRIMARY.name(), ShardKind.BUCKET.name(), shardId, memberId).encode(buf);

        Object raw = runCommand(primaryChannel, buf);
        if (raw instanceof ErrorRedisMessage message) {
            fail("Failed to reassign shard " + shardId + ": " + message.content());
        }
        assertInstanceOf(SimpleStringRedisMessage.class, raw);
    }

    private void waitForShardOwnership(Context context, int fromShard, int toShard, Member expectedOwner) {
        RoutingService routing = context.getService(RoutingService.NAME);
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            for (int shardId = fromShard; shardId < toShard; shardId++) {
                Route route = routing.findRoute(ShardKind.BUCKET, shardId);
                if (route == null || !route.primary().equals(expectedOwner)) {
                    return false;
                }
            }
            return true;
        });
    }

    private ReadExecutor createReadExecutor(BucketService bucketService) {
        DocumentRetriever documentRetriever = new DocumentRetriever(bucketService);
        CursorManager cursorManager = new CursorManager();
        PipelineEnv env = new PipelineEnv(bucketService, documentRetriever, cursorManager);
        PipelineExecutor executor = new PipelineExecutor(env);
        return new ReadExecutor(executor);
    }

    private PipelineNode createExecutionPlan(BucketMetadata metadata, String query) {
        PlannerContext ctx = new PlannerContext(metadata);
        BqlExpr parsedQuery = BqlParser.parse(query);
        LogicalNode logicalPlan = logicalPlanner.planAndValidate(parsedQuery);
        PhysicalNode physicalPlan = physicalPlanner.plan(metadata, logicalPlan, ctx);
        PhysicalNode optimizedPlan = optimizer.optimize(metadata, physicalPlan, ctx);
        return PipelineRewriter.rewrite(ctx, optimizedPlan);
    }

    private Session getSession(KronotopTestInstance instance) {
        MockChannelHandlerContext ctx = new MockChannelHandlerContext(instance.getChannel());
        Session.registerSession(instance.getContext(), ctx);
        return Session.extractSessionFromChannel(ctx.channel());
    }

    private BucketMetadata createBucket(String bucketName) {
        Session session = getSession(primaryInstance);
        return BucketMetadataUtil.createOrOpen(primaryContext, session, bucketName);
    }

    private void createIndex(String bucketName, IndexDefinition definition) {
        String namespace = primaryContext.getConfig().getString("default_namespace");
        Session session = getSession(primaryInstance);
        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(primaryContext, session, bucketName);

        DirectorySubspace subspace;
        try (Transaction tr = primaryContext.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(primaryContext, tr);
            subspace = IndexUtil.create(tx, namespace, bucketName, definition);
            tr.commit().join();
        }

        // Wait for index to be ready
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            try (Transaction tr = primaryContext.getFoundationDB().createTransaction()) {
                IndexDefinition loaded = IndexUtil.loadIndexDefinition(tr, subspace);
                return loaded.status() == IndexStatus.READY;
            }
        });

        // Wait for metadata to be updated
        long targetVersion = TransactionUtils.execute(primaryContext,
                tr -> BucketMetadataUtil.readVersion(tr, metadata.subspace()));
        BucketMetadataVersionBarrier barrier = new BucketMetadataVersionBarrier(primaryContext, metadata);
        barrier.await(targetVersion, 30, Duration.ofMillis(500));
    }

    private void insertDocuments(String bucketName, List<byte[]> documents) {
        insertDocumentsToShard(bucketName, SHARD_ID, documents);
    }

    private void insertDocumentsToShard(String bucketName, int shardId, List<byte[]> documents) {
        insertDocumentsToShard(bucketName, shardId, documents, primaryChannel);
    }

    private void insertDocumentsToShard(String bucketName, int shardId, List<byte[]> documents, EmbeddedChannel channel) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(bucketName, BucketInsertArgs.Builder.shard(shardId), documents).encode(buf);

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

    private int extractShardId(ByteBuffer buffer) {
        buffer.rewind();
        try (BsonBinaryReader reader = new BsonBinaryReader(buffer)) {
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                String fieldName = reader.readName();
                if ("shardId".equals(fieldName)) {
                    return reader.readInt32();
                } else {
                    reader.skipValue();
                }
            }
            reader.readEndDocument();
        }
        fail("Document does not contain 'shardId' field");
        return -1;
    }
}
