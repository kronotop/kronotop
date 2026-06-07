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
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BaseClusterTestWithTCPServer;
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.TestUtil;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.*;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.EntryMetadata;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.bson.BsonBinaryReader;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DocumentRetrieverTest extends BaseClusterTestWithTCPServer {

    private static final int SHARD_ID = 0;
    private static final String TEST_BUCKET = "test-bucket";

    private Context primaryContext;
    private EmbeddedChannel primaryChannel;
    private BucketService primaryBucketService;

    @BeforeEach
    void setUp() {
        KronotopTestInstance primaryInstance = getInstances().getFirst();
        primaryContext = primaryInstance.getContext();
        primaryChannel = primaryInstance.getChannel();
        primaryBucketService = primaryContext.getService(BucketService.NAME);
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldReturnEmptyListForEmptyLocations() {
        DocumentRetriever retriever = new DocumentRetriever(primaryBucketService);

        List<ByteBuffer> results = retriever.retrieveDocuments(List.of());

        assertTrue(results.isEmpty());
    }

    @Test
    void shouldRetrieveSingleDocumentLocally() {
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}")
        );
        List<ObjectId> objectIds = insertDocuments(TEST_BUCKET, documents);
        BucketMetadata metadata = openBucketMetadata(TEST_BUCKET);

        List<DocumentLocation> locations = buildDocumentLocations(metadata, objectIds);
        DocumentRetriever retriever = new DocumentRetriever(primaryBucketService);

        List<ByteBuffer> results = retriever.retrieveDocuments(locations);

        assertEquals(1, results.size());
        assertDocumentHasName(results.getFirst(), "Alice");
    }

    @Test
    void shouldRetrieveMultipleDocumentsLocally() {
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 35}")
        );
        List<ObjectId> objectIds = insertDocuments(TEST_BUCKET, documents);
        BucketMetadata metadata = openBucketMetadata(TEST_BUCKET);

        List<DocumentLocation> locations = buildDocumentLocations(metadata, objectIds);
        DocumentRetriever retriever = new DocumentRetriever(primaryBucketService);

        List<ByteBuffer> results = retriever.retrieveDocuments(locations);

        assertEquals(3, results.size());
        assertDocumentHasName(results.get(0), "Alice");
        assertDocumentHasName(results.get(1), "Bob");
        assertDocumentHasName(results.get(2), "Charlie");
    }

    @Test
    void shouldPreserveDocumentOrder() {
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'First'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Second'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Third'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Fourth'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Fifth'}")
        );
        List<ObjectId> objectIds = insertDocuments(TEST_BUCKET, documents);
        BucketMetadata metadata = openBucketMetadata(TEST_BUCKET);

        List<DocumentLocation> locations = buildDocumentLocations(metadata, objectIds);

        // Reverse order to test preservation
        List<DocumentLocation> reversed = new ArrayList<>(locations);
        java.util.Collections.reverse(reversed);

        DocumentRetriever retriever = new DocumentRetriever(primaryBucketService);
        List<ByteBuffer> results = retriever.retrieveDocuments(reversed);

        assertEquals(5, results.size());
        assertDocumentHasName(results.get(0), "Fifth");
        assertDocumentHasName(results.get(1), "Fourth");
        assertDocumentHasName(results.get(2), "Third");
        assertDocumentHasName(results.get(3), "Second");
        assertDocumentHasName(results.get(4), "First");
    }

    @Test
    void shouldRetrieveManyDocumentsExceedingBatchSize() {
        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{'name': 'Doc%d', 'index': %d}", i, i)
            ));
        }
        List<ObjectId> objectIds = insertDocuments(TEST_BUCKET, documents);
        BucketMetadata metadata = openBucketMetadata(TEST_BUCKET);

        List<DocumentLocation> locations = buildDocumentLocations(metadata, objectIds);
        DocumentRetriever retriever = new DocumentRetriever(primaryBucketService);

        List<ByteBuffer> results = retriever.retrieveDocuments(locations);

        assertEquals(20, results.size());
        for (int i = 0; i < 20; i++) {
            assertDocumentHasName(results.get(i), "Doc" + i);
        }
    }

    @Test
    void shouldRetrieveDocumentsRemotely() {
        // Insert documents on primary node
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'RemoteDoc1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'RemoteDoc2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'RemoteDoc3'}")
        );
        List<ObjectId> objectIds = insertDocuments(TEST_BUCKET, documents);
        BucketMetadata metadata = openBucketMetadata(TEST_BUCKET);

        // Add a second node that will query remotely
        KronotopTestInstance secondInstance = addNewInstance(true);
        Context secondContext = secondInstance.getContext();
        BucketService secondBucketService = secondContext.getService(BucketService.NAME);

        // Verify that from the second node's perspective, the shard is remote
        RoutingService routing = secondContext.getService(RoutingService.NAME);
        Route route = routing.findRoute(ShardKind.BUCKET, SHARD_ID);
        assertNotEquals(secondContext.getMember(), route.primary(),
                "Shard should be owned by primary, not second instance");

        // Build locations (using primary's metadata)
        List<DocumentLocation> locations = buildDocumentLocations(metadata, objectIds);

        // Retrieve from the second node (should use fetchRemote)
        DocumentRetriever retriever = new DocumentRetriever(secondBucketService);
        List<ByteBuffer> results = retriever.retrieveDocuments(locations);

        assertEquals(3, results.size());
        assertDocumentHasName(results.get(0), "RemoteDoc1");
        assertDocumentHasName(results.get(1), "RemoteDoc2");
        assertDocumentHasName(results.get(2), "RemoteDoc3");
    }

    @Test
    void shouldRetrieveManyDocumentsRemotely() {
        // Insert many documents on primary
        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{'name': 'Remote%d'}", i)
            ));
        }
        List<ObjectId> objectIds = insertDocuments(TEST_BUCKET, documents);
        BucketMetadata metadata = openBucketMetadata(TEST_BUCKET);

        // The second node retrieves remotely
        KronotopTestInstance secondInstance = addNewInstance(true);
        BucketService secondBucketService = secondInstance.getContext().getService(BucketService.NAME);

        List<DocumentLocation> locations = buildDocumentLocations(metadata, objectIds);
        DocumentRetriever retriever = new DocumentRetriever(secondBucketService);

        List<ByteBuffer> results = retriever.retrieveDocuments(locations);

        assertEquals(15, results.size());
        for (int i = 0; i < 15; i++) {
            assertDocumentHasName(results.get(i), "Remote" + i);
        }
    }

    @Test
    void shouldPreserveOrderWhenRetrievingRemotely() {
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'A'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'D'}")
        );
        List<ObjectId> objectIds = insertDocuments(TEST_BUCKET, documents);
        BucketMetadata metadata = openBucketMetadata(TEST_BUCKET);

        KronotopTestInstance secondInstance = addNewInstance(true);
        BucketService secondBucketService = secondInstance.getContext().getService(BucketService.NAME);

        List<DocumentLocation> locations = buildDocumentLocations(metadata, objectIds);

        // Reverse order
        List<DocumentLocation> reversed = new ArrayList<>(locations);
        java.util.Collections.reverse(reversed);

        DocumentRetriever retriever = new DocumentRetriever(secondBucketService);
        List<ByteBuffer> results = retriever.retrieveDocuments(reversed);

        assertEquals(4, results.size());
        assertDocumentHasName(results.get(0), "D");
        assertDocumentHasName(results.get(1), "C");
        assertDocumentHasName(results.get(2), "B");
        assertDocumentHasName(results.get(3), "A");
    }

    private void createBucket(String bucketName) {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.create(bucketName).encode(buf);
        Object response = runCommand(primaryChannel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
    }

    private BucketMetadata openBucketMetadata(String bucketName) {
        try (Transaction tr = primaryContext.getFoundationDB().createTransaction()) {
            String namespace = primaryContext.getConfig().getString("default_namespace");
            return BucketMetadataUtil.open(primaryContext, tr, namespace, bucketName);
        }
    }

    private List<ObjectId> insertDocuments(String bucketName, List<byte[]> documents) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert(bucketName, documents).encode(buf);

        Object msg = runCommand(primaryChannel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage response = (ArrayRedisMessage) msg;

        assertEquals(documents.size(), response.children().size());

        List<ObjectId> objectIds = new ArrayList<>();
        for (int i = 0; i < documents.size(); i++) {
            FullBulkStringRedisMessage message = (FullBulkStringRedisMessage) response.children().get(i);
            objectIds.add(TestUtil.bulkStringToObjectId(message));
        }
        return objectIds;
    }

    private List<DocumentLocation> buildDocumentLocations(
            BucketMetadata metadata,
            List<ObjectId> objectIds
    ) {
        List<DocumentLocation> locations = new ArrayList<>();

        // Get the primary index subspace
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        assertNotNull(primaryIndex, "Primary index should exist");

        try (Transaction tr = primaryContext.getFoundationDB().createTransaction()) {
            for (ObjectId objectId : objectIds) {
                // Look up the index entry by ObjectId: (ENTRIES, ObjectId) -> IndexEntry
                byte[] indexKey = primaryIndex.subspace().pack(
                        Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectId.toByteArray())
                );
                byte[] indexEntryBytes = tr.get(indexKey).join();
                assertNotNull(indexEntryBytes, "Index entry should exist for " + objectId);

                IndexEntry indexEntry = IndexEntry.decode(indexEntryBytes);
                EntryMetadata entryMetadata = EntryMetadata.decode(indexEntry.entryMetadata());
                locations.add(new DocumentLocation(objectId, indexEntry.shardId(), entryMetadata));
            }
        }

        return locations;
    }

    private void assertDocumentHasName(ByteBuffer buffer, String expectedName) {
        buffer.rewind();
        try (BsonBinaryReader reader = new BsonBinaryReader(buffer)) {
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                String fieldName = reader.readName();
                if ("name".equals(fieldName)) {
                    assertEquals(expectedName, reader.readString());
                    return;
                } else {
                    reader.skipValue();
                }
            }
            reader.readEndDocument();
        }
        fail("Document does not contain 'name' field");
    }
}
