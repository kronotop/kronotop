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

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.planner.physical.PhysicalPlanValidationException;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.bson.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class PipelineIntegrationTest extends BasePipelineTest {

    @Test
    void shouldSelectSortByIndexForEmptyFilterWithSortBy() {
        // Behavior: When a query has an empty filter {} with sortBy on an indexed field, the planner
        // should select a RangeScanNode using that index with null bounds for full range scan.

        // Create an index on the age field
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex);

        // Create execution plan with empty filter and sortBy age
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{}", "age");

        // Verify the plan is a RangeScanNode (not FullScanNode)
        assertInstanceOf(RangeScanNode.class, planWithParams.plan(), "Empty filter with sortBy on indexed field should use RangeScanNode");

        // Verify it selected the age index
        RangeScanNode rangeScanNode = (RangeScanNode) planWithParams.plan();
        assertEquals("age", rangeScanNode.getIndexDefinition().selector(),
                "Should select the age index for sortBy");

        // Verify the predicate covers full range (null bounds for empty filter)
        RangeScanPredicate predicate = rangeScanNode.predicate();
        assertEquals("age", predicate.selector());
        assertNull(predicate.lowerBound(), "Lower bound should be null for full range scan");
        assertNull(predicate.upperBound(), "Upper bound should be null for full range scan");
    }

    @Test
    void shouldSelectSortByIndexForEmptyFilterWithSortByDescending() {
        // Behavior: Empty filter with sortBy DESC on an indexed field should use RangeScanNode.
        // The sort direction is handled at execution time, not at plan creation.

        // Create an index on the age field
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex);

        // Create an execution plan with empty filter and sortBy age (DESC uses the same index, direction handled at execution)
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{}", "age");

        // Verify the plan is a RangeScanNode
        assertInstanceOf(RangeScanNode.class, planWithParams.plan(), "Empty filter with sortBy on indexed field should use RangeScanNode");

        // Verify it selected the age index
        RangeScanNode rangeScanNode = (RangeScanNode) planWithParams.plan();
        assertEquals("age", rangeScanNode.getIndexDefinition().selector(),
                "Should select the age index for sortBy DESC");
        assertEquals(BsonType.INT32, rangeScanNode.getIndexDefinition().bsonType(),
                "Index should be INT32 type");
    }

    @Test
    void shouldRejectSortByWhenFieldNotIndexed() {
        // Behavior: Empty filter with sortBy on a non-indexed field is rejected because
        // no index provides natural ordering for the requested sort field.

        // Create an index on age field only
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex);

        assertThrows(PhysicalPlanValidationException.class,
                () -> createPlanWithParams(metadata, "{}", "name"));
    }

    @Test
    void shouldSelectCorrectIndexWhenMultipleIndexesExist() {
        // Behavior: When multiple indexes exist, the planner selects the index matching the
        // sortBy field. Each sortBy field gets its corresponding index for optimal ordering.

        // Create indexes on both age and name fields
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name-idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex, nameIndex);

        // Create execution plan with empty filter and sortBy age
        PlanWithParams agePlanWithParams = createPlanWithParams(metadata, "{}", "age");
        assertInstanceOf(RangeScanNode.class, agePlanWithParams.plan());
        assertEquals("age", ((RangeScanNode) agePlanWithParams.plan()).getIndexDefinition().selector(),
                "Should select age index when sortBy age");

        // Create execution plan with empty filter and sortBy name
        PlanWithParams namePlanWithParams = createPlanWithParams(metadata, "{}", "name");
        assertInstanceOf(RangeScanNode.class, namePlanWithParams.plan());
        assertEquals("name", ((RangeScanNode) namePlanWithParams.plan()).getIndexDefinition().selector(),
                "Should select name index when sortBy name");
    }

    @Test
    void shouldQueryByIdWithBsonBinaryGte() {
        // Behavior: Query using _id with $gte operator where the versionstamp is passed as raw
        // BsonBinary (not string-encoded). This tests the primary index range scan with binary values.

        final String TEST_BUCKET_NAME = "test-bucket-id-binary-gte";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'David'}")
        );

        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Use the second versionstamp as the lower bound (should return Bob, Charlie, David)
        ObjectId lowerBound = objectIds.get(1);

        // Build query with BsonBinary instead of a string-encoded versionstamp
        BsonDocument query = new BsonDocument("_id",
                new BsonDocument("$gte", new BsonObjectId(lowerBound))
        );

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(3, results.size(), "Should return 3 documents (Bob, Charlie, David)");
            assertEquals(Set.of("Bob", "Charlie", "David"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldQueryByIdRangeWithIndexedField() {
        // Behavior: Query combining _id range ($gte) with an indexed secondary field condition ($gt).
        // The planner should use the most selective index scan with a residual predicate for the other condition.

        final String TEST_BUCKET_NAME = "test-id-range-indexed-field";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 18}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 19}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'David', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Eve', 'age': 22}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank', 'age': 35}")
        );

        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: _id >= versionstamp[2] (Charlie) AND age > 20
        // Expected: David (age 30), Eve (age 22), Frank (age 35)
        ObjectId lowerBound = objectIds.get(2);

        BsonDocument query = new BsonDocument("$and", new BsonArray(List.of(
                new BsonDocument("_id", new BsonDocument("$gte", new BsonObjectId(lowerBound))),
                new BsonDocument("age", new BsonDocument("$gt", new BsonInt32(20)))
        )));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(3, results.size(), "Should return 3 documents (David, Eve, Frank)");
            assertEquals(Set.of("David", "Eve", "Frank"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldQueryByIdRangeWithNonIndexedField() {
        // Behavior: Query combining _id range ($gte) with a non-indexed field condition ($eq).
        // The planner should use primary index RangeScan with residual filter on the non-indexed field.

        final String TEST_BUCKET_NAME = "test-id-range-non-indexed-field";

        // No secondary indexes created
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'status': 'inactive'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'David', 'status': 'inactive'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Eve', 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank', 'status': 'active'}")
        );

        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: _id >= versionstamp[2] (Charlie) AND status = "active"
        // Expected: Charlie (active), Eve (active), Frank (active)
        ObjectId lowerBound = objectIds.get(2);

        BsonDocument query = new BsonDocument("$and", new BsonArray(List.of(
                new BsonDocument("_id", new BsonDocument("$gte", new BsonObjectId(lowerBound))),
                new BsonDocument("status", new BsonDocument("$eq", new BsonString("active")))
        )));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(3, results.size(), "Should return 3 documents (Charlie, Eve, Frank)");
            assertEquals(Set.of("Charlie", "Eve", "Frank"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldQueryByIdRangeWithIndexedFieldUsingLte() {
        // Behavior: Query combining _id upper-bound range ($lte) with an indexed secondary field condition ($gt).
        // Tests upper-bound range on _id (not just lower-bound) with secondary index intersection.

        final String TEST_BUCKET_NAME = "test-id-range-lte-indexed";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 28}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 22}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'David', 'age': 26}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Eve', 'age': 35}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank', 'age': 40}")
        );

        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: _id <= versionstamp[3] (David) AND age > 25
        // Expected: Alice (age 28), Charlie (age 30), David (age 26)
        ObjectId upperBound = objectIds.get(3);

        BsonDocument query = new BsonDocument("$and", new BsonArray(List.of(
                new BsonDocument("_id", new BsonDocument("$lte", new BsonObjectId(upperBound))),
                new BsonDocument("age", new BsonDocument("$gt", new BsonInt32(25)))
        )));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(3, results.size(), "Should return 3 documents (Alice, Charlie, David)");
            assertEquals(Set.of("Alice", "Charlie", "David"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldQueryByIdRangeBetweenWithIndexedField() {
        // Behavior: Query combining bounded _id range ($gte and $lt) with an indexed secondary field condition ($eq).
        // Tests range consolidation on _id combined with secondary index scan.

        final String TEST_BUCKET_NAME = "test-id-range-between-indexed";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'David', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Eve', 'age': 28}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank', 'age': 30}")
        );

        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: _id >= versionstamp[1] (Bob) AND _id < versionstamp[5] (Frank) AND age = 30
        // Expected: Charlie (age 30), David (age 30)
        ObjectId lowerBound = objectIds.get(1);
        ObjectId upperBound = objectIds.get(5);

        BsonDocument query = new BsonDocument("$and", new BsonArray(List.of(
                new BsonDocument("_id", new BsonDocument("$gte", new BsonObjectId(lowerBound))),
                new BsonDocument("_id", new BsonDocument("$lt", new BsonObjectId(upperBound))),
                new BsonDocument("age", new BsonDocument("$eq", new BsonInt32(30)))
        )));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return 2 documents (Charlie, David)");
            assertEquals(Set.of("Charlie", "David"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldSelectOneIndexWhenMultipleIndexedConditionsHaveEqualSelectivity() {
        // Behavior: When two indexed fields have conditions with equal selectivity cost,
        // the planner selects one index for scanning and converts the other condition to a
        // residual predicate. Without histograms, SelectivityEstimator returns UNKNOWN (infinity)
        // for both indexes.

        final String TEST_BUCKET_NAME = "test-equal-selectivity-indexes";

        // Create indexes on both price and quantity fields
        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-idx", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-idx", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product1', 'price': 50, 'quantity': 5}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product2', 'price': 150, 'quantity': 5}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product3', 'price': 50, 'quantity': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product4', 'price': 150, 'quantity': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product5', 'price': 200, 'quantity': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Product6', 'price': 120, 'quantity': 8}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: price > 100 AND quantity > 10
        // Both conditions use range operators on indexed fields, so both have equal selectivity cost.
        // The planner selects one index and makes the other a residual predicate.
        // Expected results: Product4 (price=150, qty=15), Product5 (price=200, qty=20)
        BsonDocument query = new BsonDocument("$and", new BsonArray(List.of(
                new BsonDocument("quantity", new BsonDocument("$gt", new BsonInt32(10))),
                new BsonDocument("price", new BsonDocument("$gt", new BsonInt32(100)))
        )));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));

        // Verify plan structure: IndexScanNode with residual predicate
        assertInstanceOf(IndexScanNode.class, planWithParams.plan(),
                "Two indexed range conditions should produce an IndexScanNode (one index selected)");

        IndexScanNode indexScan = (IndexScanNode) planWithParams.plan();

        // Verify which index was selected
        assertEquals("quantity", indexScan.getIndexDefinition().selector(),
                "Should select 'quantity' index");

        // Verify the next node is TransformWithResidualPredicateNode containing the 'price' condition
        assertInstanceOf(TransformWithResidualPredicateNode.class, indexScan.next(),
                "Should have a TransformWithResidualPredicateNode for residual filtering");

        TransformWithResidualPredicateNode transformNode = (TransformWithResidualPredicateNode) indexScan.next();
        assertInstanceOf(ResidualAndNode.class, transformNode.predicate(),
                "Residual predicate should be a ResidualAndNode");

        ResidualAndNode residualAnd = (ResidualAndNode) transformNode.predicate();
        assertEquals(1, residualAnd.children().size(), "Should have 1 residual predicate child");

        assertInstanceOf(ResidualPredicate.class, residualAnd.children().get(0));
        ResidualPredicate pricePredicate = (ResidualPredicate) residualAnd.children().get(0);
        assertEquals("price", pricePredicate.selector(),
                "Residual predicate should be on 'price' field");

        // Execute and verify results
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return 2 documents (Product4, Product5)");
            assertEquals(Set.of("Product4", "Product5"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldIsolateDocumentsAcrossNamespacesWithinSameTransaction() {
        // Behavior: Documents inserted into different namespaces within the same transaction
        // should be isolated from each other. After commit, each document should only be
        // accessible from its respective namespace-bucket pair.

        String namespace1 = UUID.randomUUID().toString();
        String namespace2 = UUID.randomUUID().toString();
        String bucketName = "cross-namespace-bucket";

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<byte[], byte[]> bucketCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        // Create namespace1
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace1).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Create namespace2
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace2).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // BEGIN transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Switch to namespace1 and insert a document
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace1).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.create(bucketName).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        byte[] doc1 = BSONUtil.jsonToDocumentThenBytes("{'name': 'DocumentInNamespace1', 'namespace': 'ns1'}");
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.insert(bucketName, doc1).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage arrayMessage = (ArrayRedisMessage) response;
            assertEquals(1, arrayMessage.children().size());
            // Within transaction, an ObjectId is returned
            assertInstanceOf(FullBulkStringRedisMessage.class, arrayMessage.children().getFirst());
        }

        // Switch to namespace2 and insert a document
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace2).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.create(bucketName).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        byte[] doc2 = BSONUtil.jsonToDocumentThenBytes("{'name': 'DocumentInNamespace2', 'namespace': 'ns2'}");
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.insert(bucketName, doc2).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage arrayMessage = (ArrayRedisMessage) response;
            assertEquals(1, arrayMessage.children().size());
            assertInstanceOf(FullBulkStringRedisMessage.class, arrayMessage.children().getFirst());
        }

        // COMMIT transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.commit().encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(RESPVersion.RESP3);

        // Verify document in namespace1
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace1).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(bucketName, "{}").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, response);
            MapRedisMessage mapMessage = (MapRedisMessage) response;
            // entries key contains the actual document map
            ArrayRedisMessage entriesArray = null;
            for (Map.Entry<RedisMessage, RedisMessage> entry : mapMessage.children().entrySet()) {
                if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && "entries".equals(keyMsg.content().toString(StandardCharsets.UTF_8))) {
                    entriesArray = (ArrayRedisMessage) entry.getValue();
                    break;
                }
            }
            assertNotNull(entriesArray, "Should have entries in response");
            assertEquals(1, entriesArray.children().size(), "Namespace1 bucket should have exactly 1 document");

            // Verify exact document content
            FullBulkStringRedisMessage docMessage = (FullBulkStringRedisMessage) entriesArray.children().getFirst();
            byte[] docBytes = ByteBufUtil.getBytes(docMessage.content());
            BsonDocument retrievedDoc = BSONUtil.toBsonDocument(docBytes);
            assertEquals("DocumentInNamespace1", retrievedDoc.getString("name").getValue(),
                    "Document name should match");
            assertEquals("ns1", retrievedDoc.getString("namespace").getValue(),
                    "Document namespace field should match");
        }

        // Verify document in namespace2
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace2).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(bucketName, "{}").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, response);
            MapRedisMessage mapMessage = (MapRedisMessage) response;
            ArrayRedisMessage entriesArray = null;
            for (Map.Entry<RedisMessage, RedisMessage> entry : mapMessage.children().entrySet()) {
                if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && "entries".equals(keyMsg.content().toString(StandardCharsets.UTF_8))) {
                    entriesArray = (ArrayRedisMessage) entry.getValue();
                    break;
                }
            }
            assertNotNull(entriesArray, "Should have entries in response");
            assertEquals(1, entriesArray.children().size(), "Namespace2 bucket should have exactly 1 document");

            // Verify exact document content
            FullBulkStringRedisMessage docMessage = (FullBulkStringRedisMessage) entriesArray.children().getFirst();
            byte[] docBytes = ByteBufUtil.getBytes(docMessage.content());
            BsonDocument retrievedDoc = BSONUtil.toBsonDocument(docBytes);
            assertEquals("DocumentInNamespace2", retrievedDoc.getString("name").getValue(),
                    "Document name should match");
            assertEquals("ns2", retrievedDoc.getString("namespace").getValue(),
                    "Document namespace field should match");
        }

        // Verify cross-namespace isolation: query namespace1 bucket for namespace2's document
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace1).encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(bucketName, "{'name': 'DocumentInNamespace2'}").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, response);
            MapRedisMessage mapMessage = (MapRedisMessage) response;
            ArrayRedisMessage entriesArray = null;
            for (Map.Entry<RedisMessage, RedisMessage> entry : mapMessage.children().entrySet()) {
                if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && "entries".equals(keyMsg.content().toString(StandardCharsets.UTF_8))) {
                    entriesArray = (ArrayRedisMessage) entry.getValue();
                    break;
                }
            }
            assertNotNull(entriesArray, "Should have entries in response");
            assertEquals(0, entriesArray.children().size(), "Namespace1 bucket should NOT contain namespace2's document");
        }

        // Verify cross-namespace isolation: query namespace2 bucket for namespace1's document
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace2).encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(bucketName, "{'name': 'DocumentInNamespace1'}").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, response);
            MapRedisMessage mapMessage = (MapRedisMessage) response;
            ArrayRedisMessage entriesArray = null;
            for (Map.Entry<RedisMessage, RedisMessage> entry : mapMessage.children().entrySet()) {
                if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && "entries".equals(keyMsg.content().toString(StandardCharsets.UTF_8))) {
                    entriesArray = (ArrayRedisMessage) entry.getValue();
                    break;
                }
            }
            assertNotNull(entriesArray, "Should have entries in response");
            assertEquals(0, entriesArray.children().size(), "Namespace2 bucket should NOT contain namespace1's document");
        }
    }

    @Test
    void shouldDiscardDocumentsAcrossNamespacesOnRollback() {
        // Behavior: When a transaction is rolled back, all documents inserted across multiple
        // namespaces should be discarded. This verifies atomicity of cross-namespace operations.

        String namespace1 = UUID.randomUUID().toString();
        String namespace2 = UUID.randomUUID().toString();
        String bucketName = "rollback-test-bucket";

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<byte[], byte[]> bucketCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        // Create namespaces
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace1).encode(buf);
            runCommand(channel, buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace2).encode(buf);
            runCommand(channel, buf);
        }

        // BEGIN transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Insert document in namespace1
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace1).encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.create(bucketName).encode(buf);
            runCommand(channel, buf);
        }

        {
            byte[] doc = BSONUtil.jsonToDocumentThenBytes("{'name': 'WillBeDiscarded1'}");
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.insert(bucketName, doc).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // Insert document in namespace2
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace2).encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.create(bucketName).encode(buf);
            runCommand(channel, buf);
        }

        {
            byte[] doc = BSONUtil.jsonToDocumentThenBytes("{'name': 'WillBeDiscarded2'}");
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.insert(bucketName, doc).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // ROLLBACK transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.rollback().encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(RESPVersion.RESP3);

        // Verify namespace1 bucket is empty (the document was discarded)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace1).encode(buf);
            runCommand(channel, buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(bucketName, "{}").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, response);
            MapRedisMessage mapMessage = (MapRedisMessage) response;
            ArrayRedisMessage entriesArray = null;
            for (Map.Entry<RedisMessage, RedisMessage> entry : mapMessage.children().entrySet()) {
                if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && "entries".equals(keyMsg.content().toString(StandardCharsets.UTF_8))) {
                    entriesArray = (ArrayRedisMessage) entry.getValue();
                    break;
                }
            }
            assertNotNull(entriesArray, "Should have entries key in response");
            assertEquals(0, entriesArray.children().size(), "Namespace1 bucket should be empty after rollback");
        }

        // Verify namespace2 bucket is empty (the document was discarded)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace2).encode(buf);
            runCommand(channel, buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(bucketName, "{}").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, response);
            MapRedisMessage mapMessage = (MapRedisMessage) response;
            ArrayRedisMessage entriesArray = null;
            for (Map.Entry<RedisMessage, RedisMessage> entry : mapMessage.children().entrySet()) {
                if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && "entries".equals(keyMsg.content().toString(StandardCharsets.UTF_8))) {
                    entriesArray = (ArrayRedisMessage) entry.getValue();
                    break;
                }
            }
            assertNotNull(entriesArray, "Should have entries key in response");
            assertEquals(0, entriesArray.children().size(), "Namespace2 bucket should be empty after rollback");
        }
    }

    @Test
    void shouldIsolateBucketsWithinSameNamespaceInTransaction() {
        // Behavior: Documents inserted into different buckets within the same namespace
        // in a single transaction should be isolated by bucket after commit.

        String namespace = UUID.randomUUID().toString();
        String bucket1 = "bucket-alpha";
        String bucket2 = "bucket-beta";

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<byte[], byte[]> bucketCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        // Create namespace
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(namespace).encode(buf);
            runCommand(channel, buf);
        }

        // Switch to namespace
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(namespace).encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.create(bucket1).encode(buf);
            runCommand(channel, buf);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.create(bucket2).encode(buf);
            runCommand(channel, buf);
        }

        // BEGIN transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);
            runCommand(channel, buf);
        }

        // Insert document in bucket1
        {
            byte[] doc = BSONUtil.jsonToDocumentThenBytes("{'name': 'AlphaDoc', 'bucket': 'alpha'}");
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.insert(bucket1, doc).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // Insert document in bucket2
        {
            byte[] doc = BSONUtil.jsonToDocumentThenBytes("{'name': 'BetaDoc', 'bucket': 'beta'}");
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.insert(bucket2, doc).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // COMMIT transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.commit().encode(buf);
            runCommand(channel, buf);
        }

        BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(RESPVersion.RESP3);

        // Verify bucket1 contains only AlphaDoc
        {
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(bucket1, "{}").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, response);
            MapRedisMessage mapMessage = (MapRedisMessage) response;
            ArrayRedisMessage entriesArray = null;
            for (Map.Entry<RedisMessage, RedisMessage> entry : mapMessage.children().entrySet()) {
                if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && "entries".equals(keyMsg.content().toString(StandardCharsets.UTF_8))) {
                    entriesArray = (ArrayRedisMessage) entry.getValue();
                    break;
                }
            }
            assertNotNull(entriesArray, "Should have entries in response");
            assertEquals(1, entriesArray.children().size(), "Bucket1 should have exactly 1 document");

            FullBulkStringRedisMessage docMessage = (FullBulkStringRedisMessage) entriesArray.children().getFirst();
            byte[] docBytes = ByteBufUtil.getBytes(docMessage.content());
            BsonDocument doc = BSONUtil.toBsonDocument(docBytes);
            assertEquals("AlphaDoc", doc.getString("name").getValue());
            assertEquals("alpha", doc.getString("bucket").getValue());
        }

        // Verify bucket2 contains only BetaDoc
        {
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(bucket2, "{}").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, response);
            MapRedisMessage mapMessage = (MapRedisMessage) response;
            ArrayRedisMessage entriesArray = null;
            for (Map.Entry<RedisMessage, RedisMessage> entry : mapMessage.children().entrySet()) {
                if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && "entries".equals(keyMsg.content().toString(StandardCharsets.UTF_8))) {
                    entriesArray = (ArrayRedisMessage) entry.getValue();
                    break;
                }
            }
            assertNotNull(entriesArray, "Should have entries in response");
            assertEquals(1, entriesArray.children().size(), "Bucket2 should have exactly 1 document");

            FullBulkStringRedisMessage docMessage = (FullBulkStringRedisMessage) entriesArray.children().getFirst();
            byte[] docBytes = ByteBufUtil.getBytes(docMessage.content());
            BsonDocument doc = BSONUtil.toBsonDocument(docBytes);
            assertEquals("BetaDoc", doc.getString("name").getValue());
            assertEquals("beta", doc.getString("bucket").getValue());
        }

        // Verify cross-bucket isolation: query bucket1 for BetaDoc
        {
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(bucket1, "{'name': 'BetaDoc'}").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, response);
            MapRedisMessage mapMessage = (MapRedisMessage) response;
            ArrayRedisMessage entriesArray = null;
            for (Map.Entry<RedisMessage, RedisMessage> entry : mapMessage.children().entrySet()) {
                if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg && "entries".equals(keyMsg.content().toString(StandardCharsets.UTF_8))) {
                    entriesArray = (ArrayRedisMessage) entry.getValue();
                    break;
                }
            }
            assertNotNull(entriesArray, "Should have entries in response");
            assertEquals(0, entriesArray.children().size(), "Bucket1 should NOT contain BetaDoc");
        }
    }

    @Test
    void shouldUseCompoundIndexForLugWidthAndPriceFilter() {
        // Behavior: When a compound index covers (lug_width, price), a query filtering on both
        // fields uses a single CompoundIndexScanNode with EQ prefix on lug_width and range on price.

        final String TEST_BUCKET_NAME = "test-compound-lug-width-price";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("lug-width-price-idx", List.of(
                new CompoundIndexField("lug_width", BsonType.INT32, false),
                new CompoundIndexField("price", BsonType.DOUBLE, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Strap1', 'lug_width': 20, 'price': 29.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Strap2', 'lug_width': 22, 'price': 19.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Strap3', 'lug_width': 20, 'price': 49.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Strap4', 'lug_width': 18, 'price': 39.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Strap5', 'lug_width': 20, 'price': 15.00}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: lug_width = 20 AND price > 20.0
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{lug_width: 20, price: {$gt: 20.0}}");

        // Plan should be CompoundIndexScanNode
        assertInstanceOf(CompoundIndexScanNode.class, planWithParams.plan());

        // Execute and verify: should return 2 straps with lug_width=20 and price > 20
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            assertEquals(Set.of("Strap1", "Strap3"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldUseCompoundIndexForCategoryAndPriceRange() {
        // Behavior: A compound index on (category:STRING, price:DOUBLE) enables a single range scan
        // for "all products in a category with price above X" — a common e-commerce filter pattern.

        final String TEST_BUCKET_NAME = "test-compound-category-price";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("category-price-idx", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("price", BsonType.DOUBLE, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Basic Tee',      'category': 'clothing', 'price': 19.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Premium Hoodie', 'category': 'clothing', 'price': 89.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Silk Dress',     'category': 'clothing', 'price': 149.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Wool Coat',      'category': 'clothing', 'price': 299.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Snapback Cap',   'category': 'clothing', 'price': 34.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'USB-C Cable',    'category': 'electronics', 'price': 12.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Wireless Mouse', 'category': 'electronics', 'price': 45.00}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Mechanical KB',  'category': 'electronics', 'price': 129.00}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query: all clothing priced above $30
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{price: {$gt: 30.0}, category: 'clothing'}");

        assertInstanceOf(CompoundIndexScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(4, results.size());
            assertEquals(Set.of("Snapback Cap", "Premium Hoodie", "Silk Dress", "Wool Coat"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldUseCompoundIndexForAndQueryWithSortByOnIndexedField() {
        // Behavior: A compound index on (name:STRING, age:INT32, salary:INT32) supports
        // an $and filter on name+age with SORTBY on salary, because salary is covered by the index.

        final String TEST_BUCKET_NAME = "test-compound-and-sortby-salary";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("name-age-salary-idx", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false),
                new CompoundIndexField("salary", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'john', 'age': 25, 'salary': 7000}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'john', 'age': 25, 'salary': 5000}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'john', 'age': 25, 'salary': 3000}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'john', 'age': 30, 'salary': 9000}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'jane', 'age': 25, 'salary': 6000}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{ '$and': [{ 'name': 'john' }, { 'age': 25 }] }", "salary");

        assertInstanceOf(CompoundIndexScanNode.class, planWithParams.plan());

        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(3, results.size());
            assertEquals(Set.of(3000, 5000, 7000), extractIntegerFieldFromResults(results, "salary"));
        }
    }
}
