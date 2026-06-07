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
import com.kronotop.TestUtil;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.handlers.protocol.SortDirection;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import org.bson.BsonBoolean;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class UnionNodeWithIndexScanStrategyTest extends BasePipelineTest {
    @Test
    void shouldHandleOrQueryWithMultipleIndexes() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-multi-index";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-index", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 35 } }, { 'quantity': { '$lte': 35 } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void shouldHandleOrQueryWithAllDocumentsMatching() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-all-match";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-index", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 10 } }, { 'quantity': { '$lte': 5 } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void shouldHandleOrQueryWithLimitAndPagination() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-limit-pagination";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-index", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 35 } }, { 'quantity': { '$lte': 35 } } ] }");
        QueryOptions options = QueryOptions.builder().limit(2).build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());


        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");

        List<String> actualResult = new ArrayList<>();
        int iterationCount = 0;
        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
                if (result.isEmpty()) {
                    break;
                }
                for (ByteBuffer buffer : result) {
                    actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
                }
            }
            iterationCount++;
        }
        assertEquals(3, iterationCount);
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void shouldHandleOrQueryWithRangeScanAndComparison() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-range-scan";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-index", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 65, 'quantity': 65, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 35, '$lt': 50 } }, { 'quantity': { '$lte': 35 } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void shouldHandleOrQueryWithIndexedAndNonIndexedFields() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-mixed-strategy";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 35 } }, { 'quantity': { '$lte': 35 } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void shouldHandleOrQueryWithNonIndexedFieldReturningEmpty() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-empty-branch";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25, 'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 35 } }, { 'category': { '$eq': 'Car' } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void shouldHandleThreeWayOrQueryWithMixedIndexing() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-three-way-mixed";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 65, 'quantity': 65, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        String query = "{ $or: [ { 'price': { '$gt': 35, '$lt': 50 } }, { 'quantity': { '$lte': 35 } }, { 'category': { '$ne': 'Car' } } ] }";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 65, \"quantity\": 65, \"category\": \"Food\"}");
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void shouldReturnEmptyForThreeWayOrQueryWithAllEmptyBranches() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-all-empty-branches";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 65, 'quantity': 65, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        String query = "{ $or: [ { 'price': { '$gt': 75, '$lt': 90 } }, { 'quantity': { '$gte': 200 } }, { 'category': { '$eq': 'Car' } } ] }";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void shouldNotRewindWhenUnionResultsExactlyMatchLimit() {
        // Behavior: When the union of child results produces exactly the number of entries equal
        // to the limit, no excess entries exist and rewind should not occur. Children advance
        // their cursors normally and the next ADVANCE resumes from the correct position.

        final String TEST_BUCKET_NAME = "test-union-exact-limit-boundary";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-index", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        // 6 docs with no overlap between branches: 3 match price>30, 3 match quantity<=25
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'A', 'price': 10, 'quantity': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B', 'price': 15, 'quantity': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C', 'price': 50, 'quantity': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'D', 'price': 60, 'quantity': 60}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'E', 'price': 20, 'quantity': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'F', 'price': 70, 'quantity': 70}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 30 } }, { 'quantity': { '$lte': 25 } } ] }");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        Set<String> allNames = new HashSet<>();
        int iterations = 0;

        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);

                if (results.isEmpty()) {
                    break;
                }

                assertTrue(results.size() <= 2, "Each batch should return at most 2 documents");
                allNames.addAll(extractNamesFromResults(results));
                iterations++;

                if (iterations > 10) {
                    fail("Too many iterations for 6 documents with limit 2");
                }
            }
        }

        assertEquals(6, allNames.size(), "Should find all 6 matching documents");
        assertEquals(Set.of("A", "B", "C", "D", "E", "F"), allNames);
    }

    @Test
    void shouldRewindMixedIndexScanAndFullScanChildrenWithLimit() {
        // Behavior: $or with one indexed field and one non-indexed field creates a UnionNode with
        // an IndexScanNode child and a FullScanNode child. With limit=2, rewind must correctly
        // restore checkpoints for both scan node types.

        final String TEST_BUCKET_NAME = "test-union-mixed-rewind";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        // 10 docs: 3 match both, 3 match price only (IndexScan), 2 match category only (FullScan), 2 match neither
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B1', 'price': 110, 'category': 'rare'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B2', 'price': 120, 'category': 'rare'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B3', 'price': 130, 'category': 'rare'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'P1', 'price': 140, 'category': 'common'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'P2', 'price': 150, 'category': 'common'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'P3', 'price': 160, 'category': 'common'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C1', 'price': 50, 'category': 'rare'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C2', 'price': 60, 'category': 'rare'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'N1', 'price': 70, 'category': 'common'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'N2', 'price': 80, 'category': 'common'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 100 } }, { 'category': { '$eq': 'rare' } } ] }");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> allNames = new ArrayList<>();
        int iterations = 0;

        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);

                if (results.isEmpty()) {
                    break;
                }

                assertTrue(results.size() <= 2, "Each batch should return at most 2 documents");
                allNames.addAll(extractNamesFromResults(results));
                iterations++;

                if (iterations > 15) {
                    fail("Too many iterations for 8 matching documents with limit 2");
                }
            }
        }

        Set<String> uniqueNames = new HashSet<>(allNames);
        assertEquals(allNames.size(), uniqueNames.size(), "Should have no duplicate documents");
        assertEquals(8, allNames.size(), "Should find all 8 matching documents");

        Set<String> expectedNames = Set.of("B1", "B2", "B3", "P1", "P2", "P3", "C1", "C2");
        assertEquals(expectedNames, uniqueNames);
    }

    @Test
    void shouldUpdateWithOrQueryAndPaginatedRewind() {
        // Behavior: UPDATE with $or on two indexed fields and limit=1 triggers child rewind in
        // UnionNode. ObjectId-based deduplication prevents duplicate updates across paginated
        // iterations, even though entry handles change after each document is rewritten.

        final String TEST_BUCKET_NAME = "test-union-update-or-paginated-rewind";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-index", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        // 8 docs: 2 match both (price>100 AND quantity<=25), 2 price-only, 2 quantity-only, 2 neither
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B1', 'price': 110, 'quantity': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B2', 'price': 120, 'quantity': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'P1', 'price': 140, 'quantity': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'P2', 'price': 150, 'quantity': 60}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Q1', 'price': 50, 'quantity': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Q2', 'price': 60, 'quantity': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'N1', 'price': 70, 'quantity': 70}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'N2', 'price': 80, 'quantity': 80}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 100 } }, { 'quantity': { '$lte': 25 } } ] }");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

        UpdateOptions update = UpdateOptions.builder().set("reviewed", new BsonBoolean(true)).build();
        QueryOptions options = QueryOptions.builder().limit(1).update(update).build();
        QueryContext updateCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> allUpdatedIds = new ArrayList<>();
        int iterations = 0;

        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ObjectId> updated = updateExecutor.execute(tr, updateCtx);
                if (updated.isEmpty()) {
                    break;
                }
                assertTrue(updated.size() <= 1, "Each batch should update at most 1 document");
                allUpdatedIds.addAll(updated);
                tr.commit().join();
            }
            iterations++;
            if (iterations > 15) {
                fail("Too many iterations for 6 matching documents with limit 1");
            }
        }

        assertEquals(6, allUpdatedIds.size(), "Should update 6 matching documents");
        Set<ObjectId> uniqueIds = new HashSet<>(allUpdatedIds);
        assertEquals(allUpdatedIds.size(), uniqueIds.size(), "No duplicate updates");

        List<String> allDocs = runQueryOnBucket(metadata, "{}");
        assertEquals(8, allDocs.size(), "All 8 documents should exist");

        int reviewedCount = 0;
        for (String json : allDocs) {
            if (json.contains("\"reviewed\": true")) {
                reviewedCount++;
            }
        }
        assertEquals(6, reviewedCount, "6 documents should have reviewed=true");
    }

    @Test
    void shouldUpdateWithOrQueryLimitTwoAndRewind() {
        // Behavior: UPDATE with $or on two indexed fields and limit=2 exercises the
        // keptHandles/excessHandles split in UnionNode where a single child contributes
        // both kept and excess entries in the same batch. All matching documents are
        // updated exactly once across multiple paginated iterations.

        final String TEST_BUCKET_NAME = "test-union-update-or-limit-two-rewind";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-index", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        // 10 docs: 3 match both (price>100 AND quantity<=25), 2 price-only, 3 quantity-only, 2 neither
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B1', 'price': 110, 'quantity': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B2', 'price': 120, 'quantity': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B3', 'price': 130, 'quantity': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'P1', 'price': 140, 'quantity': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'P2', 'price': 150, 'quantity': 60}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Q1', 'price': 50, 'quantity': 5}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Q2', 'price': 60, 'quantity': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Q3', 'price': 70, 'quantity': 22}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'N1', 'price': 80, 'quantity': 70}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'N2', 'price': 90, 'quantity': 80}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 100 } }, { 'quantity': { '$lte': 25 } } ] }");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

        UpdateOptions update = UpdateOptions.builder().set("reviewed", new BsonBoolean(true)).build();
        QueryOptions options = QueryOptions.builder().limit(2).update(update).build();
        QueryContext updateCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> allUpdatedIds = new ArrayList<>();
        int iterations = 0;

        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ObjectId> updated = updateExecutor.execute(tr, updateCtx);
                if (updated.isEmpty()) {
                    break;
                }
                assertTrue(updated.size() <= 2, "Each batch should update at most 2 documents");
                allUpdatedIds.addAll(updated);
                tr.commit().join();
            }
            iterations++;
            if (iterations > 15) {
                fail("Too many iterations for 8 matching documents with limit 2");
            }
        }

        assertEquals(8, allUpdatedIds.size(), "Should update 8 matching documents");
        Set<ObjectId> uniqueIds = new HashSet<>(allUpdatedIds);
        assertEquals(allUpdatedIds.size(), uniqueIds.size(), "No duplicate updates");

        List<String> allDocs = runQueryOnBucket(metadata, "{}");
        assertEquals(10, allDocs.size(), "All 10 documents should exist");

        int reviewedCount = 0;
        for (String json : allDocs) {
            if (json.contains("\"reviewed\": true")) {
                reviewedCount++;
            }
        }
        assertEquals(8, reviewedCount, "8 documents should have reviewed=true");
    }

    @Test
    void shouldHandleOrQueryWithSingleSubQuery() {
        // Behavior: $or with a single branch degenerates to just that branch's scan.
        // With an indexed field, the single branch uses IndexScan.

        final String TEST_BUCKET_NAME = "test-bucket-or-query-single-sub";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25, 'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 35 } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, readCtx);
            assertEquals(1, results.size());
            assertEquals(Set.of(45), extractIntegerFieldFromResults(results, "price"));
        }
    }

    @Test
    void shouldHandleOrQueryWithTwoFieldsReverse() {
        // Behavior: $or with two indexed fields and SortDirection.DESC returns results
        // in reverse versionstamp order.

        final String TEST_BUCKET_NAME = "test-bucket-or-query-two-fields-reverse";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-index", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25, 'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 35 } }, { 'quantity': { '$lte': 35 } } ] }");
        QueryOptions options = QueryOptions.builder().sortDirection(SortDirection.DESC).build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = List.of(
                "{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}",
                "{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}",
                "{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}",
                "{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}",
                "{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}"
        );

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, readCtx);
            List<String> actualResult = new ArrayList<>();
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
            assertEquals(expectedResult.size(), actualResult.size());
            assertTrue(actualResult.containsAll(expectedResult));
            assertTrue(expectedResult.containsAll(actualResult));
        }
    }

    @Test
    void shouldHandleOrQueryWithLargeDatasetBatchAnalysis() {
        // Behavior: Large dataset (200 docs) with $or on two indexed fields and limit=2.
        // Paginated iteration must return all matching documents without duplicates.

        final String TEST_BUCKET_NAME = "test-union-index-large-dataset-batch";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-index", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        // Insert 200 documents with controlled distribution for OR query: price > 250 OR quantity <= 250
        List<byte[]> documents = new ArrayList<>();

        // Group 1: price <= 250, quantity <= 250 (matches quantity branch only) - 20 documents
        for (int i = 1; i <= 20; i++) {
            int price = 200 + (i % 51); // prices 200-250
            int quantity = 200 + (i % 51); // quantities 200-250
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'price': %d, 'quantity': %d}", price, quantity)));
        }

        // Group 2: price > 250, quantity <= 250 (matches both branches) - 20 documents
        for (int i = 1; i <= 20; i++) {
            int price = 251 + i; // prices 252-271
            int quantity = 200 + (i % 51); // quantities 200-250
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'price': %d, 'quantity': %d}", price, quantity)));
        }

        // Group 3: price > 250, quantity > 250 (matches price branch only) - 140 documents
        for (int i = 1; i <= 140; i++) {
            int price = 251 + (i % 100); // prices 252-350
            int quantity = 251 + (i % 100); // quantities 252-350
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'price': %d, 'quantity': %d}", price, quantity)));
        }

        // Group 4: price <= 250, quantity > 250 (matches neither) - 20 documents
        for (int i = 1; i <= 20; i++) {
            int price = 200 + (i % 51); // prices 200-250
            int quantity = 251 + (i % 50); // quantities 252-300
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'price': %d, 'quantity': %d}", price, quantity)));
        }

        assertEquals(200, documents.size(), "Should have exactly 200 documents");
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Execute query with limit=2 and analyze each batch
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 250 } }, { 'quantity': { '$lte': 250 } } ] }");

        try (Transaction tr = createTransaction()) {
            QueryOptions config = QueryOptions.builder().limit(2).build();
            QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

            int iterationCount = 0;
            int totalRetrieved = 0;
            List<String> allBatchContents = new ArrayList<>();

            while (true) {
                iterationCount++;
                List<ByteBuffer> batchResults = readExecutor.execute(tr, ctx);

                if (batchResults.isEmpty()) {
                    break;
                }

                for (ByteBuffer buffer : batchResults) {
                    buffer.rewind();
                    String json = TestUtil.bsonToJsonWithoutId(buffer);
                    allBatchContents.add(json);
                    totalRetrieved++;
                }
            }

            int actualIterations = iterationCount - 1;
            int expectedIterations = totalRetrieved / 2;

            // Verify each retrieved document matches the OR condition
            for (String json : allBatchContents) {
                int priceStart = json.indexOf("\"price\": ") + 9;
                int priceEnd = json.indexOf(",", priceStart);
                int price = Integer.parseInt(json.substring(priceStart, priceEnd).trim());

                int qtyStart = json.indexOf("\"quantity\": ") + 12;
                int qtyEnd = json.indexOf("}", qtyStart);
                int quantity = Integer.parseInt(json.substring(qtyStart, qtyEnd).trim());

                assertTrue(price > 250 || quantity <= 250,
                        String.format("Document should match OR condition: %s", json));
            }

            // Verify total against full query
            QueryOptions fullConfig = QueryOptions.builder().limit(200).build();
            QueryContext fullCtx = new QueryContext(getSession(), metadata, fullConfig, planWithParams.plan(), planWithParams.parameters());
            List<ByteBuffer> fullResults = readExecutor.execute(tr, fullCtx);

            assertEquals(fullResults.size(), totalRetrieved,
                    String.format("Batch iteration (%d) should match full query (%d)", totalRetrieved, fullResults.size()));

            assertEquals(expectedIterations, actualIterations,
                    String.format("Should need exactly %d iterations for %d docs with limit=2", expectedIterations, totalRetrieved));
        }
    }

    @Test
    void shouldHandleOrQueryWithLargeDatasetBatchAnalysisReverse() {
        // Behavior: Large dataset (200 docs) with $or on two indexed fields, limit=2,
        // and SortDirection.DESC. Paginated iteration must return all matching documents
        // in reverse order without duplicates.

        final String TEST_BUCKET_NAME = "test-union-index-large-dataset-batch-reverse";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-index", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        // Insert 200 documents with controlled distribution for OR query: price > 250 OR quantity <= 250
        List<byte[]> documents = new ArrayList<>();

        // Group 1: price <= 250, quantity <= 250 (matches quantity branch only) - 20 documents
        for (int i = 1; i <= 20; i++) {
            int price = 200 + (i % 51);
            int quantity = 200 + (i % 51);
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'price': %d, 'quantity': %d}", price, quantity)));
        }

        // Group 2: price > 250, quantity <= 250 (matches both branches) - 20 documents
        for (int i = 1; i <= 20; i++) {
            int price = 251 + i;
            int quantity = 200 + (i % 51);
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'price': %d, 'quantity': %d}", price, quantity)));
        }

        // Group 3: price > 250, quantity > 250 (matches price branch only) - 140 documents
        for (int i = 1; i <= 140; i++) {
            int price = 251 + (i % 100);
            int quantity = 251 + (i % 100);
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'price': %d, 'quantity': %d}", price, quantity)));
        }

        // Group 4: price <= 250, quantity > 250 (matches neither) - 20 documents
        for (int i = 1; i <= 20; i++) {
            int price = 200 + (i % 51);
            int quantity = 251 + (i % 50);
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'price': %d, 'quantity': %d}", price, quantity)));
        }

        assertEquals(200, documents.size(), "Should have exactly 200 documents");
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Execute query with limit=2, SortDirection=DESC and analyze each batch
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 250 } }, { 'quantity': { '$lte': 250 } } ] }");

        try (Transaction tr = createTransaction()) {
            QueryOptions config = QueryOptions.builder().limit(2).sortDirection(SortDirection.DESC).build();
            QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

            int iterationCount = 0;
            int totalRetrieved = 0;
            List<String> allBatchContents = new ArrayList<>();

            while (true) {
                iterationCount++;
                List<ByteBuffer> batchResults = readExecutor.execute(tr, ctx);

                if (batchResults.isEmpty()) {
                    break;
                }

                for (ByteBuffer buffer : batchResults) {
                    buffer.rewind();
                    String json = TestUtil.bsonToJsonWithoutId(buffer);
                    allBatchContents.add(json);
                    totalRetrieved++;
                }
            }

            int actualIterations = iterationCount - 1;
            int expectedIterations = totalRetrieved / 2;

            // Verify each retrieved document matches the OR condition
            for (String json : allBatchContents) {
                int priceStart = json.indexOf("\"price\": ") + 9;
                int priceEnd = json.indexOf(",", priceStart);
                int price = Integer.parseInt(json.substring(priceStart, priceEnd).trim());

                int qtyStart = json.indexOf("\"quantity\": ") + 12;
                int qtyEnd = json.indexOf("}", qtyStart);
                int quantity = Integer.parseInt(json.substring(qtyStart, qtyEnd).trim());

                assertTrue(price > 250 || quantity <= 250,
                        String.format("Document should match OR condition: %s", json));
            }

            // Verify total against full query
            QueryOptions fullConfig = QueryOptions.builder().limit(200).build();
            QueryContext fullCtx = new QueryContext(getSession(), metadata, fullConfig, planWithParams.plan(), planWithParams.parameters());
            List<ByteBuffer> fullResults = readExecutor.execute(tr, fullCtx);

            assertEquals(fullResults.size(), totalRetrieved,
                    String.format("Batch iteration (%d) should match full query (%d)", totalRetrieved, fullResults.size()));

            assertTrue(totalRetrieved >= 150,
                    String.format("Should have at least 150 matches, got %d", totalRetrieved));

            assertEquals(expectedIterations, actualIterations,
                    String.format("Should need exactly %d iterations for %d docs with limit=2", expectedIterations, totalRetrieved));
        }
    }
}
