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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnionNodeWithIndexScanStrategyTest extends BasePipelineTest {
    @Test
    void testOrQueryWithMultipleIndexes() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-multi-index";

        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32);
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $or: [ { 'price': { '$gt': 35 } }, { 'quantity': { '$lte': 35 } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(metadata, options, plan);

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void testOrQueryWithAllDocumentsMatching() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-all-match";

        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32);
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $or: [ { 'price': { '$gt': 10 } }, { 'quantity': { '$lte': 5 } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(metadata, options, plan);

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void testOrQueryWithLimitAndPagination() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-limit-pagination";

        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32);
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $or: [ { 'price': { '$gt': 35 } }, { 'quantity': { '$lte': 35 } } ] }");
        QueryOptions options = QueryOptions.builder().limit(2).build();
        QueryContext readCtx = new QueryContext(metadata, options, plan);


        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");

        List<String> actualResult = new ArrayList<>();
        int iterationCount = 0;
        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Map<Versionstamp, ByteBuffer> result = readExecutor.execute(tr, readCtx);
                if (result.isEmpty()) {
                    break;
                }
                for (ByteBuffer buffer : result.values()) {
                    actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
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
    void testOrQueryWithRangeScanAndComparison() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-range-scan";

        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32);
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 65, 'quantity': 65, 'category': 'Food'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $or: [ { 'price': { '$gt': 35, '$lt': 50 } }, { 'quantity': { '$lte': 35 } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(metadata, options, plan);

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void testOrQueryWithIndexedAndNonIndexedFields() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-mixed-strategy";

        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $or: [ { 'price': { '$gt': 35 } }, { 'quantity': { '$lte': 35 } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(metadata, options, plan);

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void testOrQueryWithNonIndexedFieldReturningEmpty() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-empty-branch";

        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25, 'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $or: [ { 'price': { '$gt': 35 } }, { 'category': { '$eq': 'Car' } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(metadata, options, plan);

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void testThreeWayOrQueryWithMixedIndexing() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-three-way-mixed";

        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 65, 'quantity': 65, 'category': 'Food'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        String query = "{ $or: [ { 'price': { '$gt': 35, '$lt': 50 } }, { 'quantity': { '$lte': 35 } }, { 'category': { '$ne': 'Car' } } ] }";
        PipelineNode plan = createExecutionPlan(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(metadata, options, plan);

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 65, \"quantity\": 65, \"category\": \"Food\"}");
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void testThreeWayOrQueryReturningEmptyFromAllBranches() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-all-empty-branches";

        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25,  'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 65, 'quantity': 65, 'category': 'Food'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        String query = "{ $or: [ { 'price': { '$gt': 75, '$lt': 90 } }, { 'quantity': { '$gte': 200 } }, { 'category': { '$eq': 'Car' } } ] }";
        PipelineNode plan = createExecutionPlan(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<Versionstamp, ByteBuffer> result = readExecutor.execute(tr, readCtx);
            assertTrue(result.isEmpty());
        }
    }
}
