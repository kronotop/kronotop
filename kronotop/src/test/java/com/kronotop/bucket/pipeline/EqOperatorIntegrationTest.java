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
import com.kronotop.TestUtil;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EqOperatorIntegrationTest extends BasePipelineTest {

    @Test
    void shouldHandleFullScanNodeWithNullValuesEq() {
        final String TEST_BUCKET_NAME = "test-bucket-fullscan-null-eq";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$eq': null}}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of("{\"name\": \"Frank\"}", "{\"age\": null, \"name\": \"Donald\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindNullAndMissingFieldsViaIndexScanWithEqNull() {
        final String TEST_BUCKET_NAME = "test-bucket-indexscan-null-eq";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$eq': null}}");
        assertInstanceOf(IndexScanNode.class, planWithParams.plan());
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of("{\"name\": \"Frank\"}", "{\"age\": null, \"name\": \"Donald\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleIndexScanNodeWithNullValuesEq() {
        final String TEST_BUCKET_NAME = "test-bucket-indexscan-null-eq-2";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$eq': null}}");
        assertInstanceOf(IndexScanNode.class, planWithParams.plan());
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of("{\"name\": \"Frank\"}", "{\"age\": null, \"name\": \"Donald\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldReturnEmptyResultForEmptyBucketViaFullScan() {
        final String TEST_BUCKET_NAME = "test-bucket-empty-fullscan";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$eq': 25}}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void shouldReturnEmptyResultForEmptyBucketViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-empty-indexscan";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$eq': 25}}");
        assertInstanceOf(IndexScanNode.class, planWithParams.plan());
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void shouldHandleMixedIndexedAndNonIndexedFields() {
        final String TEST_BUCKET_NAME = "test-bucket-mixed-indexed-nonindexed";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'city': 'London', 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'city': 'Paris', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'city': 'London', 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'city': 'Berlin', 'name': 'Claire'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query on indexed field (age) AND non-indexed field (city)
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$eq': 25}, 'city': {'$eq': 'London'}}");
        assertInstanceOf(IndexScanNode.class, planWithParams.plan());

        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        // Only John matches both age=25 AND city=London
        List<String> expectedResult = List.of("{\"age\": 25, \"city\": \"London\", \"name\": \"John\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleNestedFieldQueryViaFullScan() {
        final String TEST_BUCKET_NAME = "test-bucket-nested-field-fullscan";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'user': {'name': 'John', 'age': 25}, 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'user': {'name': 'Alice', 'age': 30}, 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'user': {'name': 'John', 'age': 35}, 'status': 'inactive'}"),
                BSONUtil.jsonToDocumentThenBytes("{'user': {'name': 'George', 'age': 40}, 'status': 'active'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'user.name': {'$eq': 'John'}}");
        assertInstanceOf(FullScanNode.class, planWithParams.plan());
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"user\": {\"name\": \"John\", \"age\": 25}, \"status\": \"active\"}",
                "{\"user\": {\"name\": \"John\", \"age\": 35}, \"status\": \"inactive\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleNestedFieldQueryViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-nested-field-indexscan";

        SingleFieldIndexDefinition nestedIndex = SingleFieldIndexDefinition.create("user-name-index", "user.name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, nestedIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'user': {'name': 'John', 'age': 25}, 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'user': {'name': 'Alice', 'age': 30}, 'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'user': {'name': 'John', 'age': 35}, 'status': 'inactive'}"),
                BSONUtil.jsonToDocumentThenBytes("{'user': {'name': 'George', 'age': 40}, 'status': 'active'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'user.name': {'$eq': 'John'}}");
        assertInstanceOf(IndexScanNode.class, planWithParams.plan());
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"user\": {\"name\": \"John\", \"age\": 25}, \"status\": \"active\"}",
                "{\"user\": {\"name\": \"John\", \"age\": 35}, \"status\": \"inactive\"}"
        );
        assertEquals(expectedResult, actualResult);
    }
}
