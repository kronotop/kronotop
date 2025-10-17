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

class PipelineExecutorIntegrationTest extends BasePipelineTest {

    void insertSampleData() {
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET, documents);
    }

    @Test
    void testNotExistedField() {
        final String TEST_BUCKET_NAME = "test-bucket-query-not-existed-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);
        insertSampleData();
        PipelineNode plan = createExecutionPlan(metadata, "{'not-existed-field': {'$gt': 22}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void testZeroResultsWhileComparingNull() {
        final String TEST_BUCKET_NAME = "test-bucket-query-not-existed-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);
        insertSampleData();
        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': null}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void testFullScanNodeWithNullValues_EQ() {
        final String TEST_BUCKET_NAME = "test-bucket-query-not-existed-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$eq': null}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of("{\"name\": \"Frank\"}", "{\"age\": null, \"name\": \"Donald\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void testFullScanNodeWithNullValues_NE() {
        final String TEST_BUCKET_NAME = "test-bucket-query-not-existed-field";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$ne': null}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of(
                "{\"age\": 20, \"name\": \"John\"}",
                "{\"age\": 23, \"name\": \"Alice\"}",
                "{\"age\": 25, \"name\": \"George\"}",
                "{\"age\": 35, \"name\": \"Claire\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void testIndexScanNodeWithNullValues_EQ() {
        final String TEST_BUCKET_NAME = "test-bucket-query-not-existed-field";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$eq': null}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of("{\"name\": \"Frank\"}", "{\"age\": null, \"name\": \"Donald\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void testIndexScanNodeWithNullValues_NE() {
        final String TEST_BUCKET_NAME = "test-bucket-query-not-existed-field";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$ne': null}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of(
                "{\"age\": 20, \"name\": \"John\"}",
                "{\"age\": 23, \"name\": \"Alice\"}",
                "{\"age\": 25, \"name\": \"George\"}",
                "{\"age\": 35, \"name\": \"Claire\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void testRangeScanNodeWithNullValues_NULL_Boundaries() {
        final String TEST_BUCKET_NAME = "test-bucket-query-not-existed-field";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ 'age': { '$gt': null, '$lt': null } }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void testRangeScanNodeNonsenseQuery() {
        final String TEST_BUCKET_NAME = "test-bucket-query-not-existed-field";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ 'age': { '$ne': null, '$eq': null } }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void testContradictionShouldReturnEmptyResult() {
        final String TEST_BUCKET_NAME = "test-bucket-query-not-existed-field";

        IndexDefinition ageIndex = IndexDefinition.create("name-index", "name", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ 'name': { '$eq': 'A', '$eq': 'B' } }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty());
        }
    }
}
