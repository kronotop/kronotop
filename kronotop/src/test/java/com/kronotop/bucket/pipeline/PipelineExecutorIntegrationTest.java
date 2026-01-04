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
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.bql.ast.StringVal;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.internal.VersionstampUtil;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

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
    void shouldHandleNotExistedField() {
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
    void shouldReturnZeroResultsWhenComparingNull() {
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
    void shouldHandleFullScanNodeWithNullValuesEq() {
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
    void shouldFindNullAndMissingFieldsViaIndexScanWithEqNull() {
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
    void shouldFallbackToFullScanWhenPredicateTypeMismatchesIndexType() {
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

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': '20'}}");
        assertInstanceOf(FullScanNode.class, plan);

        FullScanNode fullScanNode = (FullScanNode) plan;
        assertEquals(DefaultIndexDefinition.ID, fullScanNode.getIndexDefinition());

        assertInstanceOf(ResidualPredicate.class, fullScanNode.predicate());
        ResidualPredicate residualPredicate = (ResidualPredicate) fullScanNode.predicate();
        assertEquals(Operator.GT, residualPredicate.op());
        assertEquals("age", residualPredicate.selector());
        assertInstanceOf(StringVal.class, residualPredicate.operand());
        StringVal stringVal = (StringVal) residualPredicate.operand();
        assertEquals("20", stringVal.value());
    }

    @Test
    void shouldFindDocumentsWithNumberGreaterThanNullViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-number-gt-null";

        IndexDefinition numberIndex = IndexDefinition.create("number-index", "number", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, numberIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': -20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'number': {'$gt': null}}");
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
                "{\"number\": -20, \"name\": \"John\"}",
                "{\"number\": 23, \"name\": \"Alice\"}",
                "{\"number\": 25, \"name\": \"George\"}",
                "{\"number\": 35, \"name\": \"Claire\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindDocumentsWithNumberGreaterThanNullViaFullScan() {
        final String TEST_BUCKET_NAME = "test-bucket-number-gt-null-fullscan";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': -20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'number': {'$gt': null}}");
        assertInstanceOf(FullScanNode.class, plan);
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
                "{\"number\": -20, \"name\": \"John\"}",
                "{\"number\": 23, \"name\": \"Alice\"}",
                "{\"number\": 25, \"name\": \"George\"}",
                "{\"number\": 35, \"name\": \"Claire\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindDocumentsWithNumberGreaterThanOrEqualToNullViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-number-gte-null";

        IndexDefinition numberIndex = IndexDefinition.create("number-index", "number", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, numberIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': -20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'number': {'$gte': null}}");
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
                "{\"name\": \"Frank\"}",
                "{\"number\": null, \"name\": \"Donald\"}",
                "{\"number\": -20, \"name\": \"John\"}",
                "{\"number\": 23, \"name\": \"Alice\"}",
                "{\"number\": 25, \"name\": \"George\"}",
                "{\"number\": 35, \"name\": \"Claire\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindDocumentsWithNumberGreaterThanOrEqualToNullViaFullScan() {
        final String TEST_BUCKET_NAME = "test-bucket-number-gte-null-fullscan";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': -20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'number': {'$gte': null}}");
        assertInstanceOf(FullScanNode.class, plan);
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
                "{\"name\": \"Frank\"}",
                "{\"number\": null, \"name\": \"Donald\"}",
                "{\"number\": -20, \"name\": \"John\"}",
                "{\"number\": 23, \"name\": \"Alice\"}",
                "{\"number\": 25, \"name\": \"George\"}",
                "{\"number\": 35, \"name\": \"Claire\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindDocumentsWithNumberLessThanNullViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-number-lt-null";

        IndexDefinition numberIndex = IndexDefinition.create("number-index", "number", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, numberIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': -20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'number': {'$lt': null}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        assertTrue(actualResult.isEmpty());
    }

    @Test
    void shouldFindDocumentsWithNumberLessThanOrEqualToNullViaIndexScan() {
        final String TEST_BUCKET_NAME = "test-bucket-number-lte-null";

        IndexDefinition numberIndex = IndexDefinition.create("number-index", "number", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, numberIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': -20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'number': {'$lte': null}}");
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
                "{\"name\": \"Frank\"}",
                "{\"number\": null, \"name\": \"Donald\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindDocumentsWithNumberLessThanNullViaFullScan() {
        final String TEST_BUCKET_NAME = "test-bucket-number-lt-null-fullscan";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': -20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'number': {'$lt': null}}");
        assertInstanceOf(FullScanNode.class, plan);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        assertTrue(actualResult.isEmpty());
    }

    @Test
    void shouldFindDocumentsWithNumberLessThanOrEqualToNullViaFullScan() {
        final String TEST_BUCKET_NAME = "test-bucket-number-lte-null-fullscan";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': -20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'number': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'number': {'$lte': null}}");
        assertInstanceOf(FullScanNode.class, plan);
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
                "{\"name\": \"Frank\"}",
                "{\"number\": null, \"name\": \"Donald\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldHandleFullScanNodeWithNullValuesNe() {
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
    void shouldHandleIndexScanNodeWithNullValuesEq() {
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
    void shouldHandleIndexScanNodeWithNullValuesNe() {
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
    void shouldHandleRangeScanNodeWithNullBoundaries() {
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
    void shouldHandleRangeScanNodeNonsenseQuery() {
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
    void shouldReturnEmptyResultForContradiction() {
        final String TEST_BUCKET_NAME = "test-bucket-query-not-existed-field";

        IndexDefinition ageIndex = IndexDefinition.create("name-index", "name", BsonType.STRING);
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

    @Test
    void shouldReturnEmptyResultWhenAllDocumentsMatchNeCondition() {
        final String TEST_BUCKET_NAME = "test-bucket-ne-all-match";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes("{'status': 'ALIVE'}"));
        }

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'status': {'$ne': 'ALIVE'}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void shouldReturnEmptyResultWhenQueryingIdGreaterThanMax() {
        final String TEST_BUCKET_NAME = "test-bucket-id-gt-max";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes("{'status': 'ALIVE'}"));
        }

        List<Versionstamp> versionstamps = insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);
        Versionstamp greatestId = versionstamps.getLast();

        String query = String.format("{'_id': {'$gt': '%s'}}", VersionstampUtil.base32HexEncode(greatestId));
        PipelineNode plan = createExecutionPlan(metadata, query);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void shouldReturnEmptyResultWhenQueryingIdLessThanMin() {
        final String TEST_BUCKET_NAME = "test-bucket-id-lt-min";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes("{'status': 'ALIVE'}"));
        }

        List<Versionstamp> versionstamps = insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);
        Versionstamp smallestId = versionstamps.getFirst();

        String query = String.format("{'_id': {'$lt': '%s'}}", VersionstampUtil.base32HexEncode(smallestId));
        PipelineNode plan = createExecutionPlan(metadata, query);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty());
        }
    }
}
