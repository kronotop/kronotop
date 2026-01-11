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
import com.kronotop.bucket.index.IndexDefinition;
import org.bson.*;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class NonStrictTypesIntegrationTest extends BasePipelineTest {

    @Override
    protected String getConfigFileName() {
        return "test-non-strict-types.conf";
    }

    @Test
    void shouldFindStringValueViaFullScanWhenQueryingInt32IndexWithStringPredicate() {
        final String TEST_BUCKET_NAME = "test-bucket-query-not-existed-field";

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': null, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': '21', 'name': 'Joe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': '20'}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of("{\"age\": \"21\", \"name\": \"Joe\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindInt32ValueViaFullScanWhenQueryingStringIndexWithInt32Predicate() {
        final String TEST_BUCKET_NAME = "test-bucket-int32-predicate";

        IndexDefinition nameIndex = IndexDefinition.create("score-index", "score", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, nameIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 'high', 'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Joe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'low', 'name': 'Alice'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'score': {'$eq': 50}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of("{\"score\": 50, \"name\": \"Joe\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindDoubleValueViaFullScanWhenQueryingInt32IndexWithDoublePredicate() {
        final String TEST_BUCKET_NAME = "test-bucket-double-predicate";

        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 10, 'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 19.99, 'name': 'Joe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 12, 'name': 'Alice'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'price': {'$lt': 20.0}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of("{\"price\": 19.99, \"name\": \"Joe\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindInt64ValueViaFullScanWhenQueryingStringIndexWithInt64Predicate() {
        final String TEST_BUCKET_NAME = "test-bucket-int64-predicate";

        IndexDefinition idIndex = IndexDefinition.create("bigid-index", "bigId", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, idIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'bigId': 'abc123', 'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'bigId': {'$numberLong': '9223372036854775807'}, 'name': 'Joe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'bigId': 'xyz789', 'name': 'Alice'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'bigId': {'$eq': {'$numberLong': '9223372036854775807'}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of("{\"bigId\": 9223372036854775807, \"name\": \"Joe\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindBooleanValueViaFullScanWhenQueryingInt32IndexWithBooleanPredicate() {
        final String TEST_BUCKET_NAME = "test-bucket-boolean-predicate";

        IndexDefinition activeIndex = IndexDefinition.create("active-index", "active", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'active': 1, 'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'active': true, 'name': 'Joe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'active': 0, 'name': 'Alice'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'active': {'$eq': true}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of("{\"active\": true, \"name\": \"Joe\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindDecimal128ValueViaFullScanWhenQueryingInt32IndexWithDecimal128Predicate() {
        final String TEST_BUCKET_NAME = "test-bucket-decimal128-predicate";

        IndexDefinition amountIndex = IndexDefinition.create("amount-index", "amount", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, amountIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'amount': 100, 'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberDecimal': '123.456'}, 'name': 'Joe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': 200, 'name': 'Alice'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'amount': {'$eq': {'$numberDecimal': '123.456'}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of("{\"amount\": {\"$numberDecimal\": \"123.456\"}, \"name\": \"Joe\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindDateTimeValueViaFullScanWhenQueryingStringIndexWithDateTimePredicate() {
        final String TEST_BUCKET_NAME = "test-bucket-datetime-predicate";

        IndexDefinition createdIndex = IndexDefinition.create("created-index", "created", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, createdIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'created': 'yesterday', 'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'created': {'$date': '2024-01-15T10:30:00Z'}, 'name': 'Joe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'created': 'today', 'name': 'Alice'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'created': {'$eq': {'$date': '2024-01-15T10:30:00Z'}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        List<String> expectedResult = List.of("{\"created\": {\"$date\": \"2024-01-15T10:30:00Z\"}, \"name\": \"Joe\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindBinaryValueViaFullScanWhenQueryingStringIndexWithBinaryPredicate() {
        final String TEST_BUCKET_NAME = "test-bucket-binary-predicate";

        IndexDefinition dataIndex = IndexDefinition.create("data-index", "data", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, dataIndex);

        byte[] binaryData = new byte[]{0x01, 0x02, 0x03, 0x04};

        // Insert documents - one with string, one with binary
        BsonDocument docWithString = new BsonDocument()
                .append("data", new BsonString("text"))
                .append("name", new BsonString("Frank"));
        BsonDocument docWithBinary = new BsonDocument()
                .append("data", new BsonBinary(binaryData))
                .append("name", new BsonString("Joe"));
        BsonDocument docWithString2 = new BsonDocument()
                .append("data", new BsonString("other"))
                .append("name", new BsonString("Alice"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(docWithString),
                BSONUtil.toBytes(docWithBinary),
                BSONUtil.toBytes(docWithString2)
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Build query using BSON: {'data': {'$eq': Binary(...)}}
        BsonDocument query = new BsonDocument()
                .append("data", new BsonDocument("$eq", new BsonBinary(binaryData)));

        PipelineNode plan = createExecutionPlan(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Joe\""));
    }

    @Test
    void shouldFindTimestampValueViaFullScanWhenQueryingStringIndexWithTimestampPredicate() {
        final String TEST_BUCKET_NAME = "test-bucket-timestamp-predicate";

        IndexDefinition tsIndex = IndexDefinition.create("ts-index", "ts", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, tsIndex);

        BsonTimestamp timestamp = new BsonTimestamp(1234567890, 1);

        // Insert documents - one with string, one with timestamp
        BsonDocument docWithString = new BsonDocument()
                .append("ts", new BsonString("early"))
                .append("name", new BsonString("Frank"));
        BsonDocument docWithTimestamp = new BsonDocument()
                .append("ts", timestamp)
                .append("name", new BsonString("Joe"));
        BsonDocument docWithString2 = new BsonDocument()
                .append("ts", new BsonString("late"))
                .append("name", new BsonString("Alice"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(docWithString),
                BSONUtil.toBytes(docWithTimestamp),
                BSONUtil.toBytes(docWithString2)
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Build query using BSON: {'ts': {'$eq': Timestamp(...)}}
        BsonDocument query = new BsonDocument()
                .append("ts", new BsonDocument("$eq", timestamp));

        PipelineNode plan = createExecutionPlan(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Joe\""));
    }

    @Test
    void shouldMatchDocumentsWithInOperatorWhenQueryTypesMismatchIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-in-query-type-mismatch";

        // INT32 index, but we query with STRING values
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': '25', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': '30', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query with STRING values - falls back to FullScan due to type mismatch
        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$in': ['25', '30']}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should match only documents where age is STRING "25" or "30"
        List<String> expectedResult = List.of(
                "{\"age\": \"25\", \"name\": \"Bob\"}",
                "{\"age\": \"30\", \"name\": \"Diana\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchPartialValuesWithInOperatorWhenSomeTypesMismatch() {
        final String TEST_BUCKET_NAME = "test-bucket-in-partial-type-match";

        // INT32 index on score field
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        // Documents with mixed types for score field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'fifty', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'hundred', 'name': 'Diana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 75, 'name': 'Eve'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query with INT32 values - should match only INT32 documents
        PipelineNode plan = createExecutionPlan(metadata, "{'score': {'$in': [50, 100]}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should match only documents with INT32 values 50 or 100
        // String values "fifty" and "hundred" are skipped (type mismatch)
        // UnionNode order is an implementation detail, sort for comparison
        Collections.sort(actualResult);
        List<String> expectedResult = List.of(
                "{\"score\": 100, \"name\": \"Charlie\"}",
                "{\"score\": 50, \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldSkipMismatchedTypesInMultikeyArrayWithInOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-in-multikey-mixed-types";

        // INT32 index on values field (multikey for arrays)
        IndexDefinition valuesIndex = IndexDefinition.create("values-index", "values", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valuesIndex);

        // Documents with arrays containing mixed types
        // With strictTypes=false, non-INT32 elements are skipped during indexing
        BsonDocument doc1 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonInt32(1),
                        new BsonInt32(2),
                        new BsonString("three")
                )))
                .append("name", new BsonString("Alice"));

        BsonDocument doc2 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonString("one"),
                        new BsonString("two")
                )))
                .append("name", new BsonString("Bob"));

        BsonDocument doc3 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonInt32(3),
                        new BsonInt32(4)
                )))
                .append("name", new BsonString("Charlie"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3)
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for documents containing 1 or 3 in their values array
        PipelineNode plan = createExecutionPlan(metadata, "{'values': {'$in': [1, 3]}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should match Alice (has 1) and Charlie (has 3)
        // Bob is not matched (only has string values, not indexed)
        // UnionNode order is an implementation detail, sort for comparison
        Collections.sort(actualResult);
        List<String> expectedResult = List.of(
                "{\"values\": [1, 2, \"three\"], \"name\": \"Alice\"}",
                "{\"values\": [3, 4], \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFallbackToFullScanWithMixedTypeInQueryOnMultikeyIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-in-multikey-mixed-query";

        // INT32 index on values field (multikey for arrays)
        IndexDefinition valuesIndex = IndexDefinition.create("values-index", "values", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valuesIndex);

        // Documents with arrays containing mixed types
        BsonDocument doc1 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonInt32(1),
                        new BsonInt32(2),
                        new BsonString("three")
                )))
                .append("name", new BsonString("Alice"));

        BsonDocument doc2 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonString("one"),
                        new BsonString("two")
                )))
                .append("name", new BsonString("Bob"));

        BsonDocument doc3 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonInt32(3),
                        new BsonInt32(4)
                )))
                .append("name", new BsonString("Charlie"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3)
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Mixed-type $in query - contains STRING 'one' which doesn't match INT32 index
        // This causes fallback to FullScan since isOperandTypeMatch fails
        BsonDocument query = new BsonDocument()
                .append("values", new BsonDocument("$in", new BsonArray(List.of(
                        new BsonInt32(1),
                        new BsonInt32(3),
                        new BsonString("one")
                ))));

        PipelineNode plan = createExecutionPlan(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // With FullScan (due to mixed-type query), all matching documents are found:
        // - Alice matched (has INT32 1 in values array)
        // - Bob matched (has STRING "one" in values array)
        // - Charlie matched (has INT32 3 in values array)
        // Results in insertion order (FullScan)
        assertEquals(3, actualResult.size());
        assertTrue(actualResult.get(0).contains("\"name\": \"Alice\""));
        assertTrue(actualResult.get(1).contains("\"name\": \"Bob\""));
        assertTrue(actualResult.get(2).contains("\"name\": \"Charlie\""));
    }

    @Test
    void shouldMatchDocumentsWithNinOperatorWhenQueryTypesMismatchIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-query-type-mismatch";

        // INT32 index, but we query with STRING values
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': '25', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': '30', 'name': 'Diana'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query with STRING values in $nin - falls back to FullScan due to type mismatch
        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$nin': ['25', '30']}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should exclude documents where age is STRING "25" or "30"
        // INT32 values 25 and 30 are NOT excluded (different type)
        List<String> expectedResult = List.of(
                "{\"age\": 25, \"name\": \"Alice\"}",
                "{\"age\": 30, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchDocumentsWithNinOperatorWhenDocumentTypesMismatch() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-doc-type-mismatch";

        // INT32 index on score field
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        // Documents with mixed types for score field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'fifty', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'hundred', 'name': 'Diana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 75, 'name': 'Eve'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query to exclude INT32 values 50 and 100
        PipelineNode plan = createExecutionPlan(metadata, "{'score': {'$nin': [50, 100]}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // With index scan on INT32 index:
        // - Alice (50) excluded - in exclusion list
        // - Charlie (100) excluded - in exclusion list
        // - Eve (75) included - INT32 value not in exclusion list
        // - Bob and Diana have string values, not indexed as INT32
        List<String> expectedResult = List.of(
                "{\"score\": 75, \"name\": \"Eve\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldExcludeMixedTypeArrayValuesWithNinOperatorViaFullScan() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-array-mixed-types";

        // No secondary index on array field - uses FullScan
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Documents with arrays containing mixed types
        BsonDocument doc1 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonInt32(1),
                        new BsonInt32(2),
                        new BsonString("three")
                )))
                .append("name", new BsonString("Alice"));

        BsonDocument doc2 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonString("one"),
                        new BsonString("two")
                )))
                .append("name", new BsonString("Bob"));

        BsonDocument doc3 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonInt32(3),
                        new BsonInt32(4)
                )))
                .append("name", new BsonString("Charlie"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3)
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query to exclude documents containing INT32 values 1 or 2 in their values array
        // Uses FullScan - compares actual array element values
        PipelineNode plan = createExecutionPlan(metadata, "{'values': {'$nin': [1, 2]}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Alice is excluded (has INT32 values 1 and 2 in array)
        // Bob is included (only has STRING values - type mismatch with exclusion list)
        // Charlie is included (has INT32 3 and 4 - not in exclusion list)
        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Bob\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Charlie\"")));
    }

    @Test
    void shouldExcludeMixedTypeArrayValuesWithMixedTypeNinQueryViaFullScan() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-array-mixed-query";

        // No secondary index on array field - uses FullScan
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Documents with arrays containing mixed types
        BsonDocument doc1 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonInt32(1),
                        new BsonInt32(2),
                        new BsonString("three")
                )))
                .append("name", new BsonString("Alice"));

        BsonDocument doc2 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonString("one"),
                        new BsonString("two")
                )))
                .append("name", new BsonString("Bob"));

        BsonDocument doc3 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonInt32(3),
                        new BsonInt32(4)
                )))
                .append("name", new BsonString("Charlie"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3)
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Mixed-type $nin query: exclude documents containing 1, 2 (INT32) or "one" (STRING)
        BsonDocument query = new BsonDocument()
                .append("values", new BsonDocument("$nin", new BsonArray(List.of(
                        new BsonInt32(1),
                        new BsonInt32(2),
                        new BsonString("one")
                ))));

        PipelineNode plan = createExecutionPlan(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Alice is excluded (has INT32 values 1 and 2 in exclusion list)
        // Bob is excluded (has STRING "one" in exclusion list)
        // Charlie is included (has 3 and 4 - none in exclusion list)
        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Charlie\""));
    }

    @Test
    void shouldFallbackToFullScanWithMixedTypeNinQueryOnTypedIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-nin-mixed-type-query";

        // INT32 index on score field
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        // Documents with mixed types for score field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'fifty', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'hundred', 'name': 'Diana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 75, 'name': 'Eve'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Mixed-type $nin query - contains STRING 'fifty' which doesn't match INT32 index
        // This causes fallback to FullScan since isOperandTypeMatch fails
        BsonDocument query = new BsonDocument()
                .append("score", new BsonDocument("$nin", new BsonArray(List.of(
                        new BsonInt32(50),
                        new BsonInt32(100),
                        new BsonString("fifty")
                ))));

        PipelineNode plan = createExecutionPlan(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // With FullScan (due to mixed-type query):
        // - Alice (50) excluded - matches INT32 50 in exclusion list
        // - Bob ('fifty') excluded - matches STRING 'fifty' in exclusion list
        // - Charlie (100) excluded - matches INT32 100 in exclusion list
        // - Diana ('hundred') included - not in exclusion list
        // - Eve (75) included - not in exclusion list
        // Results in insertion order (FullScan)
        List<String> expectedResult = List.of(
                "{\"score\": \"hundred\", \"name\": \"Diana\"}",
                "{\"score\": 75, \"name\": \"Eve\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    // ==================== $elemMatch with Non-Strict Types ====================

    @Test
    void shouldMatchElemMatchOnDocArrayWhenQueryTypeMismatchesIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-query-type-mismatch";

        // INT32 index on items.price (multikey), but we query with STRING predicate
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "items.price", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'sku': 'A1', 'price': 100}, {'sku': 'A2', 'price': 200}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'sku': 'B1', 'price': '150'}, {'sku': 'B2', 'price': '250'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'sku': 'C1', 'price': 300}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query with STRING predicate - falls back to FullScan due to type mismatch
        PipelineNode plan = createExecutionPlan(metadata, "{'items': {'$elemMatch': {'price': {'$gt': '100'}}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should match Order2 (STRING prices '150' > '100' and '250' > '100')
        // INT32 prices are not matched due to type mismatch
        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Order2\""));
    }

    @Test
    void shouldMatchElemMatchOnDocArrayWithMixedTypeDocuments() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-mixed-type-docs";

        // INT32 index on items.price (multikey)
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "items.price", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        // Documents with mixed INT32/STRING prices
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'sku': 'A1', 'price': 100}, {'sku': 'A2', 'price': 200}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'sku': 'B1', 'price': '150'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'sku': 'C1', 'price': 300}, {'sku': 'C2', 'price': '400'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'sku': 'D1', 'price': 50}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query with INT32 predicate - uses index, matches only INT32 prices
        PipelineNode plan = createExecutionPlan(metadata, "{'items': {'$elemMatch': {'price': {'$gt': 150}}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should match:
        // - Order1: has price 200 > 150 (INT32) ✓
        // - Order2: price '150' is STRING, not matched
        // - Order3: has price 300 > 150 (INT32) ✓
        // - Order4: price 50 < 150
        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Order1\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Order3\"")));
    }

    @Test
    void shouldMatchScalarElemMatchWhenQueryTypeMismatchesIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-scalar-elemmatch-type-mismatch";

        // INT32 index on scores array (multikey)
        IndexDefinition scoresIndex = IndexDefinition.create("scores-index", "scores", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoresIndex);

        // Documents with INT32 and STRING scores
        BsonDocument doc1 = new BsonDocument()
                .append("scores", new BsonArray(List.of(new BsonInt32(85), new BsonInt32(90))))
                .append("name", new BsonString("Alice"));
        BsonDocument doc2 = new BsonDocument()
                .append("scores", new BsonArray(List.of(new BsonString("75"), new BsonString("80"))))
                .append("name", new BsonString("Bob"));
        BsonDocument doc3 = new BsonDocument()
                .append("scores", new BsonArray(List.of(new BsonInt32(95))))
                .append("name", new BsonString("Charlie"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3)
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query with STRING predicate - falls back to FullScan
        PipelineNode plan = createExecutionPlan(metadata, "{'scores': {'$elemMatch': {'$gt': '70'}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should match Bob (STRING scores '75' > '70' and '80' > '70')
        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Bob\""));
    }

    @Test
    void shouldMatchScalarElemMatchWithMixedTypeArrayElements() {
        final String TEST_BUCKET_NAME = "test-bucket-scalar-elemmatch-mixed-elements";

        // INT32 index on values array (multikey)
        IndexDefinition valuesIndex = IndexDefinition.create("values-index", "values", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valuesIndex);

        // Documents with arrays containing mixed INT32/STRING elements
        BsonDocument doc1 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonInt32(10),
                        new BsonString("twenty"),
                        new BsonInt32(30)
                )))
                .append("name", new BsonString("Alice"));
        BsonDocument doc2 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonString("five"),
                        new BsonString("fifteen")
                )))
                .append("name", new BsonString("Bob"));
        BsonDocument doc3 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonInt32(50),
                        new BsonInt32(60)
                )))
                .append("name", new BsonString("Charlie"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3)
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query with INT32 predicate - uses index on INT32 values
        PipelineNode plan = createExecutionPlan(metadata, "{'values': {'$elemMatch': {'$gte': 25}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should match:
        // - Alice: has INT32 30 >= 25 ✓
        // - Bob: only STRING values, not indexed
        // - Charlie: has INT32 50, 60 >= 25 ✓
        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Alice\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Charlie\"")));
    }

    @Test
    void shouldFallbackToFullScanWithMixedTypeInInsideElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-in-mixed-types";

        // INT32 index on items.qty (multikey)
        IndexDefinition qtyIndex = IndexDefinition.create("qty-index", "items.qty", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, qtyIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'sku': 'A1', 'qty': 5}, {'sku': 'A2', 'qty': 10}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'sku': 'B1', 'qty': 'five'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'sku': 'C1', 'qty': 15}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Mixed-type $in inside $elemMatch - causes fallback to FullScan
        BsonDocument query = new BsonDocument()
                .append("items", new BsonDocument("$elemMatch",
                        new BsonDocument("qty", new BsonDocument("$in", new BsonArray(List.of(
                                new BsonInt32(5),
                                new BsonInt32(15),
                                new BsonString("five")
                        ))))));

        PipelineNode plan = createExecutionPlan(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // With FullScan, all matching documents found:
        // - Order1: has qty 5 (INT32) ✓
        // - Order2: has qty 'five' (STRING) ✓
        // - Order3: has qty 15 (INT32) ✓
        assertEquals(3, actualResult.size());
    }

    @Test
    void shouldFallbackToFullScanWithMixedTypeNinInsideElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-nin-mixed-types";

        // INT32 index on items.qty (multikey)
        IndexDefinition qtyIndex = IndexDefinition.create("qty-index", "items.qty", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, qtyIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'sku': 'A1', 'qty': 5}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'sku': 'B1', 'qty': 'ten'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'sku': 'C1', 'qty': 15}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'sku': 'D1', 'qty': 20}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Mixed-type $nin inside $elemMatch - causes fallback to FullScan
        BsonDocument query = new BsonDocument()
                .append("items", new BsonDocument("$elemMatch",
                        new BsonDocument("qty", new BsonDocument("$nin", new BsonArray(List.of(
                                new BsonInt32(5),
                                new BsonString("ten")
                        ))))));

        PipelineNode plan = createExecutionPlan(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // With FullScan, excludes documents matching $nin:
        // - Order1: qty 5 excluded
        // - Order2: qty 'ten' excluded
        // - Order3: qty 15 included ✓
        // - Order4: qty 20 included ✓
        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Order3\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Order4\"")));
    }

    @Test
    void shouldCombineTypeMismatchedIndexWithElemMatch() {
        final String TEST_BUCKET_NAME = "test-bucket-combined-type-mismatch";

        // INT32 index on status field
        IndexDefinition statusIndex = IndexDefinition.create("status-index", "status", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, statusIndex);

        // Documents with mixed status types and items arrays
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'status': 1, 'items': [{'price': 100}, {'price': 200}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'status': 'active', 'items': [{'price': 150}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'status': 1, 'items': [{'price': 50}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'status': 'active', 'items': [{'price': 300}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query with STRING status (type mismatch) + $elemMatch on items
        PipelineNode plan = createExecutionPlan(metadata, "{'status': 'active', 'items': {'$elemMatch': {'price': {'$gt': 100}}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Falls back to FullScan due to type mismatch on status
        // - Order1: status INT32 1 != 'active'
        // - Order2: status 'active' ✓, price 150 > 100 ✓
        // - Order3: status INT32 1 != 'active'
        // - Order4: status 'active' ✓, price 300 > 100 ✓
        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Order2\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Order4\"")));
    }

    @Test
    void shouldMatchNestedElemMatchWithTypeMismatchAtInnerLevel() {
        final String TEST_BUCKET_NAME = "test-bucket-nested-elemmatch-type-mismatch";

        // Index on outer department name (multikey)
        IndexDefinition nameIndex = IndexDefinition.create("dept-name-index", "departments.name", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, nameIndex);

        // Documents with nested arrays, inner prices have mixed types
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store1', 'departments': [{'name': 'Electronics', 'products': [{'price': 500}, {'price': 800}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store2', 'departments': [{'name': 'Electronics', 'products': [{'price': '600'}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store3', 'departments': [{'name': 'Electronics', 'products': [{'price': 300}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store4', 'departments': [{'name': 'Clothing', 'products': [{'price': 900}]}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Nested $elemMatch with STRING price predicate at inner level
        String query = "{'departments': {'$elemMatch': {'name': 'Electronics', 'products': {'$elemMatch': {'price': {'$gt': '500'}}}}}}";
        PipelineNode plan = createExecutionPlan(metadata, query);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Outer condition uses index (name='Electronics')
        // Inner $elemMatch with STRING predicate matches only STRING prices
        // - Store1: Electronics, prices INT32 500, 800 - not matched (type mismatch)
        // - Store2: Electronics, price STRING '600' > '500' ✓
        // - Store3: Electronics, price INT32 300 - not matched (type mismatch)
        // - Store4: Clothing - filtered by outer condition
        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Store2\""));
    }

    @Test
    void shouldHandleElemMatchWithNullInMixedTypeArray() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-null-mixed";

        // INT32 index on values array (multikey)
        IndexDefinition valuesIndex = IndexDefinition.create("values-index", "values", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valuesIndex);

        // Documents with arrays containing nulls and mixed types
        BsonDocument doc1 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonInt32(10),
                        BsonNull.VALUE,
                        new BsonInt32(30)
                )))
                .append("name", new BsonString("Alice"));
        BsonDocument doc2 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        BsonNull.VALUE,
                        new BsonString("twenty")
                )))
                .append("name", new BsonString("Bob"));
        BsonDocument doc3 = new BsonDocument()
                .append("values", new BsonArray(List.of(
                        new BsonInt32(50),
                        new BsonInt32(60)
                )))
                .append("name", new BsonString("Charlie"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3)
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for INT32 values >= 25
        PipelineNode plan = createExecutionPlan(metadata, "{'values': {'$elemMatch': {'$gte': 25}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Should match:
        // - Alice: has INT32 30 >= 25 ✓ (null is skipped)
        // - Bob: only null and STRING, no INT32 values
        // - Charlie: has INT32 50, 60 >= 25 ✓
        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Alice\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Charlie\"")));
    }

    @Test
    void shouldMatchElemMatchWhenSomeArrayElementsHaveWrongType() {
        final String TEST_BUCKET_NAME = "test-bucket-elemmatch-partial-type-match";

        // INT32 index on items.quantity (multikey)
        IndexDefinition qtyIndex = IndexDefinition.create("qty-index", "items.quantity", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, qtyIndex);

        // Each document's items array has elements with mixed types for quantity
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'sku': 'A1', 'quantity': 5}, {'sku': 'A2', 'quantity': 'ten'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'sku': 'B1', 'quantity': 'five'}, {'sku': 'B2', 'quantity': 'fifteen'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'sku': 'C1', 'quantity': 20}, {'sku': 'C2', 'quantity': 'thirty'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'sku': 'D1', 'quantity': 3}]}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for items with INT32 quantity > 10
        PipelineNode plan = createExecutionPlan(metadata, "{'items': {'$elemMatch': {'quantity': {'$gt': 10}}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Index only contains INT32 quantities
        // - Order1: qty 5 (INT32) not > 10, qty 'ten' (STRING) not indexed
        // - Order2: only STRING quantities, not indexed
        // - Order3: qty 20 (INT32) > 10 ✓
        // - Order4: qty 3 (INT32) not > 10
        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Order3\""));
    }

    @Test
    void shouldHandleElemMatchOnArrayWithMixedScalarsAndDocuments() {
        // Array containing both scalars AND documents: [10, {value: 20}, 30]
        final String TEST_BUCKET_NAME = "test-bucket-mixed-scalars-documents";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Documents with arrays containing both scalars and embedded documents
        BsonDocument doc1 = new BsonDocument()
                .append("data", new BsonArray(List.of(
                        new BsonInt32(10),
                        new BsonDocument("value", new BsonInt32(20)),
                        new BsonInt32(30)
                )))
                .append("name", new BsonString("Mixed1"));

        BsonDocument doc2 = new BsonDocument()
                .append("data", new BsonArray(List.of(
                        new BsonDocument("value", new BsonInt32(50)),
                        new BsonInt32(60),
                        new BsonDocument("value", new BsonInt32(70))
                )))
                .append("name", new BsonString("Mixed2"));

        BsonDocument doc3 = new BsonDocument()
                .append("data", new BsonArray(List.of(
                        new BsonInt32(100),
                        new BsonInt32(200)
                )))
                .append("name", new BsonString("OnlyScalars"));

        BsonDocument doc4 = new BsonDocument()
                .append("data", new BsonArray(List.of(
                        new BsonDocument("value", new BsonInt32(5)),
                        new BsonDocument("value", new BsonInt32(15))
                )))
                .append("name", new BsonString("OnlyDocuments"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3),
                BSONUtil.toBytes(doc4)
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Test 1: $elemMatch with scalar predicate - should match scalar elements
        PipelineNode plan1 = createExecutionPlan(metadata, "{'data': {'$elemMatch': {'$gte': 25}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx1 = new QueryContext(metadata, config, plan1);

        List<String> result1 = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx1);
            for (ByteBuffer buffer : results.values()) {
                result1.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Mixed1: has scalar 30 >= 25 ✓
        // Mixed2: has scalar 60 >= 25 ✓
        // OnlyScalars: has 100, 200 >= 25 ✓
        // OnlyDocuments: no scalars
        assertEquals(3, result1.size());
        assertTrue(result1.stream().anyMatch(r -> r.contains("\"name\": \"Mixed1\"")));
        assertTrue(result1.stream().anyMatch(r -> r.contains("\"name\": \"Mixed2\"")));
        assertTrue(result1.stream().anyMatch(r -> r.contains("\"name\": \"OnlyScalars\"")));

        // Test 2: $elemMatch with document field predicate - should match document elements
        PipelineNode plan2 = createExecutionPlan(metadata, "{'data': {'$elemMatch': {'value': {'$gte': 50}}}}");
        QueryContext ctx2 = new QueryContext(metadata, config, plan2);

        List<String> result2 = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx2);
            for (ByteBuffer buffer : results.values()) {
                result2.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Mixed1: doc has value=20 < 50
        // Mixed2: docs have value=50 and value=70 >= 50 ✓
        // OnlyScalars: no documents
        // OnlyDocuments: docs have value=5, value=15 < 50
        assertEquals(1, result2.size());
        assertTrue(result2.getFirst().contains("\"name\": \"Mixed2\""));
    }

    @Test
    void shouldHandleElemMatchOnArrayWithMixedScalarsAndDocumentsWithIndex() {
        // Array containing both scalars AND documents with INT32 index on scalar values
        final String TEST_BUCKET_NAME = "test-bucket-mixed-scalars-documents-indexed";

        // Create INT32 multiKey index on 'data' array for scalar values
        IndexDefinition dataIndex = IndexDefinition.create("data-index", "data", BsonType.INT32, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, dataIndex);

        // Documents with arrays containing both scalars and embedded documents
        BsonDocument doc1 = new BsonDocument()
                .append("data", new BsonArray(List.of(
                        new BsonInt32(10),
                        new BsonDocument("value", new BsonInt32(20)),
                        new BsonInt32(30)
                )))
                .append("name", new BsonString("Mixed1"));

        BsonDocument doc2 = new BsonDocument()
                .append("data", new BsonArray(List.of(
                        new BsonDocument("value", new BsonInt32(50)),
                        new BsonInt32(60),
                        new BsonDocument("value", new BsonInt32(70))
                )))
                .append("name", new BsonString("Mixed2"));

        BsonDocument doc3 = new BsonDocument()
                .append("data", new BsonArray(List.of(
                        new BsonInt32(100),
                        new BsonInt32(200)
                )))
                .append("name", new BsonString("OnlyScalars"));

        BsonDocument doc4 = new BsonDocument()
                .append("data", new BsonArray(List.of(
                        new BsonDocument("value", new BsonInt32(5)),
                        new BsonDocument("value", new BsonInt32(15))
                )))
                .append("name", new BsonString("OnlyDocuments"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3),
                BSONUtil.toBytes(doc4)
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Test 1: $elemMatch with scalar predicate - should use INT32 index on scalar elements
        PipelineNode plan1 = createExecutionPlan(metadata, "{'data': {'$elemMatch': {'$gte': 25}}}");

        // Verify plan uses IndexScanNode with the multiKey index
        assertInstanceOf(IndexScanNode.class, plan1, "Root should be IndexScanNode for multiKey indexed $elemMatch");
        IndexScanNode indexScanNode = (IndexScanNode) plan1;
        assertEquals(dataIndex.id(), indexScanNode.getIndexDefinition().id(), "Should use the multiKey data index");
        assertTrue(indexScanNode.getIndexDefinition().multiKey(), "Index should be marked as multiKey");

        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx1 = new QueryContext(metadata, config, plan1);

        List<String> result1 = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx1);
            for (ByteBuffer buffer : results.values()) {
                result1.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Index only contains INT32 scalars, documents in array are ignored
        // Mixed1: has scalar 30 >= 25 ✓
        // Mixed2: has scalar 60 >= 25 ✓
        // OnlyScalars: has 100, 200 >= 25 ✓
        // OnlyDocuments: no INT32 scalars indexed
        assertEquals(3, result1.size());
        assertTrue(result1.stream().anyMatch(r -> r.contains("\"name\": \"Mixed1\"")));
        assertTrue(result1.stream().anyMatch(r -> r.contains("\"name\": \"Mixed2\"")));
        assertTrue(result1.stream().anyMatch(r -> r.contains("\"name\": \"OnlyScalars\"")));

        // Test 2: $elemMatch with document field predicate - should match document elements
        // No index on 'data.value', so this will use FullScanNode
        PipelineNode plan2 = createExecutionPlan(metadata, "{'data': {'$elemMatch': {'value': {'$gte': 50}}}}");
        assertInstanceOf(FullScanNode.class, plan2, "Should use FullScanNode when querying unindexed nested field");

        QueryContext ctx2 = new QueryContext(metadata, config, plan2);

        List<String> result2 = new ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx2);
            for (ByteBuffer buffer : results.values()) {
                result2.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
        }

        // Mixed1: doc has value=20 < 50
        // Mixed2: docs have value=50 and value=70 >= 50 ✓
        // OnlyScalars: no documents
        // OnlyDocuments: docs have value=5, value=15 < 50
        assertEquals(1, result2.size());
        assertTrue(result2.getFirst().contains("\"name\": \"Mixed2\""));
    }
}
