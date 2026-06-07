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
import com.kronotop.bucket.handlers.protocol.SortDirection;
import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import org.bson.*;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NonStrictTypesIntegrationTest extends BasePipelineTest {

    @Override
    protected String getConfigFileName() {
        return "test-non-strict-types.conf";
    }

    @Test
    void shouldFindStringValueViaFullScanWhenQueryingInt32IndexWithStringPredicate() {
        final String TEST_BUCKET_NAME = "test-bucket-query-not-existed-field";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$gt': '20'}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of("{\"age\": \"21\", \"name\": \"Joe\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindInt32ValueViaFullScanWhenQueryingStringIndexWithInt32Predicate() {
        final String TEST_BUCKET_NAME = "test-bucket-int32-predicate";

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, nameIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 'high', 'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Joe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'low', 'name': 'Alice'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'score': {'$eq': 50}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of("{\"score\": 50, \"name\": \"Joe\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindAllNumericValuesViaFullScanWhenQueryingInt32IndexWithDoublePredicate() {
        // Behavior: DOUBLE predicate on INT32 index falls back to full scan. Numeric widening
        // in PredicateEvaluator allows INT32 values to be compared against DOUBLE predicates.
        final String TEST_BUCKET_NAME = "test-bucket-double-predicate";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 10, 'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 19.99, 'name': 'Joe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 12, 'name': 'Alice'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'price': {'$lt': 20.0}}");
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
                "{\"price\": 10, \"name\": \"Frank\"}",
                "{\"price\": 19.99, \"name\": \"Joe\"}",
                "{\"price\": 12, \"name\": \"Alice\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindInt64ValueViaFullScanWhenQueryingStringIndexWithInt64Predicate() {
        final String TEST_BUCKET_NAME = "test-bucket-int64-predicate";

        SingleFieldIndexDefinition idIndex = SingleFieldIndexDefinition.create("bigid-index", "bigId", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, idIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'bigId': 'abc123', 'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'bigId': {'$numberLong': '9223372036854775807'}, 'name': 'Joe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'bigId': 'xyz789', 'name': 'Alice'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'bigId': {'$eq': {'$numberLong': '9223372036854775807'}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of("{\"bigId\": 9223372036854775807, \"name\": \"Joe\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindBooleanValueViaFullScanWhenQueryingInt32IndexWithBooleanPredicate() {
        final String TEST_BUCKET_NAME = "test-bucket-boolean-predicate";

        SingleFieldIndexDefinition activeIndex = SingleFieldIndexDefinition.create("active-index", "active", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, activeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'active': 1, 'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'active': true, 'name': 'Joe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'active': 0, 'name': 'Alice'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'active': {'$eq': true}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of("{\"active\": true, \"name\": \"Joe\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindDecimal128ValueViaFullScanWhenQueryingInt32IndexWithDecimal128Predicate() {
        final String TEST_BUCKET_NAME = "test-bucket-decimal128-predicate";

        SingleFieldIndexDefinition amountIndex = SingleFieldIndexDefinition.create("amount-index", "amount", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, amountIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'amount': 100, 'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberDecimal': '123.456'}, 'name': 'Joe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': 200, 'name': 'Alice'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'amount': {'$eq': {'$numberDecimal': '123.456'}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of("{\"amount\": {\"$numberDecimal\": \"123.456\"}, \"name\": \"Joe\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindDateTimeValueViaFullScanWhenQueryingStringIndexWithDateTimePredicate() {
        final String TEST_BUCKET_NAME = "test-bucket-datetime-predicate";

        SingleFieldIndexDefinition createdIndex = SingleFieldIndexDefinition.create("created-index", "created", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, createdIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'created': 'yesterday', 'name': 'Frank'}"),
                BSONUtil.jsonToDocumentThenBytes("{'created': {'$date': '2024-01-15T10:30:00Z'}, 'name': 'Joe'}"),
                BSONUtil.jsonToDocumentThenBytes("{'created': 'today', 'name': 'Alice'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'created': {'$eq': {'$date': '2024-01-15T10:30:00Z'}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of("{\"created\": {\"$date\": \"2024-01-15T10:30:00Z\"}, \"name\": \"Joe\"}");
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindBinaryValueViaFullScanWhenQueryingStringIndexWithBinaryPredicate() {
        final String TEST_BUCKET_NAME = "test-bucket-binary-predicate";

        SingleFieldIndexDefinition dataIndex = SingleFieldIndexDefinition.create("data-index", "data", BsonType.STRING, false, IndexStatus.WAITING);
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Build query using BSON: {'data': {'$eq': Binary(...)}}
        BsonDocument query = new BsonDocument()
                .append("data", new BsonDocument("$eq", new BsonBinary(binaryData)));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Joe\""));
    }

    @Test
    void shouldFindTimestampValueViaFullScanWhenQueryingStringIndexWithTimestampPredicate() {
        final String TEST_BUCKET_NAME = "test-bucket-timestamp-predicate";

        SingleFieldIndexDefinition tsIndex = SingleFieldIndexDefinition.create("ts-index", "ts", BsonType.STRING, false, IndexStatus.WAITING);
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Build query using BSON: {'ts': {'$eq': Timestamp(...)}}
        BsonDocument query = new BsonDocument()
                .append("ts", new BsonDocument("$eq", timestamp));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Joe\""));
    }

    @Test
    void shouldMatchDocumentsWithInOperatorWhenQueryTypesMismatchIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-in-query-type-mismatch";

        // INT32 index, but we query with STRING values
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': '25', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': '30', 'name': 'Diana'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query with STRING values - falls back to FullScan due to type mismatch
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$in': ['25', '30']}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        // Documents with mixed types for score field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'fifty', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'hundred', 'name': 'Diana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 75, 'name': 'Eve'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query with INT32 values - should match only INT32 documents
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'score': {'$in': [50, 100]}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition valuesIndex = SingleFieldIndexDefinition.create("values-index", "values", BsonType.INT32, true, IndexStatus.WAITING);
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query for documents containing 1 or 3 in their values array
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'values': {'$in': [1, 3]}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition valuesIndex = SingleFieldIndexDefinition.create("values-index", "values", BsonType.INT32, true, IndexStatus.WAITING);
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Mixed-type $in query - contains STRING 'one' which doesn't match INT32 index
        // This causes fallback to FullScan since isOperandTypeMatch fails
        BsonDocument query = new BsonDocument()
                .append("values", new BsonDocument("$in", new BsonArray(List.of(
                        new BsonInt32(1),
                        new BsonInt32(3),
                        new BsonString("one")
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': '25', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': '30', 'name': 'Diana'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query with STRING values in $nin - falls back to FullScan due to type mismatch
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$nin': ['25', '30']}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        // Documents with mixed types for score field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'fifty', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'hundred', 'name': 'Diana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 75, 'name': 'Eve'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query to exclude INT32 values 50 and 100
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'score': {'$nin': [50, 100]}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query to exclude documents containing INT32 values 1 or 2 in their values array
        // Uses FullScan - compares actual array element values
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'values': {'$nin': [1, 2]}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Mixed-type $nin query: exclude documents containing 1, 2 (INT32) or "one" (STRING)
        BsonDocument query = new BsonDocument()
                .append("values", new BsonDocument("$nin", new BsonArray(List.of(
                        new BsonInt32(1),
                        new BsonInt32(2),
                        new BsonString("one")
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        // Documents with mixed types for score field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'fifty', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 'hundred', 'name': 'Diana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 75, 'name': 'Eve'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Mixed-type $nin query - contains STRING 'fifty' which doesn't match INT32 index
        // This causes fallback to FullScan since isOperandTypeMatch fails
        BsonDocument query = new BsonDocument()
                .append("score", new BsonDocument("$nin", new BsonArray(List.of(
                        new BsonInt32(50),
                        new BsonInt32(100),
                        new BsonString("fifty")
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "items.price", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'sku': 'A1', 'price': 100}, {'sku': 'A2', 'price': 200}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'sku': 'B1', 'price': '150'}, {'sku': 'B2', 'price': '250'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'sku': 'C1', 'price': 300}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query with STRING predicate - falls back to FullScan due to type mismatch
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'items': {'$elemMatch': {'price': {'$gt': '100'}}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "items.price", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        // Documents with mixed INT32/STRING prices
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'sku': 'A1', 'price': 100}, {'sku': 'A2', 'price': 200}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'sku': 'B1', 'price': '150'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'sku': 'C1', 'price': 300}, {'sku': 'C2', 'price': '400'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'sku': 'D1', 'price': 50}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query with INT32 predicate - uses index, matches only INT32 prices
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'items': {'$elemMatch': {'price': {'$gt': 150}}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition scoresIndex = SingleFieldIndexDefinition.create("scores-index", "scores", BsonType.INT32, true, IndexStatus.WAITING);
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query with STRING predicate - falls back to FullScan
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'scores': {'$elemMatch': {'$gt': '70'}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition valuesIndex = SingleFieldIndexDefinition.create("values-index", "values", BsonType.INT32, true, IndexStatus.WAITING);
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query with INT32 predicate - uses index on INT32 values
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'values': {'$elemMatch': {'$gte': 25}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition qtyIndex = SingleFieldIndexDefinition.create("qty-index", "items.qty", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, qtyIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'sku': 'A1', 'qty': 5}, {'sku': 'A2', 'qty': 10}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'sku': 'B1', 'qty': 'five'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'sku': 'C1', 'qty': 15}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Mixed-type $in inside $elemMatch - causes fallback to FullScan
        BsonDocument query = new BsonDocument()
                .append("items", new BsonDocument("$elemMatch",
                        new BsonDocument("qty", new BsonDocument("$in", new BsonArray(List.of(
                                new BsonInt32(5),
                                new BsonInt32(15),
                                new BsonString("five")
                        ))))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition qtyIndex = SingleFieldIndexDefinition.create("qty-index", "items.qty", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, qtyIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'sku': 'A1', 'qty': 5}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'sku': 'B1', 'qty': 'ten'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'sku': 'C1', 'qty': 15}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'sku': 'D1', 'qty': 20}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Mixed-type $nin inside $elemMatch - causes fallback to FullScan
        BsonDocument query = new BsonDocument()
                .append("items", new BsonDocument("$elemMatch",
                        new BsonDocument("qty", new BsonDocument("$nin", new BsonArray(List.of(
                                new BsonInt32(5),
                                new BsonString("ten")
                        ))))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-index", "status", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, statusIndex);

        // Documents with mixed status types and items arrays
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'status': 1, 'items': [{'price': 100}, {'price': 200}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'status': 'active', 'items': [{'price': 150}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'status': 1, 'items': [{'price': 50}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'status': 'active', 'items': [{'price': 300}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query with STRING status (type mismatch) + $elemMatch on items
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': 'active', 'items': {'$elemMatch': {'price': {'$gt': 100}}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("dept-name-index", "departments.name", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, nameIndex);

        // Documents with nested arrays, inner prices have mixed types
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store1', 'departments': [{'name': 'Electronics', 'products': [{'price': 500}, {'price': 800}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store2', 'departments': [{'name': 'Electronics', 'products': [{'price': '600'}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store3', 'departments': [{'name': 'Electronics', 'products': [{'price': 300}]}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Store4', 'departments': [{'name': 'Clothing', 'products': [{'price': 900}]}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Nested $elemMatch with STRING price predicate at inner level
        String query = "{'departments': {'$elemMatch': {'name': 'Electronics', 'products': {'$elemMatch': {'price': {'$gt': '500'}}}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition valuesIndex = SingleFieldIndexDefinition.create("values-index", "values", BsonType.INT32, true, IndexStatus.WAITING);
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query for INT32 values >= 25
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'values': {'$elemMatch': {'$gte': 25}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition qtyIndex = SingleFieldIndexDefinition.create("qty-index", "items.quantity", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, qtyIndex);

        // Each document's items array has elements with mixed types for quantity
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order1', 'items': [{'sku': 'A1', 'quantity': 5}, {'sku': 'A2', 'quantity': 'ten'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order2', 'items': [{'sku': 'B1', 'quantity': 'five'}, {'sku': 'B2', 'quantity': 'fifteen'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order3', 'items': [{'sku': 'C1', 'quantity': 20}, {'sku': 'C2', 'quantity': 'thirty'}]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Order4', 'items': [{'sku': 'D1', 'quantity': 3}]}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query for items with INT32 quantity > 10
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'items': {'$elemMatch': {'quantity': {'$gt': 10}}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Test 1: $elemMatch with scalar predicate - should match scalar elements
        PlanWithParams planWithParams1 = createPlanWithParams(metadata, "{'data': {'$elemMatch': {'$gte': 25}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx1 = new QueryContext(getSession(), metadata, config, planWithParams1.plan(), planWithParams1.parameters());

        List<String> result1 = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx1);
            for (ByteBuffer buffer : results) {
                result1.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        PlanWithParams planWithParams2 = createPlanWithParams(metadata, "{'data': {'$elemMatch': {'value': {'$gte': 50}}}}");
        QueryContext ctx2 = new QueryContext(getSession(), metadata, config, planWithParams2.plan(), planWithParams2.parameters());

        List<String> result2 = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx2);
            for (ByteBuffer buffer : results) {
                result2.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        SingleFieldIndexDefinition dataIndex = SingleFieldIndexDefinition.create("data-index", "data", BsonType.INT32, true, IndexStatus.WAITING);
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

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Test 1: $elemMatch with scalar predicate - should use INT32 index on scalar elements
        PlanWithParams planWithParams1 = createPlanWithParams(metadata, "{'data': {'$elemMatch': {'$gte': 25}}}");

        // Verify plan uses IndexScanNode with the multiKey index
        assertInstanceOf(IndexScanNode.class, planWithParams1.plan(), "Root should be IndexScanNode for multiKey indexed $elemMatch");
        IndexScanNode indexScanNode = (IndexScanNode) planWithParams1.plan();
        assertEquals(dataIndex.id(), indexScanNode.getIndexDefinition().id(), "Should use the multiKey data index");
        assertTrue(indexScanNode.getIndexDefinition().multiKey(), "Index should be marked as multiKey");

        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx1 = new QueryContext(getSession(), metadata, config, planWithParams1.plan(), planWithParams1.parameters());

        List<String> result1 = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx1);
            for (ByteBuffer buffer : results) {
                result1.add(TestUtil.bsonToJsonWithoutId(buffer));
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
        PlanWithParams planWithParams2 = createPlanWithParams(metadata, "{'data': {'$elemMatch': {'value': {'$gte': 50}}}}");
        assertInstanceOf(FullScanNode.class, planWithParams2.plan(), "Should use FullScanNode when querying unindexed nested field");

        QueryContext ctx2 = new QueryContext(getSession(), metadata, config, planWithParams2.plan(), planWithParams2.parameters());

        List<String> result2 = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx2);
            for (ByteBuffer buffer : results) {
                result2.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        // Mixed1: doc has value=20 < 50
        // Mixed2: docs have value=50 and value=70 >= 50 ✓
        // OnlyScalars: no documents
        // OnlyDocuments: docs have value=5, value=15 < 50
        assertEquals(1, result2.size());
        assertTrue(result2.getFirst().contains("\"name\": \"Mixed2\""));
    }

    // ==================== Numeric Widening: INT32 vs INT64 Range Operators ====================

    @Test
    void shouldFindInt32ValuesViaFullScanWhenQueryingInt32IndexWithInt64GtPredicate() {
        // Behavior: INT64 $gt predicate on INT32 index falls back to full scan. Numeric widening
        // promotes INT32 values to INT64 for comparison.
        final String TEST_BUCKET_NAME = "test-bucket-int64-gt-predicate";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 200, 'name': 'Diana'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'score': {'$gt': {'$numberLong': '100'}}}");

        List<String> expectedResult = List.of(
                "{\"score\": 150, \"name\": \"Charlie\"}",
                "{\"score\": 200, \"name\": \"Diana\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindInt32ValuesViaFullScanWhenQueryingInt32IndexWithInt64GtePredicate() {
        // Behavior: INT64 $gte predicate on INT32 index falls back to full scan. Numeric widening
        // enables boundary equality comparison between INT32 and INT64.
        final String TEST_BUCKET_NAME = "test-bucket-int64-gte-predicate";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'score': {'$gte': {'$numberLong': '100'}}}");

        List<String> expectedResult = List.of(
                "{\"score\": 100, \"name\": \"Bob\"}",
                "{\"score\": 150, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindInt32ValuesViaFullScanWhenQueryingInt32IndexWithInt64LtePredicate() {
        // Behavior: INT64 $lte predicate on INT32 index falls back to full scan. Numeric widening
        // enables boundary equality comparison between INT32 and INT64.
        final String TEST_BUCKET_NAME = "test-bucket-int64-lte-predicate";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'score': {'$lte': {'$numberLong': '100'}}}");

        List<String> expectedResult = List.of(
                "{\"score\": 50, \"name\": \"Alice\"}",
                "{\"score\": 100, \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    // ==================== Numeric Widening: INT64 vs DECIMAL128 Range Operators ====================

    @Test
    void shouldFindInt64ValuesViaFullScanWhenQueryingInt64IndexWithDecimal128GtPredicate() {
        // Behavior: DECIMAL128 $gt predicate on INT64 index falls back to full scan. Numeric widening
        // promotes INT64 values to DECIMAL128 for comparison.
        final String TEST_BUCKET_NAME = "test-bucket-decimal128-gt-int64";

        SingleFieldIndexDefinition amountIndex = SingleFieldIndexDefinition.create("amount-index", "amount", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, amountIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '1000'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '2000'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '3000'}, 'name': 'Charlie'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'amount': {'$gt': {'$numberDecimal': '1500'}}}");

        List<String> expectedResult = List.of(
                "{\"amount\": 2000, \"name\": \"Bob\"}",
                "{\"amount\": 3000, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindInt64ValuesViaFullScanWhenQueryingInt64IndexWithDecimal128LtPredicate() {
        // Behavior: DECIMAL128 $lt predicate on INT64 index falls back to full scan. Numeric widening
        // promotes INT64 values to DECIMAL128 for comparison.
        final String TEST_BUCKET_NAME = "test-bucket-decimal128-lt-int64";

        SingleFieldIndexDefinition amountIndex = SingleFieldIndexDefinition.create("amount-index", "amount", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, amountIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '1000'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '2000'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '3000'}, 'name': 'Charlie'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'amount': {'$lt': {'$numberDecimal': '2500'}}}");

        List<String> expectedResult = List.of(
                "{\"amount\": 1000, \"name\": \"Alice\"}",
                "{\"amount\": 2000, \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    // ==================== Numeric Widening: DOUBLE vs DECIMAL128 Range Operators ====================

    @Test
    void shouldFindDoubleValuesViaFullScanWhenQueryingDoubleIndexWithDecimal128GtePredicate() {
        // Behavior: DECIMAL128 $gte predicate on DOUBLE index falls back to full scan. Numeric widening
        // promotes DOUBLE values to DECIMAL128 for comparison.
        final String TEST_BUCKET_NAME = "test-bucket-decimal128-gte-double";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.DOUBLE, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 9.99, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 19.99, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 29.99, 'name': 'Charlie'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'price': {'$gte': {'$numberDecimal': '19.99'}}}");

        List<String> expectedResult = List.of(
                "{\"price\": 19.99, \"name\": \"Bob\"}",
                "{\"price\": 29.99, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    // ==================== Numeric Widening: $ne Operator ====================

    @Test
    void shouldExcludeInt32ValueViaFullScanWhenQueryingInt32IndexWithDoubleNePredicate() {
        // Behavior: DOUBLE $ne predicate on INT32 index falls back to full scan. Numeric widening
        // enables cross-type inequality comparison, excluding the matching INT32 value.
        final String TEST_BUCKET_NAME = "test-bucket-double-ne-predicate";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'score': {'$ne': 100.0}}");

        List<String> expectedResult = List.of(
                "{\"score\": 50, \"name\": \"Alice\"}",
                "{\"score\": 150, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldExcludeInt64ValueViaFullScanWhenQueryingInt64IndexWithDecimal128NePredicate() {
        // Behavior: DECIMAL128 $ne predicate on INT64 index falls back to full scan. Numeric widening
        // enables INT64 vs DECIMAL128 inequality comparison.
        final String TEST_BUCKET_NAME = "test-bucket-decimal128-ne-int64";

        SingleFieldIndexDefinition countIndex = SingleFieldIndexDefinition.create("count-index", "count", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, countIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'count': {'$numberLong': '500'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'count': {'$numberLong': '1000'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'count': {'$numberLong': '1500'}, 'name': 'Charlie'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'count': {'$ne': {'$numberDecimal': '1000'}}}");

        List<String> expectedResult = List.of(
                "{\"count\": 500, \"name\": \"Alice\"}",
                "{\"count\": 1500, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    // ==================== Numeric Widening: INT64 vs DOUBLE via DECIMAL128 Common Type ====================

    @Test
    void shouldFindInt64ValuesViaFullScanWhenQueryingInt64IndexWithDoubleLtPredicate() {
        // Behavior: DOUBLE predicate on INT64 index falls back to full scan. INT64 and DOUBLE are
        // both promoted to DECIMAL128 via commonType for comparison.
        final String TEST_BUCKET_NAME = "test-bucket-double-lt-int64";

        SingleFieldIndexDefinition valueIndex = SingleFieldIndexDefinition.create("value-index", "value", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valueIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '100'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '200'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '300'}, 'name': 'Charlie'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'value': {'$lt': 250.0}}");

        List<String> expectedResult = List.of(
                "{\"value\": 100, \"name\": \"Alice\"}",
                "{\"value\": 200, \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    // ==================== Numeric Widening: $in/$nin with Cross-Numeric Types ====================

    @Test
    void shouldMatchInt32ValuesWithDoubleInListViaFullScan() {
        // Behavior: $in with DOUBLE values on INT32 index falls back to full scan. Numeric widening
        // in valuesEqual enables INT32 documents to match DOUBLE operands.
        final String TEST_BUCKET_NAME = "test-bucket-in-double-widening";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 200, 'name': 'Diana'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("score", new BsonDocument("$in", new BsonArray(List.of(
                        new BsonDouble(100.0),
                        new BsonDouble(200.0)
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Bob\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Diana\"")));
    }

    @Test
    void shouldExcludeInt32ValuesWithDecimal128NinListViaFullScan() {
        // Behavior: $nin with DECIMAL128 values on INT32 index falls back to full scan. Numeric widening
        // enables INT32 documents to be excluded by DECIMAL128 operands.
        final String TEST_BUCKET_NAME = "test-bucket-nin-decimal128-widening";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("score", new BsonDocument("$nin", new BsonArray(List.of(
                        new BsonDecimal128(new Decimal128(new BigDecimal("100"))),
                        new BsonDecimal128(new Decimal128(new BigDecimal("150")))
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Alice\""));
    }

    // ==================== Numeric Widening: Array Elements ====================

    @Test
    void shouldMatchArrayWithInt32ElementsViaFullScanWhenQueryingWithDoublePredicate() {
        // Behavior: DOUBLE predicate against array containing INT32 elements triggers full scan.
        // Numeric widening enables element-wise INT32 vs DOUBLE comparison.
        final String TEST_BUCKET_NAME = "test-bucket-array-int32-double-widening";

        SingleFieldIndexDefinition valuesIndex = SingleFieldIndexDefinition.create("values-index", "values", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valuesIndex);

        BsonDocument doc1 = new BsonDocument()
                .append("values", new BsonArray(List.of(new BsonInt32(10), new BsonInt32(20))))
                .append("name", new BsonString("Alice"));
        BsonDocument doc2 = new BsonDocument()
                .append("values", new BsonArray(List.of(new BsonInt32(30), new BsonInt32(40))))
                .append("name", new BsonString("Bob"));
        BsonDocument doc3 = new BsonDocument()
                .append("values", new BsonArray(List.of(new BsonInt32(5), new BsonInt32(8))))
                .append("name", new BsonString("Charlie"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3)
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'values': {'$gt': 15.0}}");

        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Alice\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Bob\"")));
    }

    // ==================== Compound Index: Numeric Widening ====================

    @Test
    void shouldFindDocumentsViaCompoundIndexWhenInt32PredicateMatchesInt64FieldsNonStrict() {
        // Behavior: INT32 query literals match INT64 compound index fields via lossless widening
        // in non-strict mode
        final String TEST_BUCKET_NAME = "test-bucket-compound-int32-to-int64-nonstrict";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_x_y", List.of(
                new CompoundIndexField("x", BsonType.INT64, false),
                new CompoundIndexField("y", BsonType.INT64, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'x': {'$numberLong': '10'}, 'y': {'$numberLong': '20'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': {'$numberLong': '10'}, 'y': {'$numberLong': '30'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': {'$numberLong': '20'}, 'y': {'$numberLong': '20'}, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'x': 10, 'y': 20}");

        assertEquals(1, actualResult.size());
        assertEquals(List.of("{\"x\": 10, \"y\": 20, \"name\": \"Alice\"}"), actualResult);
    }

    @Test
    void shouldFindDocumentsViaCompoundIndexWhenInt32RangeMatchesDoubleFieldNonStrict() {
        // Behavior: INT32 range predicate on second DOUBLE compound field widens correctly
        // in non-strict mode
        final String TEST_BUCKET_NAME = "test-bucket-compound-int32-range-double-nonstrict";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_price", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("price", BsonType.DOUBLE, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'price': 49.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'price': 150.0}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'price': 300.0}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'price': 200.0}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'name': 'Alice', 'price': {'$gt': 100}}");

        assertEquals(2, actualResult.size());
    }

    @Test
    void shouldFallBackToFullScanFromCompoundIndexWhenInt64PredicateMatchesDoubleFieldNonStrict() {
        // Behavior: INT64 predicate on DOUBLE compound index is lossy; falls back to full scan
        // with numeric cross-type comparison in non-strict mode
        final String TEST_BUCKET_NAME = "test-bucket-compound-lossy-fallback-nonstrict";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_x_y", List.of(
                new CompoundIndexField("x", BsonType.DOUBLE, false),
                new CompoundIndexField("y", BsonType.DOUBLE, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'x': 42.0, 'y': 1.5, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': 99.0, 'y': 2.5, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': 42.0, 'y': 3.0, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // INT64 on DOUBLE = lossy, compound index rejected, full scan with cross-type comparison
        List<String> actualResult = runQueryOnBucket(metadata, "{'x': {'$numberLong': '42'}, 'y': 1.5}");

        assertEquals(1, actualResult.size());
        assertEquals(List.of("{\"x\": 42.0, \"y\": 1.5, \"name\": \"Alice\"}"), actualResult);
    }

    // ==================== Numeric Widening: $eq Operator ====================

    @Test
    void shouldMatchInt32ValueWithInt64EqPredicate() {
        // Behavior: INT64 $eq predicate on INT32 index falls back to full scan. Numeric widening
        // promotes INT32 value to INT64 for equality comparison.
        final String TEST_BUCKET_NAME = "test-bucket-eq-int32-vs-int64";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("score", new BsonDocument("$eq", new BsonInt64(100)));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(List.of("{\"score\": 100, \"name\": \"Bob\"}"), actualResult);
    }

    @Test
    void shouldMatchInt32ValueWithDoubleEqPredicate() {
        // Behavior: DOUBLE $eq predicate on INT32 index falls back to full scan. Numeric widening
        // promotes INT32 value to DOUBLE for equality comparison.
        final String TEST_BUCKET_NAME = "test-bucket-eq-int32-vs-double";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'score': {'$eq': 100.0}}");

        assertEquals(List.of("{\"score\": 100, \"name\": \"Bob\"}"), actualResult);
    }

    @Test
    void shouldMatchInt32ValueWithDecimal128EqPredicate() {
        // Behavior: DECIMAL128 $eq predicate on INT32 index falls back to full scan. Numeric widening
        // promotes INT32 value to DECIMAL128 for equality comparison.
        final String TEST_BUCKET_NAME = "test-bucket-eq-int32-vs-decimal128";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'score': {'$eq': {'$numberDecimal': '100'}}}");

        assertEquals(List.of("{\"score\": 100, \"name\": \"Bob\"}"), actualResult);
    }

    @Test
    void shouldMatchInt64ValueWithDecimal128EqPredicate() {
        // Behavior: DECIMAL128 $eq predicate on INT64 index falls back to full scan. Numeric widening
        // promotes INT64 value to DECIMAL128 for equality comparison.
        final String TEST_BUCKET_NAME = "test-bucket-eq-int64-vs-decimal128";

        SingleFieldIndexDefinition amountIndex = SingleFieldIndexDefinition.create("amount-index", "amount", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, amountIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '1000'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '2000'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '3000'}, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'amount': {'$eq': {'$numberDecimal': '2000'}}}");

        assertEquals(List.of("{\"amount\": 2000, \"name\": \"Bob\"}"), actualResult);
    }

    @Test
    void shouldMatchDoubleValueWithDecimal128EqPredicate() {
        // Behavior: DECIMAL128 $eq predicate on DOUBLE index falls back to full scan. Numeric widening
        // promotes DOUBLE value to DECIMAL128 for equality comparison.
        final String TEST_BUCKET_NAME = "test-bucket-eq-double-vs-decimal128";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.DOUBLE, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 9.99, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 19.99, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 29.99, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'price': {'$eq': {'$numberDecimal': '19.99'}}}");

        assertEquals(List.of("{\"price\": 19.99, \"name\": \"Bob\"}"), actualResult);
    }

    @Test
    void shouldMatchInt64ValueWithDoubleEqPredicateViaDecimal128() {
        // Behavior: DOUBLE $eq predicate on INT64 index falls back to full scan. INT64 and DOUBLE are
        // both promoted to DECIMAL128 via commonType for equality comparison.
        final String TEST_BUCKET_NAME = "test-bucket-eq-int64-vs-double";

        SingleFieldIndexDefinition valueIndex = SingleFieldIndexDefinition.create("value-index", "value", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valueIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '100'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '200'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '300'}, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'value': {'$eq': 200.0}}");

        assertEquals(List.of("{\"value\": 200, \"name\": \"Bob\"}"), actualResult);
    }

    // ==================== Numeric Widening: INT32 vs INT64 Gap Fillers ====================

    @Test
    void shouldFindInt32ValuesWithInt64LtPredicate() {
        // Behavior: INT64 $lt predicate on INT32 index falls back to full scan. Numeric widening
        // promotes INT32 values to INT64 for less-than comparison.
        final String TEST_BUCKET_NAME = "test-bucket-lt-int32-vs-int64";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'score': {'$lt': {'$numberLong': '100'}}}");

        assertEquals(List.of("{\"score\": 50, \"name\": \"Alice\"}"), actualResult);
    }

    @Test
    void shouldExcludeInt32ValueWithInt64NePredicate() {
        // Behavior: INT64 $ne predicate on INT32 index falls back to full scan. Numeric widening
        // enables cross-type inequality between INT32 and INT64.
        final String TEST_BUCKET_NAME = "test-bucket-ne-int32-vs-int64";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'score': {'$ne': {'$numberLong': '100'}}}");

        List<String> expectedResult = List.of(
                "{\"score\": 50, \"name\": \"Alice\"}",
                "{\"score\": 150, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchInt32ValuesWithInt64InList() {
        // Behavior: $in with INT64 values on INT32 index falls back to full scan. Numeric widening
        // in valuesEqual enables INT32 documents to match INT64 operands.
        final String TEST_BUCKET_NAME = "test-bucket-in-int32-vs-int64";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 200, 'name': 'Diana'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("score", new BsonDocument("$in", new BsonArray(List.of(
                        new BsonInt64(100),
                        new BsonInt64(200)
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Bob\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Diana\"")));
    }

    @Test
    void shouldExcludeInt32ValuesWithInt64NinList() {
        // Behavior: $nin with INT64 values on INT32 index falls back to full scan. Numeric widening
        // enables INT32 documents to be excluded by INT64 operands.
        final String TEST_BUCKET_NAME = "test-bucket-nin-int32-vs-int64";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("score", new BsonDocument("$nin", new BsonArray(List.of(
                        new BsonInt64(100),
                        new BsonInt64(150)
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Alice\""));
    }

    // ==================== Numeric Widening: INT32 vs DOUBLE Gap Fillers ====================

    @Test
    void shouldFindInt32ValuesWithDoubleGtePredicate() {
        // Behavior: DOUBLE $gte predicate on INT32 index falls back to full scan. Numeric widening
        // promotes INT32 values to DOUBLE for boundary comparison.
        final String TEST_BUCKET_NAME = "test-bucket-gte-int32-vs-double";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'score': {'$gte': 100.0}}");

        List<String> expectedResult = List.of(
                "{\"score\": 100, \"name\": \"Bob\"}",
                "{\"score\": 150, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindInt32ValuesWithDoubleLtPredicate() {
        // Behavior: DOUBLE $lt predicate on INT32 index falls back to full scan. Numeric widening
        // promotes INT32 values to DOUBLE for less-than comparison.
        final String TEST_BUCKET_NAME = "test-bucket-lt-int32-vs-double";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'score': {'$lt': 100.0}}");

        assertEquals(List.of("{\"score\": 50, \"name\": \"Alice\"}"), actualResult);
    }

    @Test
    void shouldFindInt32ValuesWithDoubleLtePredicate() {
        // Behavior: DOUBLE $lte predicate on INT32 index falls back to full scan. Numeric widening
        // promotes INT32 values to DOUBLE for boundary comparison.
        final String TEST_BUCKET_NAME = "test-bucket-lte-int32-vs-double";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'score': {'$lte': 100.0}}");

        List<String> expectedResult = List.of(
                "{\"score\": 50, \"name\": \"Alice\"}",
                "{\"score\": 100, \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldExcludeInt32ValuesWithDoubleNinList() {
        // Behavior: $nin with DOUBLE values on INT32 index falls back to full scan. Numeric widening
        // enables INT32 documents to be excluded by DOUBLE operands.
        final String TEST_BUCKET_NAME = "test-bucket-nin-int32-vs-double";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("score", new BsonDocument("$nin", new BsonArray(List.of(
                        new BsonDouble(50.0),
                        new BsonDouble(150.0)
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Bob\""));
    }

    // ==================== Numeric Widening: INT32 vs DECIMAL128 Gap Fillers ====================

    @Test
    void shouldFindInt32ValuesWithDecimal128GtPredicate() {
        // Behavior: DECIMAL128 $gt predicate on INT32 index falls back to full scan. Numeric widening
        // promotes INT32 values to DECIMAL128 for comparison.
        final String TEST_BUCKET_NAME = "test-bucket-gt-int32-vs-decimal128";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'score': {'$gt': {'$numberDecimal': '75'}}}");

        List<String> expectedResult = List.of(
                "{\"score\": 100, \"name\": \"Bob\"}",
                "{\"score\": 150, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindInt32ValuesWithDecimal128LtPredicate() {
        // Behavior: DECIMAL128 $lt predicate on INT32 index falls back to full scan. Numeric widening
        // promotes INT32 values to DECIMAL128 for comparison.
        final String TEST_BUCKET_NAME = "test-bucket-lt-int32-vs-decimal128";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'score': {'$lt': {'$numberDecimal': '100'}}}");

        assertEquals(List.of("{\"score\": 50, \"name\": \"Alice\"}"), actualResult);
    }

    @Test
    void shouldMatchInt32ValuesWithDecimal128InList() {
        // Behavior: $in with DECIMAL128 values on INT32 index falls back to full scan. Numeric widening
        // in valuesEqual enables INT32 documents to match DECIMAL128 operands.
        final String TEST_BUCKET_NAME = "test-bucket-in-int32-vs-decimal128";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 200, 'name': 'Diana'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("score", new BsonDocument("$in", new BsonArray(List.of(
                        new BsonDecimal128(new Decimal128(new BigDecimal("100"))),
                        new BsonDecimal128(new Decimal128(new BigDecimal("200")))
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Bob\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Diana\"")));
    }

    // ==================== Numeric Widening: INT64 vs DECIMAL128 Gap Fillers ====================

    @Test
    void shouldFindInt64ValuesWithDecimal128GtePredicate() {
        // Behavior: DECIMAL128 $gte predicate on INT64 index falls back to full scan. Numeric widening
        // promotes INT64 values to DECIMAL128 for boundary comparison.
        final String TEST_BUCKET_NAME = "test-bucket-gte-int64-vs-decimal128";

        SingleFieldIndexDefinition amountIndex = SingleFieldIndexDefinition.create("amount-index", "amount", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, amountIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '1000'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '2000'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '3000'}, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'amount': {'$gte': {'$numberDecimal': '2000'}}}");

        List<String> expectedResult = List.of(
                "{\"amount\": 2000, \"name\": \"Bob\"}",
                "{\"amount\": 3000, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindInt64ValuesWithDecimal128LtePredicate() {
        // Behavior: DECIMAL128 $lte predicate on INT64 index falls back to full scan. Numeric widening
        // promotes INT64 values to DECIMAL128 for boundary comparison.
        final String TEST_BUCKET_NAME = "test-bucket-lte-int64-vs-decimal128";

        SingleFieldIndexDefinition amountIndex = SingleFieldIndexDefinition.create("amount-index", "amount", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, amountIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '1000'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '2000'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '3000'}, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'amount': {'$lte': {'$numberDecimal': '2000'}}}");

        List<String> expectedResult = List.of(
                "{\"amount\": 1000, \"name\": \"Alice\"}",
                "{\"amount\": 2000, \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchInt64ValuesWithDecimal128InList() {
        // Behavior: $in with DECIMAL128 values on INT64 index falls back to full scan. Numeric widening
        // in valuesEqual enables INT64 documents to match DECIMAL128 operands.
        final String TEST_BUCKET_NAME = "test-bucket-in-int64-vs-decimal128";

        SingleFieldIndexDefinition amountIndex = SingleFieldIndexDefinition.create("amount-index", "amount", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, amountIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '1000'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '2000'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '3000'}, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '4000'}, 'name': 'Diana'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("amount", new BsonDocument("$in", new BsonArray(List.of(
                        new BsonDecimal128(new Decimal128(new BigDecimal("1000"))),
                        new BsonDecimal128(new Decimal128(new BigDecimal("3000")))
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Alice\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Charlie\"")));
    }

    @Test
    void shouldExcludeInt64ValuesWithDecimal128NinList() {
        // Behavior: $nin with DECIMAL128 values on INT64 index falls back to full scan. Numeric widening
        // enables INT64 documents to be excluded by DECIMAL128 operands.
        final String TEST_BUCKET_NAME = "test-bucket-nin-int64-vs-decimal128";

        SingleFieldIndexDefinition amountIndex = SingleFieldIndexDefinition.create("amount-index", "amount", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, amountIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '1000'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '2000'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'amount': {'$numberLong': '3000'}, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("amount", new BsonDocument("$nin", new BsonArray(List.of(
                        new BsonDecimal128(new Decimal128(new BigDecimal("1000"))),
                        new BsonDecimal128(new Decimal128(new BigDecimal("3000")))
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Bob\""));
    }

    // ==================== Numeric Widening: DOUBLE vs DECIMAL128 Gap Fillers ====================

    @Test
    void shouldFindDoubleValuesWithDecimal128GtPredicate() {
        // Behavior: DECIMAL128 $gt predicate on DOUBLE index falls back to full scan. Numeric widening
        // promotes DOUBLE values to DECIMAL128 for comparison.
        final String TEST_BUCKET_NAME = "test-bucket-gt-double-vs-decimal128";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.DOUBLE, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 9.99, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 19.99, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 29.99, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'price': {'$gt': {'$numberDecimal': '15.0'}}}");

        List<String> expectedResult = List.of(
                "{\"price\": 19.99, \"name\": \"Bob\"}",
                "{\"price\": 29.99, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldFindDoubleValuesWithDecimal128LtPredicate() {
        // Behavior: DECIMAL128 $lt predicate on DOUBLE index falls back to full scan. Numeric widening
        // promotes DOUBLE values to DECIMAL128 for comparison.
        final String TEST_BUCKET_NAME = "test-bucket-lt-double-vs-decimal128";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.DOUBLE, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 9.99, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 19.99, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 29.99, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'price': {'$lt': {'$numberDecimal': '20.0'}}}");

        List<String> expectedResult = List.of(
                "{\"price\": 9.99, \"name\": \"Alice\"}",
                "{\"price\": 19.99, \"name\": \"Bob\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldExcludeDoubleValueWithDecimal128NePredicate() {
        // Behavior: DECIMAL128 $ne predicate on DOUBLE index falls back to full scan. Numeric widening
        // enables DOUBLE vs DECIMAL128 inequality comparison.
        final String TEST_BUCKET_NAME = "test-bucket-ne-double-vs-decimal128";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.DOUBLE, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 9.99, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 19.99, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 29.99, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'price': {'$ne': {'$numberDecimal': '19.99'}}}");

        List<String> expectedResult = List.of(
                "{\"price\": 9.99, \"name\": \"Alice\"}",
                "{\"price\": 29.99, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchDoubleValuesWithDecimal128InList() {
        // Behavior: $in with DECIMAL128 values on DOUBLE index falls back to full scan. Numeric widening
        // in valuesEqual enables DOUBLE documents to match DECIMAL128 operands.
        final String TEST_BUCKET_NAME = "test-bucket-in-double-vs-decimal128";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.DOUBLE, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 9.99, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 19.99, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 29.99, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 39.99, 'name': 'Diana'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("price", new BsonDocument("$in", new BsonArray(List.of(
                        new BsonDecimal128(new Decimal128(new BigDecimal("9.99"))),
                        new BsonDecimal128(new Decimal128(new BigDecimal("29.99")))
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Alice\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Charlie\"")));
    }

    @Test
    void shouldExcludeDoubleValuesWithDecimal128NinList() {
        // Behavior: $nin with DECIMAL128 values on DOUBLE index falls back to full scan. Numeric widening
        // enables DOUBLE documents to be excluded by DECIMAL128 operands.
        final String TEST_BUCKET_NAME = "test-bucket-nin-double-vs-decimal128";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.DOUBLE, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 9.99, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 19.99, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 29.99, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("price", new BsonDocument("$nin", new BsonArray(List.of(
                        new BsonDecimal128(new Decimal128(new BigDecimal("9.99"))),
                        new BsonDecimal128(new Decimal128(new BigDecimal("29.99")))
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Bob\""));
    }

    // ==================== Numeric Widening: INT64 vs DOUBLE via DECIMAL128 Gap Fillers ====================

    @Test
    void shouldFindInt64ValuesWithDoubleGtPredicateViaDecimal128() {
        // Behavior: DOUBLE $gt predicate on INT64 index falls back to full scan. INT64 and DOUBLE are
        // both promoted to DECIMAL128 via commonType for comparison.
        final String TEST_BUCKET_NAME = "test-bucket-gt-int64-vs-double";

        SingleFieldIndexDefinition valueIndex = SingleFieldIndexDefinition.create("value-index", "value", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valueIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '100'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '200'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '300'}, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        List<String> actualResult = runQueryOnBucket(metadata, "{'value': {'$gt': 150.0}}");

        List<String> expectedResult = List.of(
                "{\"value\": 200, \"name\": \"Bob\"}",
                "{\"value\": 300, \"name\": \"Charlie\"}"
        );
        assertEquals(expectedResult, actualResult);
    }

    @Test
    void shouldMatchInt64ValuesWithDoubleInListViaDecimal128() {
        // Behavior: $in with DOUBLE values on INT64 index falls back to full scan. INT64 and DOUBLE are
        // both promoted to DECIMAL128 via commonType in valuesEqual.
        final String TEST_BUCKET_NAME = "test-bucket-in-int64-vs-double";

        SingleFieldIndexDefinition valueIndex = SingleFieldIndexDefinition.create("value-index", "value", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valueIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '100'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '200'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '300'}, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '400'}, 'name': 'Diana'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("value", new BsonDocument("$in", new BsonArray(List.of(
                        new BsonDouble(100.0),
                        new BsonDouble(300.0)
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Alice\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Charlie\"")));
    }

    @Test
    void shouldExcludeInt64ValuesWithDoubleNinListViaDecimal128() {
        // Behavior: $nin with DOUBLE values on INT64 index falls back to full scan. INT64 and DOUBLE are
        // both promoted to DECIMAL128 via commonType in valuesEqual.
        final String TEST_BUCKET_NAME = "test-bucket-nin-int64-vs-double";

        SingleFieldIndexDefinition valueIndex = SingleFieldIndexDefinition.create("value-index", "value", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valueIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '100'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '200'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'value': {'$numberLong': '300'}, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("value", new BsonDocument("$nin", new BsonArray(List.of(
                        new BsonDouble(100.0),
                        new BsonDouble(300.0)
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Bob\""));
    }

    // ==================== $all with Numeric Widening ====================

    @Test
    void shouldMatchAllWithInt64ValuesAgainstInt32ArrayElements() {
        // Behavior: $all with INT64 query values matches INT32 array elements via numeric widening
        // in valuesEqual. $all always uses full scan.
        final String TEST_BUCKET_NAME = "test-bucket-all-int64-vs-int32";

        SingleFieldIndexDefinition scoresIndex = SingleFieldIndexDefinition.create("scores-index", "scores", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoresIndex);

        BsonDocument doc1 = new BsonDocument()
                .append("scores", new BsonArray(List.of(new BsonInt32(10), new BsonInt32(20), new BsonInt32(30))))
                .append("name", new BsonString("Alice"));
        BsonDocument doc2 = new BsonDocument()
                .append("scores", new BsonArray(List.of(new BsonInt32(10), new BsonInt32(40))))
                .append("name", new BsonString("Bob"));
        BsonDocument doc3 = new BsonDocument()
                .append("scores", new BsonArray(List.of(new BsonInt32(20), new BsonInt32(30))))
                .append("name", new BsonString("Charlie"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3)
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("scores", new BsonDocument("$all", new BsonArray(List.of(
                        new BsonInt64(10),
                        new BsonInt64(20)
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(1, actualResult.size());
        assertTrue(actualResult.getFirst().contains("\"name\": \"Alice\""));
    }

    @Test
    void shouldMatchAllWithMixedNumericTypesInQueryList() {
        // Behavior: $all with mixed numeric types (INT64, DOUBLE) in query list matches INT32 array
        // elements via numeric widening. Each element is compared using valuesEqual.
        final String TEST_BUCKET_NAME = "test-bucket-all-mixed-numeric";

        SingleFieldIndexDefinition valuesIndex = SingleFieldIndexDefinition.create("values-index", "values", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, valuesIndex);

        BsonDocument doc1 = new BsonDocument()
                .append("values", new BsonArray(List.of(new BsonInt32(10), new BsonInt32(20), new BsonInt32(30))))
                .append("name", new BsonString("Alice"));
        BsonDocument doc2 = new BsonDocument()
                .append("values", new BsonArray(List.of(new BsonInt32(10), new BsonInt32(50))))
                .append("name", new BsonString("Bob"));
        BsonDocument doc3 = new BsonDocument()
                .append("values", new BsonArray(List.of(new BsonInt32(10), new BsonInt32(20), new BsonInt32(50))))
                .append("name", new BsonString("Charlie"));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3)
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        BsonDocument query = new BsonDocument()
                .append("values", new BsonDocument("$all", new BsonArray(List.of(
                        new BsonInt64(10),
                        new BsonDouble(20.0)
                ))));

        PlanWithParams planWithParams = createPlanWithParams(metadata, BSONUtil.toBytes(query));
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Alice\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Charlie\"")));
    }

    // ==================== $not with Numeric Widening ====================

    @Test
    void shouldMatchNotGtWithInt64PredicateOnInt32Documents() {
        // Behavior: $not wrapping $gt with INT64 predicate on INT32 document values. Numeric widening
        // promotes INT32 to INT64 for comparison, then $not inverts the result. $not always uses
        // full scan.
        final String TEST_BUCKET_NAME = "test-bucket-not-gt-int64-widening";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 200, 'name': 'Diana'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'score': {'$not': {'$gt': {'$numberLong': '100'}}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Alice\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Bob\"")));
    }

    @Test
    void shouldMatchNotEqWithDoublePredicateOnInt32Documents() {
        // Behavior: $not wrapping $eq with DOUBLE predicate on INT32 document values. Numeric widening
        // promotes INT32 to DOUBLE for equality, then $not inverts the result. $not always uses
        // full scan.
        final String TEST_BUCKET_NAME = "test-bucket-not-eq-double-widening";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 100, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'score': {'$not': {'$eq': 100.0}}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(2, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Alice\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Charlie\"")));
    }

    // ==================== $or with Mixed Type Predicates ====================

    @Test
    void shouldMatchOrWithNumericAndStringPredicates() {
        // Behavior: $or where one branch uses numeric widening (INT64 predicate on INT32 values) and
        // another matches string values. Both branches execute via full scan and results are unioned.
        final String TEST_BUCKET_NAME = "test-bucket-or-numeric-string-mix";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 200, 'status': 'active', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'status': 'inactive', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 80, 'status': 'pending', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'status': 'pending', 'name': 'Diana'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$or': [{'score': {'$gt': {'$numberLong': '100'}}}, {'status': 'pending'}]}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        Collections.sort(actualResult);

        assertEquals(3, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Alice\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Charlie\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Diana\"")));
    }

    @Test
    void shouldMatchOrWithDifferentNumericWideningOnTwoFields() {
        // Behavior: $or where both branches use numeric widening on different fields — INT64 predicate
        // on INT32 field and DECIMAL128 predicate on DOUBLE field. Results are unioned.
        final String TEST_BUCKET_NAME = "test-bucket-or-two-numeric-widening";

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.DOUBLE, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, scoreIndex, priceIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'score': 200, 'price': 9.99, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 50, 'price': 29.99, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 30, 'price': 5.0, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'score': 150, 'price': 25.0, 'name': 'Diana'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$or': [{'score': {'$gt': {'$numberLong': '100'}}}, {'price': {'$gte': {'$numberDecimal': '25.0'}}}]}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        Collections.sort(actualResult);

        assertEquals(3, actualResult.size());
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Alice\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Bob\"")));
        assertTrue(actualResult.stream().anyMatch(r -> r.contains("\"name\": \"Diana\"")));
    }

    // ==================== RESULTSORT with Mixed Numeric Types ====================

    @Test
    void shouldResultSortAscOnFieldWithMixedNumericTypes() {
        // Behavior: RESULTSORT ASC on a field containing INT32, INT64, and DOUBLE values. All numeric
        // types share the same type bracket and are sorted by numeric value using lossless widening.
        final String TEST_BUCKET_NAME = "test-bucket-resultsort-mixed-numeric-asc";

        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-index", "status", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, statusIndex);

        BsonDocument doc1 = new BsonDocument()
                .append("status", new BsonString("active"))
                .append("value", new BsonDouble(25.5));
        BsonDocument doc2 = new BsonDocument()
                .append("status", new BsonString("active"))
                .append("value", new BsonInt32(10));
        BsonDocument doc3 = new BsonDocument()
                .append("status", new BsonString("active"))
                .append("value", new BsonInt64(50));
        BsonDocument doc4 = new BsonDocument()
                .append("status", new BsonString("active"))
                .append("value", new BsonInt32(30));
        BsonDocument doc5 = new BsonDocument()
                .append("status", new BsonString("active"))
                .append("value", new BsonDouble(5.5));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3),
                BSONUtil.toBytes(doc4),
                BSONUtil.toBytes(doc5)
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': 'active'}");
        QueryOptions config = QueryOptions.builder()
                .resultSortField("value")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(5, actualResult.size());
        assertEquals("{\"status\": \"active\", \"value\": 5.5}", actualResult.get(0));
        assertEquals("{\"status\": \"active\", \"value\": 10}", actualResult.get(1));
        assertEquals("{\"status\": \"active\", \"value\": 25.5}", actualResult.get(2));
        assertEquals("{\"status\": \"active\", \"value\": 30}", actualResult.get(3));
        assertEquals("{\"status\": \"active\", \"value\": 50}", actualResult.get(4));
    }

    @Test
    void shouldResultSortDescOnFieldWithMixedNumericTypes() {
        // Behavior: RESULTSORT DESC on a field containing INT32, INT64, and DOUBLE values. All numeric
        // types share the same type bracket and are sorted in descending numeric order using lossless
        // widening.
        final String TEST_BUCKET_NAME = "test-bucket-resultsort-mixed-numeric-desc";

        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-index", "status", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, statusIndex);

        BsonDocument doc1 = new BsonDocument()
                .append("status", new BsonString("active"))
                .append("value", new BsonInt32(10));
        BsonDocument doc2 = new BsonDocument()
                .append("status", new BsonString("active"))
                .append("value", new BsonDouble(25.5));
        BsonDocument doc3 = new BsonDocument()
                .append("status", new BsonString("active"))
                .append("value", new BsonInt64(50));
        BsonDocument doc4 = new BsonDocument()
                .append("status", new BsonString("active"))
                .append("value", new BsonInt32(30));
        BsonDocument doc5 = new BsonDocument()
                .append("status", new BsonString("active"))
                .append("value", new BsonDouble(5.5));

        List<byte[]> documents = List.of(
                BSONUtil.toBytes(doc1),
                BSONUtil.toBytes(doc2),
                BSONUtil.toBytes(doc3),
                BSONUtil.toBytes(doc4),
                BSONUtil.toBytes(doc5)
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': 'active'}");
        QueryOptions config = QueryOptions.builder()
                .resultSortField("value")
                .resultSortDirection(SortDirection.DESC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(5, actualResult.size());
        assertEquals("{\"status\": \"active\", \"value\": 50}", actualResult.get(0));
        assertEquals("{\"status\": \"active\", \"value\": 30}", actualResult.get(1));
        assertEquals("{\"status\": \"active\", \"value\": 25.5}", actualResult.get(2));
        assertEquals("{\"status\": \"active\", \"value\": 10}", actualResult.get(3));
        assertEquals("{\"status\": \"active\", \"value\": 5.5}", actualResult.get(4));
    }
}
