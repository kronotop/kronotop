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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
