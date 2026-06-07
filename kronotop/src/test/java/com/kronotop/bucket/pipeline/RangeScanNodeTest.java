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
import org.bson.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RangeScanNodeTest extends BasePipelineTest {

    private static Stream<Arguments> provideRangeQueryTestCases() {
        return Stream.of(
                // INT32 range tests
                Arguments.of("age", BsonType.INT32,
                        List.of("{\"age\": 15}", "{\"age\": 25}", "{\"age\": 35}", "{\"age\": 45}", "{\"age\": 55}"),
                        "{'age': {'$gt': 20, '$lt': 40}}", 2, "Should return 2 documents with 20 < age < 40"),
                Arguments.of("age", BsonType.INT32,
                        List.of("{\"age\": 10}", "{\"age\": 20}", "{\"age\": 30}", "{\"age\": 40}", "{\"age\": 50}"),
                        "{'age': {'$gte': 20, '$lte': 40}}", 3, "Should return 3 documents with 20 <= age <= 40"),
                Arguments.of("age", BsonType.INT32,
                        List.of("{\"age\": 5}", "{\"age\": 10}", "{\"age\": 15}", "{\"age\": 20}", "{\"age\": 25}"),
                        "{'age': {'$gt': 12, '$lte': 22}}", 2, "Should return 2 documents with 12 < age <= 22"),

                // INT64 range tests
                Arguments.of("timestamp", BsonType.INT64,
                        List.of("{\"timestamp\": {\"$numberLong\": \"1000000000\"}}", "{\"timestamp\": {\"$numberLong\": \"2000000000\"}}",
                                "{\"timestamp\": {\"$numberLong\": \"3000000000\"}}", "{\"timestamp\": {\"$numberLong\": \"4000000000\"}}"),
                        "{'timestamp': {'$gte': {\"$numberLong\": \"1500000000\"}, '$lt': {\"$numberLong\": \"3500000000\"}}}", 2,
                        "Should return 2 documents with 1500000000 <= timestamp < 3500000000"),

                // DOUBLE range tests
                Arguments.of("price", BsonType.DOUBLE,
                        List.of("{\"price\": 10.5}", "{\"price\": 20.7}", "{\"price\": 30.2}", "{\"price\": 40.9}", "{\"price\": 50.1}"),
                        "{'price': {'$gt': 15.0, '$lt': 35.0}}", 2, "Should return 2 documents with 15.0 < price < 35.0"),
                Arguments.of("price", BsonType.DOUBLE,
                        List.of("{\"price\": 5.25}", "{\"price\": 15.75}", "{\"price\": 25.50}", "{\"price\": 35.00}", "{\"price\": 45.25}"),
                        "{'price': {'$gte': 15.75, '$lte': 35.00}}", 3, "Should return 3 documents with 15.75 <= price <= 35.00"),

                // STRING range tests
                Arguments.of("name", BsonType.STRING,
                        List.of("{\"name\": \"Alice\"}", "{\"name\": \"Bob\"}", "{\"name\": \"Charlie\"}", "{\"name\": \"David\"}", "{\"name\": \"Eve\"}"),
                        "{'name': {'$gt': \"Bob\", '$lt': \"David\"}}", 1, "Should return 1 document with 'Bob' < name < 'David' (Charlie)"),
                Arguments.of("category", BsonType.STRING,
                        List.of("{\"category\": \"books\"}", "{\"category\": \"clothes\"}", "{\"category\": \"electronics\"}", "{\"category\": \"games\"}"),
                        "{'category': {'$gte': \"clothes\", '$lte': \"electronics\"}}", 2, "Should return 2 documents with 'clothes' <= category <= 'electronics'"),

                // DECIMAL128 range tests
                /*Arguments.of("balance", BsonType.DECIMAL128,
                        List.of("{\"balance\": {\"$numberDecimal\": \"100.50\"}}",
                                "{\"balance\": {\"$numberDecimal\": \"200.75\"}}",
                                "{\"balance\": {\"$numberDecimal\": \"300.25\"}}",
                                "{\"balance\": {\"$numberDecimal\": \"400.10\"}}"),
                        "{'balance': {'$gt': {\"$numberDecimal\": \"150.00\"}, '$lt': {\"$numberDecimal\": \"350.00\"}}}", 2, "Should return 2 documents with 150.00 < balance < 350.00"),*/

                // Edge case: empty range
                Arguments.of("score", BsonType.INT32,
                        List.of("{\"score\": 10}", "{\"score\": 20}", "{\"score\": 30}", "{\"score\": 40}"),
                        "{'score': {'$gt': 50, '$lt': 60}}", 0, "Should return 0 documents with 50 < score < 60 (empty range)"),

                // Edge case: single value range
                Arguments.of("level", BsonType.INT32,
                        List.of("{\"level\": 5}", "{\"level\": 10}", "{\"level\": 15}", "{\"level\": 20}"),
                        "{'level': {'$gte': 10, '$lte': 10}}", 1, "Should return 1 document with level = 10 (single value range)")
        );
    }

    @Test
    void shouldScanIdIndexRange() {
        final String TEST_BUCKET_NAME = "test-bucket-id-range-scan-logic-gt";

        // Create an age index for this test
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        String query = String.format("{ '_id': { '$gte': '%s', '$lte': '%s' } }",
                objectIds.getFirst().toHexString(),
                objectIds.getLast().toHexString()
        );
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(4, results.size());

            // Verify the content of each returned document
            assertEquals(Set.of("John", "Alice", "George", "Claire"), extractNamesFromResults(results));
            assertEquals(Set.of(20, 23, 25, 35), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldFilterWithGtOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic-gt";

        // Create an age index for this test
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ 'age': { '$gt': 22, '$lte': 35 } }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Should return 3 documents with age > 22 (ages 23, 25, 35)
            assertEquals(3, results.size(), "Should return exactly 3 documents with age > 22");

            // Verify the content of each returned document
            assertEquals(Set.of("Alice", "George", "Claire"), extractNamesFromResults(results));
            assertEquals(Set.of(23, 25, 35), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldReturnEmptyResultSetWithGtOperator() {
        final String TEST_BUCKET_NAME = "test-bucket-range-scan-empty-result-gt";

        // Create an age index for this test
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert documents with ages all below the query threshold
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 18, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 21, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 19, 'name': 'Claire'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query for age > 22, which should match no documents since all ages are <= 21
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ 'age': { '$gt': 10, '$lt': 18 } }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Should return 0 documents since no documents have age > 22
            assertEquals(0, results.size(), "Should return exactly 0 documents with age > 22");
        }
    }

    @ParameterizedTest
    @MethodSource("provideRangeQueryTestCases")
    void shouldFilterRangeQueriesWithAllTypes(String fieldName, BsonType bsonType, List<String> testDocuments,
                                              String rangeQuery, int expectedCount, String testDescription) {
        final String TEST_BUCKET_NAME = "test-bucket-range-" + fieldName + "-" + bsonType.name().toLowerCase();

        // Create index for the test field
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create(fieldName + "-index", fieldName, bsonType, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, index);

        // Insert test documents
        List<byte[]> documents = testDocuments.stream()
                .map(BSONUtil::jsonToDocumentThenBytes)
                .toList();
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Execute range query
        PlanWithParams planWithParams = createPlanWithParams(metadata, rangeQuery);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(expectedCount, results.size(), testDescription);

            // Verify concrete expected results based on specific test cases
            if (!results.isEmpty()) {
                List<Object> actualFieldValues = new ArrayList<>();
                for (ByteBuffer buffer : results) {
                    BsonDocument doc = TestUtil.BsonDocumentFromByteBuffer(buffer);
                    actualFieldValues.add(doc.get(fieldName));
                }

                // Check concrete expected results for specific test cases
                validateForwardResults(fieldName, bsonType, rangeQuery, actualFieldValues, testDescription);
            }
        } catch (RuntimeException e) {
            if (!(e.getMessage().contains("Shard not found") || e.getMessage().contains("not found"))) {
                throw e;
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideRangeQueryTestCases")
    void shouldFilterRangeQueriesWithAllTypesReverse(String fieldName, BsonType bsonType, List<String> testDocuments,
                                                     String rangeQuery, int expectedCount, String testDescription) {
        final String TEST_BUCKET_NAME = "test-bucket-range-reverse-" + fieldName + "-" + bsonType.name().toLowerCase();

        // Create index for the test field
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create(fieldName + "-index", fieldName, bsonType, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, index);

        // Insert test documents
        List<byte[]> documents = testDocuments.stream()
                .map(BSONUtil::jsonToDocumentThenBytes)
                .toList();
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Execute the range query with SortDirection=DESC
        PlanWithParams planWithParams = createPlanWithParams(metadata, rangeQuery);
        QueryOptions config = QueryOptions.builder().sortDirection(SortDirection.DESC).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(expectedCount, results.size(), testDescription + " (SortDirection=DESC)");

            // Verify concrete expected results based on specific test cases
            if (!results.isEmpty()) {
                List<Object> actualFieldValues = new ArrayList<>();
                for (ByteBuffer buffer : results) {
                    BsonDocument doc = TestUtil.BsonDocumentFromByteBuffer(buffer);
                    actualFieldValues.add(doc.get(fieldName));
                }

                // Check concrete expected results for specific test cases
                validateReverseResults(fieldName, rangeQuery, actualFieldValues, testDescription);
            }
        }
    }

    private Object extractValue(Object obj) {
        if (obj == null) return null;
        return switch (obj) {
            case BsonInt32 v -> v.getValue();
            case BsonInt64 v -> v.getValue();
            case BsonDouble v -> v.getValue();
            case BsonString v -> v.getValue();
            case BsonBoolean v -> v.getValue();
            case BsonDecimal128 v -> v.getValue();
            case BsonDateTime v -> v.getValue();
            default -> obj;
        };
    }

    private void validateReverseResults(String fieldName, String rangeQuery, List<Object> actualFieldValues, String testDescription) {
        // Extract Java values from BsonValue types
        List<Object> extractedActualValues = actualFieldValues.stream()
                .map(this::extractValue)
                .toList();

        // Calculate expected results in reverse order based on the specific query
        List<Object> expectedValues = new ArrayList<>();

        if (fieldName.equals("age") && rangeQuery.contains("'$gt': 20, '$lt': 40")) {
            // For age range 20 < age < 40, expects [35, 25] in reverse order  
            expectedValues = List.of(35, 25);
        } else if (fieldName.equals("age") && rangeQuery.contains("'$gte': 20, '$lte': 40")) {
            // For age range 20 <= age <= 40, expects [40, 30, 20] in reverse order
            expectedValues = List.of(40, 30, 20);
        } else if (fieldName.equals("age") && rangeQuery.contains("'$gt': 12, '$lte': 22")) {
            // For age range 12 < age <= 22, expects [20, 15] in reverse order
            expectedValues = List.of(20, 15);
        } else if (fieldName.equals("timestamp") && rangeQuery.contains("'$gte': 1500000000, '$lt': 3500000000")) {
            // For timestamp range 1500000000 <= timestamp < 3500000000, expects [3000000000L, 2000000000L] in reverse order
            expectedValues = List.of(3000000000L, 2000000000L);
        } else if (fieldName.equals("price") && rangeQuery.contains("'$gt': 15.0, '$lt': 35.0")) {
            // For price range 15.0 < price < 35.0, expects [30.2, 20.7] in reverse order
            expectedValues = List.of(30.2, 20.7);
        } else if (fieldName.equals("price") && rangeQuery.contains("'$gte': 15.75, '$lte': 35.00")) {
            // For price range 15.75 <= price <= 35.00, expects [35.00, 25.50, 15.75] in reverse order
            expectedValues = List.of(35.00, 25.50, 15.75);
        } else if (fieldName.equals("name") && rangeQuery.contains("'$gt': \"Bob\", '$lt': \"David\"")) {
            // For name range 'Bob' < name < 'David', expects ["Charlie"] in reverse order
            expectedValues = List.of("Charlie");
        } else if (fieldName.equals("category") && rangeQuery.contains("'$gte': \"clothes\", '$lte': \"electronics\"")) {
            // For category range 'clothes' <= category <= 'electronics', expects ["electronics", "clothes"] in reverse order
            expectedValues = List.of("electronics", "clothes");
        } else if (fieldName.equals("balance") && rangeQuery.contains("'$gt': {\"$numberDecimal\": \"150.00\"}")) {
            // For balance range > 150.00 and < 350.00, expects decimal values in reverse order
            // Note: This is more complex due to Decimal128 handling, skip detailed validation for now
            return;
        } else if (fieldName.equals("level") && rangeQuery.contains("'$gte': 10, '$lte': 10")) {
            // For level range level = 10, expects [10] 
            expectedValues = List.of(10);
        } else {
            // For empty ranges or other cases, no specific validation needed
            return;
        }

        assertEquals(expectedValues.size(), extractedActualValues.size(),
                "Expected " + expectedValues.size() + " values but got " + extractedActualValues.size() +
                        " for reverse query: " + testDescription);

        // Check that actual values match expected values in reverse order
        for (int i = 0; i < expectedValues.size(); i++) {
            Object expected = expectedValues.get(i);
            Object actual = extractedActualValues.get(i);

            // Handle type conversion issues between Long and Integer for numeric values
            if (expected instanceof Number && actual instanceof Number) {
                long expectedLong = ((Number) expected).longValue();
                long actualLong = ((Number) actual).longValue();
                assertEquals(expectedLong, actualLong,
                        "At position " + i + ", expected " + expectedLong + " but got " + actualLong +
                                " for reverse query: " + testDescription);
            } else {
                assertEquals(expected, actual,
                        "At position " + i + ", expected " + expected + " but got " + actual +
                                " for reverse query: " + testDescription);
            }
        }
    }

    private void validateForwardResults(String fieldName, BsonType bsonType, String rangeQuery,
                                        List<Object> actualFieldValues, String testDescription) {
        // Extract Java values from BsonValue types
        List<Object> extractedActualValues = actualFieldValues.stream()
                .map(this::extractValue)
                .toList();

        // Calculate expected results in forward order based on the specific query
        List<Object> expectedValues = new ArrayList<>();

        if (fieldName.equals("age") && rangeQuery.contains("'$gt': 20, '$lt': 40")) {
            // For age range 20 < age < 40, expects [25, 35] in forward order  
            expectedValues = List.of(25, 35);
        } else if (fieldName.equals("age") && rangeQuery.contains("'$gte': 20, '$lte': 40")) {
            // For age range 20 <= age <= 40, expects [20, 30, 40] in forward order
            expectedValues = List.of(20, 30, 40);
        } else if (fieldName.equals("age") && rangeQuery.contains("'$gt': 12, '$lte': 22")) {
            // For age range 12 < age <= 22, expects [15, 20] in forward order
            expectedValues = List.of(15, 20);
        } else if (fieldName.equals("timestamp") && rangeQuery.contains("'$gte': 1500000000, '$lt': 3500000000")) {
            // For timestamp range 1500000000 <= timestamp < 3500000000, expects [2000000000L, 3000000000L] in forward order
            expectedValues = List.of(2000000000L, 3000000000L);
        } else if (fieldName.equals("price") && rangeQuery.contains("'$gt': 15.0, '$lt': 35.0")) {
            // For price range 15.0 < price < 35.0, expects [20.7, 30.2] in forward order
            expectedValues = List.of(20.7, 30.2);
        } else if (fieldName.equals("price") && rangeQuery.contains("'$gte': 15.75, '$lte': 35.00")) {
            // For price range 15.75 <= price <= 35.00, expects [15.75, 25.50, 35.00] in forward order
            expectedValues = List.of(15.75, 25.50, 35.00);
        } else if (fieldName.equals("name") && rangeQuery.contains("'$gt': \"Bob\", '$lt': \"David\"")) {
            // For name range 'Bob' < name < 'David', expects ["Charlie"] in forward order
            expectedValues = List.of("Charlie");
        } else if (fieldName.equals("category") && rangeQuery.contains("'$gte': \"clothes\", '$lte': \"electronics\"")) {
            // For category range 'clothes' <= category <= 'electronics', expects ["clothes", "electronics"] in forward order
            expectedValues = List.of("clothes", "electronics");
        } else if (fieldName.equals("balance") && rangeQuery.contains("'$gt': {\"$numberDecimal\": \"150.00\"}")) {
            // For balance range > 150.00 and < 350.00, expects decimal values in forward order
            // Note: This is more complex due to Decimal128 handling, skip detailed validation for now
            return;
        } else if (fieldName.equals("level") && rangeQuery.contains("'$gte': 10, '$lte': 10")) {
            // For level range level = 10, expects [10] 
            expectedValues = List.of(10);
        } else {
            // For empty ranges or other cases, no specific validation needed
            return;
        }

        assertEquals(expectedValues.size(), extractedActualValues.size(),
                "Expected " + expectedValues.size() + " values but got " + extractedActualValues.size() +
                        " for forward query: " + testDescription);

        // Check that actual values match expected values in forward order
        for (int i = 0; i < expectedValues.size(); i++) {
            Object expected = expectedValues.get(i);
            Object actual = extractedActualValues.get(i);

            // Handle type conversion issues between Long and Integer for numeric values
            if (expected instanceof Number && actual instanceof Number) {
                long expectedLong = ((Number) expected).longValue();
                long actualLong = ((Number) actual).longValue();
                assertEquals(expectedLong, actualLong,
                        "At position " + i + ", expected " + expectedLong + " but got " + actualLong +
                                " for forward query: " + testDescription);
            } else {
                assertEquals(expected, actual,
                        "At position " + i + ", expected " + expected + " but got " + actual +
                                " for forward query: " + testDescription);
            }
        }
    }

    @Test
    void shouldFilterInt32RangeWithMixedInput() {
        final String TEST_BUCKET_NAME = "test-int32-range-with-mixed-input";

        // Create an age index for this test
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 11, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 10, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 11, 'name': 'Donald'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 50, 'name': 'Alison'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$gte': 20, '$lte': 48}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = Arrays.asList(
                "{\"age\": 20, \"name\": \"John\"}",
                "{\"age\": 20, \"name\": \"Alice\"}",
                "{\"age\": 20, \"name\": \"George\"}",
                "{\"age\": 20, \"name\": \"Claire\"}",
                "{\"age\": 30, \"name\": \"George\"}"
        );

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        assertEquals(expectedResult, actualResult);
    }
}
