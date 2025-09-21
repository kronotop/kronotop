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
import com.kronotop.internal.VersionstampUtil;
import org.bson.BsonBinaryReader;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class FullScanNodeTest extends BasePipelineTest {

    @Test
    void testFullScan() {
        final String TEST_BUCKET_NAME = "test-bucket-full-scan";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(4, results.size(), "Should return exactly 3 documents with age > 22");

            // Verify the content of each returned document
            assertEquals(Set.of("John", "Alice", "George", "Claire"), extractNamesFromResults(results));
            assertEquals(Set.of(20, 23, 25, 35), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void testNeOperatorOnPrimaryIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic-gt";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        List<Versionstamp> versionstamps = insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        String query = String.format("{'_id': {'$ne': '%s'}}", VersionstampUtil.base32HexEncode(versionstamps.getFirst()));
        PipelineNode plan = createExecutionPlan(metadata, query);
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Should return 3 documents with age > 22 (ages 23, 25, 35)
            assertEquals(3, results.size(), "Should return exactly 3 documents with age > 22");

            // Verify the content of each returned document
            assertEquals(Set.of("Alice", "George", "Claire"), extractNamesFromResults(results));
            assertEquals(Set.of(23, 25, 35), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void testGtOperatorFiltersCorrectly() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic-gt";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Should return 3 documents with age > 22 (ages 23, 25, 35)
            assertEquals(3, results.size(), "Should return exactly 3 documents with age > 22");

            // Verify the content of each returned document
            assertEquals(Set.of("Alice", "George", "Claire"), extractNamesFromResults(results));
            assertEquals(Set.of(23, 25, 35), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void testGtOperatorFiltersCorrectlyReverse() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic-gt";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
        QueryOptions config = QueryOptions.builder().reverse(true).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Should return 3 documents with age > 22 (ages 23, 25, 35)
            assertEquals(3, results.size(), "Should return exactly 3 documents with age > 22");

            // Extract results in order and validate reverse sorting (descending by age)
            List<Integer> resultAges = new ArrayList<>();
            List<String> resultNames = new ArrayList<>();

            for (ByteBuffer buffer : results.values()) {
                String json = BSONUtil.fromBson(buffer.array()).toJson();
                System.out.println(json);

                // Extract age and name for order validation
                buffer.rewind();
                try (BsonBinaryReader reader = new BsonBinaryReader(buffer)) {
                    reader.readStartDocument();
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        if ("age".equals(fieldName)) {
                            resultAges.add(reader.readInt32());
                        } else if ("name".equals(fieldName)) {
                            resultNames.add(reader.readString());
                        } else {
                            reader.skipValue();
                        }
                    }
                    reader.readEndDocument();
                }
            }

            // Verify the results are in reverse order (descending by age: 35, 25, 23)
            List<Integer> expectedAgesInReverseOrder = List.of(35, 25, 23);
            List<String> expectedNamesInReverseOrder = List.of("Claire", "George", "Alice");

            assertEquals(expectedAgesInReverseOrder, resultAges,
                    "Ages should be in descending order: [35, 25, 23]");
            assertEquals(expectedNamesInReverseOrder, resultNames,
                    "Names should be in corresponding reverse order: [Claire, George, Alice]");
        }
    }

    @Test
    void testNeOperatorFiltersCorrectly() {
        final String TEST_BUCKET_NAME = "test-bucket-ne-operator";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Bob'}"), // Same age as Alice
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$ne': 23}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            System.out.println("=== NE Operator Results ===");
            for (ByteBuffer buffer : results.values()) {
                String json = BSONUtil.fromBson(buffer.array()).toJson();
                System.out.println(json);
            }

            // Should return 3 documents with age != 23 (ages 20, 25, 35)
            // Excluding Alice and Bob who both have age = 23
            assertEquals(3, results.size(), "Should return exactly 3 documents with age != 23");

            // Verify the content of each returned document
            Set<String> returnedNames = extractNamesFromResults(results);
            Set<Integer> returnedAges = extractIntegerFieldFromResults(results, "age");

            // Should include John (20), George (25), Claire (35)
            // Should exclude Alice and Bob (both age 23)
            assertEquals(Set.of("John", "George", "Claire"), returnedNames,
                    "Should return John, George, and Claire (excluding Alice and Bob with age 23)");
            assertEquals(Set.of(20, 25, 35), returnedAges,
                    "Should return ages 20, 25, 35 (excluding age 23)");

            // Verify that none of the returned documents have age = 23
            for (Integer age : returnedAges) {
                assertNotEquals(23, age, "No returned document should have age = 23");
            }
        }
    }

    @Test
    void testEqOperatorFiltersCorrectly() {
        final String TEST_BUCKET_NAME = "test-bucket-eq-operator";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"), // Same age as Alice
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Claire'}"), // Same age as Alice and George
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'David'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$eq': 25}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            System.out.println("=== EQ Operator Results ===");
            for (ByteBuffer buffer : results.values()) {
                String json = BSONUtil.fromBson(buffer.array()).toJson();
                System.out.println(json);
            }

            // Should return 3 documents with age = 25 (Alice, George, Claire)
            // Excluding John (20), Bob (30), and David (35)
            assertEquals(3, results.size(), "Should return exactly 3 documents with age = 25");

            // Verify the content of each returned document
            Set<String> returnedNames = extractNamesFromResults(results);
            Set<Integer> returnedAges = extractIntegerFieldFromResults(results, "age");

            // Should include Alice, George, Claire (all age 25)
            // Should exclude John (20), Bob (30), David (35)
            assertEquals(Set.of("Alice", "George", "Claire"), returnedNames,
                    "Should return Alice, George, and Claire (all with age 25)");
            assertEquals(Set.of(25), returnedAges,
                    "Should return only age 25");

            // Verify that all returned documents have age = 25
            for (Integer age : returnedAges) {
                assertEquals(25, age, "All returned documents should have age = 25");
            }
        }
    }

    @Test
    void testLtOperatorFiltersCorrectly() {
        final String TEST_BUCKET_NAME = "test-bucket-lt-operator";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 15, 'name': 'Amy'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$lt': 23}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            System.out.println("=== LT Operator Results ===");
            for (ByteBuffer buffer : results.values()) {
                String json = BSONUtil.fromBson(buffer.array()).toJson();
                System.out.println(json);
            }

            // Should return 2 documents with age < 23 (ages 15, 20)
            // Excluding Alice (23), George (25), and Claire (35)
            assertEquals(2, results.size(), "Should return exactly 2 documents with age < 23");

            // Verify the content of each returned document
            Set<String> returnedNames = extractNamesFromResults(results);
            Set<Integer> returnedAges = extractIntegerFieldFromResults(results, "age");

            // Should include Amy (15), John (20)
            // Should exclude Alice (23), George (25), Claire (35)
            assertEquals(Set.of("Amy", "John"), returnedNames,
                    "Should return Amy and John (ages < 23)");
            assertEquals(Set.of(15, 20), returnedAges,
                    "Should return ages 15, 20 (both < 23)");

            // Verify that all returned documents have age < 23
            for (Integer age : returnedAges) {
                assertTrue(age < 23, "All returned documents should have age < 23, but found: " + age);
            }
        }
    }

    @Test
    void testGteOperatorFiltersCorrectly() {
        final String TEST_BUCKET_NAME = "test-bucket-gte-operator";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 15, 'name': 'Amy'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gte': 23}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            System.out.println("=== GTE Operator Results ===");
            for (ByteBuffer buffer : results.values()) {
                String json = BSONUtil.fromBson(buffer.array()).toJson();
                System.out.println(json);
            }

            // Should return 3 documents with age >= 23 (ages 23, 25, 35)
            // Excluding Amy (15) and John (20)
            assertEquals(3, results.size(), "Should return exactly 3 documents with age >= 23");

            // Verify the content of each returned document
            Set<String> returnedNames = extractNamesFromResults(results);
            Set<Integer> returnedAges = extractIntegerFieldFromResults(results, "age");

            // Should include Alice (23), George (25), Claire (35)
            // Should exclude Amy (15), John (20)
            assertEquals(Set.of("Alice", "George", "Claire"), returnedNames,
                    "Should return Alice, George, and Claire (ages >= 23)");
            assertEquals(Set.of(23, 25, 35), returnedAges,
                    "Should return ages 23, 25, 35 (all >= 23)");

            // Verify that all returned documents have age >= 23
            for (Integer age : returnedAges) {
                assertTrue(age >= 23, "All returned documents should have age >= 23, but found: " + age);
            }
        }
    }

    @Test
    void testLteOperatorFiltersCorrectly() {
        final String TEST_BUCKET_NAME = "test-bucket-lte-operator";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 15, 'name': 'Amy'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$lte': 23}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

            System.out.println("=== LTE Operator Results ===");
            for (ByteBuffer buffer : results.values()) {
                String json = BSONUtil.fromBson(buffer.array()).toJson();
                System.out.println(json);
            }

            // Should return 3 documents with age <= 23 (ages 15, 20, 23)
            // Excluding George (25) and Claire (35)
            assertEquals(3, results.size(), "Should return exactly 3 documents with age <= 23");

            // Verify the content of each returned document
            Set<String> returnedNames = extractNamesFromResults(results);
            Set<Integer> returnedAges = extractIntegerFieldFromResults(results, "age");

            // Should include Amy (15), John (20), Alice (23)
            // Should exclude George (25), Claire (35)
            assertEquals(Set.of("Amy", "John", "Alice"), returnedNames,
                    "Should return Amy, John, and Alice (ages <= 23)");
            assertEquals(Set.of(15, 20, 23), returnedAges,
                    "Should return ages 15, 20, 23 (all <= 23)");

            // Verify that all returned documents have age <= 23
            for (Integer age : returnedAges) {
                assertTrue(age <= 23, "All returned documents should have age <= 23, but found: " + age);
            }
        }
    }

    @Test
    void testGtOperatorReturnsZeroResults() {
        final String TEST_BUCKET_NAME = "test-bucket-zero-results";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with ages that won't match our query
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 10, 'name': 'Amy'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 15, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 18, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'George'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for age > 50 which should return no results since all ages are <= 20
        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 50}}");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            // Should return 0 documents since no document has age > 50
            assertEquals(0, results.size(), "Should return exactly 0 documents with age > 50");
        }
    }
}
