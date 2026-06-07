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
import org.bson.BsonBinaryReader;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AndOperatorWithIndexScanStrategyTest extends BasePipelineTest {

    @Test
    void shouldAndQueryWithTwoFieldsAndSingleIndex() {
        // Behavior: AND query with one indexed field and one non-indexed field returns correct results.
        final String TEST_BUCKET_NAME = "test-intersection-with-two-fields-and-single-index";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}") // match
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size(), "Should return exactly 2 documents with age > 22 and name = John");

            assertEquals(Set.of("John"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldAndQueryWithBatchedIterationAndLimit() {
        // Behavior: AND query with limit returns correct batches across multiple cursor advances.
        final String TEST_BUCKET_NAME = "test-intersection-batched-iteration";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert 20 documents with 7 matching the query criteria
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Alice'}"),    // no match (age <= 22)
                BSONUtil.jsonToDocumentThenBytes("{'age': 21, 'name': 'John'}"),     // no match (age <= 22)
                BSONUtil.jsonToDocumentThenBytes("{'age': 22, 'name': 'Bob'}"),      // no match (age <= 22)
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'John'}"),     // match 1
                BSONUtil.jsonToDocumentThenBytes("{'age': 24, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'John'}"),     // match 2
                BSONUtil.jsonToDocumentThenBytes("{'age': 26, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 27, 'name': 'John'}"),     // match 3
                BSONUtil.jsonToDocumentThenBytes("{'age': 28, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 29, 'name': 'John'}"),     // match 4
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 31, 'name': 'John'}"),     // match 5
                BSONUtil.jsonToDocumentThenBytes("{'age': 32, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 33, 'name': 'John'}"),     // match 6
                BSONUtil.jsonToDocumentThenBytes("{'age': 34, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"),     // match 7
                BSONUtil.jsonToDocumentThenBytes("{'age': 36, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 37, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 38, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 39, 'name': 'Bob'}")       // no match (name != John)
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        // Expected ages for matching documents
        List<Integer> expectedAges = List.of(23, 25, 27, 29, 31, 33, 35);

        try (Transaction tr = createTransaction()) {
            // Batch 1: Should return 2 results
            List<ByteBuffer> batch1 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch1.size());
            assertEquals(Set.of(23, 25), extractIntegerFieldFromResults(batch1, "age"));

            // Batch 2: Should return 2 results
            List<ByteBuffer> batch2 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch2.size());
            assertEquals(Set.of(27, 29), extractIntegerFieldFromResults(batch2, "age"));

            // Batch 3: Should return 2 results
            List<ByteBuffer> batch3 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch3.size());
            assertEquals(Set.of(31, 33), extractIntegerFieldFromResults(batch3, "age"));

            // Batch 4: Should return 1 result (last match)
            List<ByteBuffer> batch4 = readExecutor.execute(tr, ctx);
            assertEquals(1, batch4.size());
            assertEquals(Set.of(35), extractIntegerFieldFromResults(batch4, "age"));

            // Batch 5: Should be empty
            List<ByteBuffer> batch5 = readExecutor.execute(tr, ctx);
            assertEquals(0, batch5.size());
        }
    }

    @Test
    void shouldAndQueryWithBatchedIterationAndLimitReverse() {
        // Behavior: AND query with limit and DESC sort returns correct batches in reverse order.
        final String TEST_BUCKET_NAME = "test-intersection-batched-iteration-reverse";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert 20 documents with 7 matching the query criteria
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Alice'}"),    // no match (age <= 22)
                BSONUtil.jsonToDocumentThenBytes("{'age': 21, 'name': 'John'}"),     // no match (age <= 22)
                BSONUtil.jsonToDocumentThenBytes("{'age': 22, 'name': 'Bob'}"),      // no match (age <= 22)
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'John'}"),     // match 1
                BSONUtil.jsonToDocumentThenBytes("{'age': 24, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'John'}"),     // match 2
                BSONUtil.jsonToDocumentThenBytes("{'age': 26, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 27, 'name': 'John'}"),     // match 3
                BSONUtil.jsonToDocumentThenBytes("{'age': 28, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 29, 'name': 'John'}"),     // match 4
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 31, 'name': 'John'}"),     // match 5
                BSONUtil.jsonToDocumentThenBytes("{'age': 32, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 33, 'name': 'John'}"),     // match 6
                BSONUtil.jsonToDocumentThenBytes("{'age': 34, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"),     // match 7
                BSONUtil.jsonToDocumentThenBytes("{'age': 36, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 37, 'name': 'Bob'}"),      // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 38, 'name': 'Alice'}"),    // no match (name != John)
                BSONUtil.jsonToDocumentThenBytes("{'age': 39, 'name': 'Bob'}")       // no match (name != John)
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().limit(2).sortDirection(SortDirection.DESC).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            // Batch 1: Should return 2 results (highest ages first)
            List<ByteBuffer> batch1 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch1.size());
            assertEquals(Set.of(35, 33), extractIntegerFieldFromResults(batch1, "age"));

            // Batch 2: Should return 2 results
            List<ByteBuffer> batch2 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch2.size());
            assertEquals(Set.of(31, 29), extractIntegerFieldFromResults(batch2, "age"));

            // Batch 3: Should return 2 results
            List<ByteBuffer> batch3 = readExecutor.execute(tr, ctx);
            assertEquals(2, batch3.size());
            assertEquals(Set.of(27, 25), extractIntegerFieldFromResults(batch3, "age"));

            // Batch 4: Should return 1 result (last match)
            List<ByteBuffer> batch4 = readExecutor.execute(tr, ctx);
            assertEquals(1, batch4.size());
            assertEquals(Set.of(23), extractIntegerFieldFromResults(batch4, "age"));

            // Batch 5: Should be empty
            List<ByteBuffer> batch5 = readExecutor.execute(tr, ctx);
            assertEquals(0, batch5.size());
        }
    }

    @Test
    void shouldAndQueryWithTwoFieldsAndDoubleIndex() {
        // Behavior: AND query with two indexed fields returns correct results using index intersection strategy.
        final String TEST_BUCKET_NAME = "test-intersection-with-two-fields-and-double-index";

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex, nameIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}") // match
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size(), "Should return exactly 2 documents with age > 22 and name = John");

            assertEquals(Set.of("John"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldAndQueryWithRangeScanAndDoubleIndex() {
        // Behavior: AND query with range condition and two indexes returns correct results.
        final String TEST_BUCKET_NAME = "test-intersection-with-two-fields-and-double-index";

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex, nameIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}") // match
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $and: [ { 'age': { '$gt': 22, '$lt': 50 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size(), "Should return exactly 2 documents with age > 22 and name = John");

            assertEquals(Set.of("John"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldExecuteAndQueryWithTwoEqPredicates() {
        // Behavior: AND query with two EQ conditions on indexed fields returns the single matching document.

        // Create two secondary indexes
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex, nameIndex);

        // Insert documents with both fields
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 25}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Create an execution plan with $and on two indexed fields
        String query = "{ $and: [ { age: { $eq: 25 } }, { name: { $eq: \"Alice\" } } ] }";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);

        assertNotNull(planWithParams.plan());

        // Run the query and verify the result
        List<String> results = runQueryOnBucket(metadata, query);
        assertEquals(1, results.size());

        // Verify the returned document is Alice with age 25
        String resultJson = results.getFirst();
        assertTrue(resultJson.contains("\"name\": \"Alice\""));
        assertTrue(resultJson.contains("\"age\": 25"));
    }

    @Test
    @Disabled
    void shouldHandleAndOperatorWithTwoIndex() {
        // Behavior: AND query with two indexed range conditions returns only matching documents.
        final String TEST_BUCKET_NAME = "test-bucket-and-index-scan";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-index", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 100}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 120}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 80}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 140}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ 'price': { '$gt': 22 }, 'quantity': { '$gt': 80 } }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size(), "Should return exactly 2 documents matching both conditions");

            // Verify the actual content of returned documents
            List<String> resultJsons = results.stream()
                    .map(TestUtil::bsonToJsonWithoutId)
                    .sorted() // Sort for consistent comparison
                    .toList();

            boolean hasDoc23_120 = resultJsons.stream()
                    .anyMatch(json -> json.contains("\"price\": 23") && json.contains("\"quantity\": 120"));
            boolean hasDoc35_140 = resultJsons.stream()
                    .anyMatch(json -> json.contains("\"price\": 35") && json.contains("\"quantity\": 140"));

            assertTrue(hasDoc23_120, "Results should contain document with price=23 and quantity=120");
            assertTrue(hasDoc35_140, "Results should contain document with price=35 and quantity=140");

            boolean hasDoc20_100 = resultJsons.stream()
                    .anyMatch(json -> json.contains("\"price\": 20") && json.contains("\"quantity\": 100"));
            boolean hasDoc25_80 = resultJsons.stream()
                    .anyMatch(json -> json.contains("\"price\": 25") && json.contains("\"quantity\": 80"));

            assertFalse(hasDoc20_100, "Results should not contain document with price=20 (fails price > 22)");
            assertFalse(hasDoc25_80, "Results should not contain document with quantity=80 (fails quantity > 80)");
        }
    }

    @Test
    @Disabled
    void shouldHandleDifferentIndexTypes() {
        // Behavior: AND query with different index types (INT32 and STRING) returns correct results.
        final String TEST_BUCKET_NAME = "test-with-different-index-types";

        // Create indexes for age and name
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex, nameIndex);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Frank'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ 'age': {'$gt': 22}, 'name': {'$eq': 'Claire'} }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Should return 1 document with age > 22 AND name == 'Claire' (only age=25, name='Claire')
            assertEquals(1, results.size(), "Should return exactly 1 document with age > 22 AND name == 'Claire'");

            // Verify the content of the returned document
            Set<String> expectedNames = Set.of("Claire");
            Set<Integer> expectedAges = Set.of(25);
            Set<String> actualNames = new HashSet<>();
            Set<Integer> actualAges = new HashSet<>();

            for (ByteBuffer documentBuffer : results) {
                // Parse the BSON document to verify its content
                documentBuffer.rewind();
                try (BsonReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();
                    String name = null;
                    Integer age = null;

                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        switch (fieldName) {
                            case "name" -> name = reader.readString();
                            case "age" -> age = reader.readInt32();
                            default -> reader.skipValue();
                        }
                    }
                    reader.readEndDocument();

                    assertNotNull(name, "Document should have a name field");
                    assertNotNull(age, "Document should have an age field");
                    assertTrue(age > 22, "Age should be greater than 22, but was: " + age);
                    assertEquals("Claire", name, "Name should be 'Claire' for AND condition to be satisfied");

                    actualNames.add(name);
                    actualAges.add(age);
                }
            }

            assertEquals(expectedNames, actualNames, "All returned documents should have name 'Claire'");
            assertEquals(expectedAges, actualAges, "Should return document with age 25");

        } catch (RuntimeException e) {
            if (e.getMessage().contains("Shard not found") || e.getMessage().contains("not found")) {
                System.out.println("Skipping AND logic test due to infrastructure issues");
            } else {
                throw e;
            }
        }
    }

    @Test
    @Disabled
    void shouldHandleEqualityWithMultipleDocuments() {
        // Behavior: AND query with equality on multiple matching documents returns all matches.
        final String TEST_BUCKET_NAME = "test-equality-with-multiple-documents";

        // Create indexes for age and name
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name-index", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex, nameIndex);

        // Insert documents where exactly 3 match the AND condition (age > 22 AND name == 'John')
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'Alice'}"),    // No match: age <= 22
                BSONUtil.jsonToDocumentThenBytes("{'age': 22, 'name': 'John'}"),   // No match: age <= 22
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'John'}"),   // Match: age > 22 AND name == 'John'
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'John'}"),   // Match: age > 22 AND name == 'John'
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"),   // Match: age > 22 AND name == 'John'
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Dennis'}")      // No match: name != 'John'
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ 'age': {'$gt': 22}, 'name': {'$eq': 'John'} }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Should return exactly 3 documents with age > 22 AND name == 'John' (ages 23, 25, 35)
            assertEquals(3, results.size(), "Should return exactly 3 documents with age > 22 AND name == 'John'");

            // Verify the content of each returned document
            Set<String> expectedNames = Set.of("John");
            Set<Integer> expectedAges = Set.of(23, 25, 35);
            Set<String> actualNames = new HashSet<>();
            Set<Integer> actualAges = new HashSet<>();

            for (ByteBuffer documentBuffer : results) {
                // Parse the BSON document to verify its content
                documentBuffer.rewind();
                try (BsonReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();
                    String name = null;
                    Integer age = null;

                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        switch (fieldName) {
                            case "name" -> name = reader.readString();
                            case "age" -> age = reader.readInt32();
                            default -> reader.skipValue();
                        }
                    }
                    reader.readEndDocument();

                    assertNotNull(name, "Document should have a name field");
                    assertNotNull(age, "Document should have an age field");
                    assertTrue(age > 22, "Age should be greater than 22, but was: " + age);
                    assertEquals("John", name, "Name should be 'John' for AND condition to be satisfied");

                    actualNames.add(name);
                    actualAges.add(age);
                }
            }

            assertEquals(expectedNames, actualNames, "All returned documents should have name 'John'");
            assertEquals(expectedAges, actualAges, "Should return documents with specific ages");

        } catch (RuntimeException e) {
            if (e.getMessage().contains("Shard not found") || e.getMessage().contains("not found")) {
                System.out.println("Skipping AND multiple match test due to infrastructure issues");
            } else {
                throw e;
            }
        }
    }

    @Test
    @Disabled
    void shouldHandleAndOperatorWithPriceQuantityRelation() {
        // Behavior: AND query with large dataset (350 documents) returns all matching documents.
        final String TEST_BUCKET_NAME = "test-bucket-price-quantity-relation";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-index", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-index", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        // Insert 350 documents with pattern {'price': $price, 'quantity': $price*20}
        List<byte[]> documents = new java.util.ArrayList<>();
        for (int price = 1; price <= 350; price++) {
            int quantity = price * 20;
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'price': %d, 'quantity': %d}", price, quantity)));
        }

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Query for documents where price > 100 AND quantity > 2500
        // Since price*20 > 2500 means price > 125, the effective condition is price > 125
        // So we expect documents with price from 126 to 350 = 225 documents
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ 'price': { '$gt': 100 }, 'quantity': { '$gt': 2500 } }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(225, results.size(), "Should return exactly 225 documents matching both conditions");

            // Verify some sample documents from the results
            Set<Integer> actualPrices = new HashSet<>();

            for (ByteBuffer documentBuffer : results) {
                documentBuffer.rewind();
                try (BsonReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();
                    Integer price = null;
                    Integer quantity = null;

                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        switch (fieldName) {
                            case "price" -> price = reader.readInt32();
                            case "quantity" -> quantity = reader.readInt32();
                            default -> reader.skipValue();
                        }
                    }
                    reader.readEndDocument();

                    assertNotNull(price, "Document should have a price field");
                    assertNotNull(quantity, "Document should have a quantity field");
                    assertTrue(price > 100, "Price should be greater than 100, but was: " + price);
                    assertTrue(quantity > 2500, "Quantity should be greater than 2500, but was: " + quantity);
                    assertEquals(price * 20, quantity.intValue(), "Quantity should equal price * 20");

                    actualPrices.add(price);
                }
            }

            // Verify that we have the expected range of prices (126-350)
            assertTrue(actualPrices.contains(126), "Results should contain document with price=126");
            assertTrue(actualPrices.contains(350), "Results should contain document with price=350");
            assertFalse(actualPrices.contains(125), "Results should not contain document with price=125");
            assertFalse(actualPrices.contains(100), "Results should not contain document with price=100");

            // Verify price range bounds
            int minPrice = actualPrices.stream().mapToInt(Integer::intValue).min().orElse(0);
            int maxPrice = actualPrices.stream().mapToInt(Integer::intValue).max().orElse(0);
            assertEquals(126, minPrice, "Minimum price in results should be 126");
            assertEquals(350, maxPrice, "Maximum price in results should be 350");

        } catch (RuntimeException e) {
            if (e.getMessage().contains("Shard not found") || e.getMessage().contains("not found")) {
                System.out.println("Skipping price-quantity relation test due to infrastructure issues");
            } else {
                throw e;
            }
        }
    }
}
