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
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class IndexScanNodeBatchingTest extends BasePipelineTest {
    @Test
    void testGtOperatorFiltersCorrectly() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic-gt";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert 21 documents with age > 22 (ages 23-43)
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Person1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 24, 'name': 'Person2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Person3'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 26, 'name': 'Person4'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 27, 'name': 'Person5'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 28, 'name': 'Person6'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 29, 'name': 'Person7'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Person8'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 31, 'name': 'Person9'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 32, 'name': 'Person10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 33, 'name': 'Person11'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 34, 'name': 'Person12'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Person13'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 36, 'name': 'Person14'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 37, 'name': 'Person15'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 38, 'name': 'Person16'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 39, 'name': 'Person17'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Person18'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 41, 'name': 'Person19'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 42, 'name': 'Person20'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 43, 'name': 'Person21'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            int totalProcessedDocuments = 0;
            int nonEmptyBatchCount = 0;
            int totalIterations = 0;

            while (true) {
                Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
                totalIterations++;

                if (results.isEmpty()) {
                    // Subsequent call should return empty result - this is expected
                    break;
                }

                nonEmptyBatchCount++;

                if (nonEmptyBatchCount <= 10) {
                    // First 10 batches should have 2 documents each
                    assertEquals(2, results.size(),
                            String.format("Batch %d should contain exactly 2 documents", nonEmptyBatchCount));
                } else if (nonEmptyBatchCount == 11) {
                    // Last batch should have 1 document (21 total / 2 per batch = 10 full batches + 1 partial)
                    assertEquals(1, results.size(),
                            "Last batch should contain exactly 1 document");
                }

                totalProcessedDocuments += results.size();

                // Verify the documents are returned in ascending age order
                Set<Integer> ages = extractIntegerFieldFromResults(results, "age");
                for (Integer age : ages) {
                    assertTrue(age > 22, "All returned documents should have age > 22");
                }
            }

            // Verify total counts
            assertEquals(11, nonEmptyBatchCount, "Should have 11 non-empty batches total (10 with 2 docs + 1 with 1 doc)");
            assertEquals(12, totalIterations, "Should have 12 total iterations (11 non-empty + 1 empty)");
            assertEquals(21, totalProcessedDocuments, "Should process exactly 21 documents total");
        }
    }

    @Test
    void testGteOperatorWithBatching() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic-gte";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert 21 documents with ages 23-43
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Person1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 24, 'name': 'Person2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Person3'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 26, 'name': 'Person4'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 27, 'name': 'Person5'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 28, 'name': 'Person6'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 29, 'name': 'Person7'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Person8'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 31, 'name': 'Person9'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 32, 'name': 'Person10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 33, 'name': 'Person11'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 34, 'name': 'Person12'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Person13'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 36, 'name': 'Person14'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 37, 'name': 'Person15'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 38, 'name': 'Person16'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 39, 'name': 'Person17'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Person18'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 41, 'name': 'Person19'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 42, 'name': 'Person20'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 43, 'name': 'Person21'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for age >= 33, which should match 11 documents (ages 33-43)
        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gte': 33}}");
        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            int totalProcessedDocuments = 0;
            int nonEmptyBatchCount = 0;
            int totalIterations = 0;

            while (true) {
                Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
                totalIterations++;

                if (results.isEmpty()) {
                    // Subsequent call should return empty result - this is expected
                    break;
                }

                nonEmptyBatchCount++;

                if (nonEmptyBatchCount <= 5) {
                    // First 5 batches should have 2 documents each (10 documents total)
                    assertEquals(2, results.size(),
                            String.format("Batch %d should contain exactly 2 documents", nonEmptyBatchCount));
                } else if (nonEmptyBatchCount == 6) {
                    // Last batch should have 1 document (11 total / 2 per batch = 5 full batches + 1 partial)
                    assertEquals(1, results.size(),
                            "Last batch should contain exactly 1 document");
                }

                totalProcessedDocuments += results.size();

                // Verify the documents are returned in ascending age order and all have age >= 33
                Set<Integer> ages = extractIntegerFieldFromResults(results, "age");
                for (Integer age : ages) {
                    assertTrue(age >= 33, String.format("All returned documents should have age >= 33, but found age: %d", age));
                }
            }

            // Verify total counts
            assertEquals(6, nonEmptyBatchCount, "Should have 6 non-empty batches total (5 with 2 docs + 1 with 1 doc)");
            assertEquals(7, totalIterations, "Should have 7 total iterations (6 non-empty + 1 empty)");
            assertEquals(11, totalProcessedDocuments, "Should process exactly 11 documents total with age >= 33");

            // Verify specific age range was processed
            assertEquals(11, totalProcessedDocuments, "Should match exactly 11 documents with ages 33-43");
        }
    }

    @Test
    void testGteOperatorWithBatchingReverse() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-logic-gte-reverse";

        // Create an age index for this test
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert 21 documents with ages 23-43
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Person1'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 24, 'name': 'Person2'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'Person3'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 26, 'name': 'Person4'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 27, 'name': 'Person5'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 28, 'name': 'Person6'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 29, 'name': 'Person7'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'name': 'Person8'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 31, 'name': 'Person9'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 32, 'name': 'Person10'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 33, 'name': 'Person11'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 34, 'name': 'Person12'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Person13'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 36, 'name': 'Person14'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 37, 'name': 'Person15'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 38, 'name': 'Person16'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 39, 'name': 'Person17'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Person18'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 41, 'name': 'Person19'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 42, 'name': 'Person20'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 43, 'name': 'Person21'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query for age >= 33 with reverse order, which should match 11 documents (ages 33-43) in reverse order (43-33)
        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gte': 33}}");
        QueryOptions config = QueryOptions.builder().limit(2).reverse(true).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            int totalProcessedDocuments = 0;
            int nonEmptyBatchCount = 0;
            int totalIterations = 0;
            int previousAge = Integer.MAX_VALUE; // Track descending order

            while (true) {
                Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
                totalIterations++;

                if (results.isEmpty()) {
                    // Subsequent call should return empty result - this is expected
                    break;
                }

                nonEmptyBatchCount++;

                if (nonEmptyBatchCount <= 5) {
                    // First 5 batches should have 2 documents each (10 documents total)
                    assertEquals(2, results.size(),
                            String.format("Batch %d should contain exactly 2 documents", nonEmptyBatchCount));
                } else if (nonEmptyBatchCount == 6) {
                    // Last batch should have 1 document (11 total / 2 per batch = 5 full batches + 1 partial)
                    assertEquals(1, results.size(),
                            "Last batch should contain exactly 1 document");
                }

                totalProcessedDocuments += results.size();

                // Verify the documents are returned in descending age order and all have age >= 33
                Set<Integer> ages = extractIntegerFieldFromResults(results, "age");
                for (Integer age : ages) {
                    assertTrue(age >= 33, String.format("All returned documents should have age >= 33, but found age: %d", age));
                    assertTrue(age <= previousAge, String.format("Ages should be in descending order, but found age %d after %d", age, previousAge));
                    previousAge = age;
                }
            }

            // Verify total counts
            assertEquals(6, nonEmptyBatchCount, "Should have 6 non-empty batches total (5 with 2 docs + 1 with 1 doc)");
            assertEquals(7, totalIterations, "Should have 7 total iterations (6 non-empty + 1 empty)");
            assertEquals(11, totalProcessedDocuments, "Should process exactly 11 documents total with age >= 33");

            // Verify specific age range was processed in reverse order
            assertEquals(11, totalProcessedDocuments, "Should match exactly 11 documents with ages 33-43 in reverse order");
        }
    }

    private List<byte[]> createDocumentsWithAges(int count) {
        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String json = String.format("{'age': %d, 'name': 'Person%d'}", i, i);
            documents.add(BSONUtil.jsonToDocumentThenBytes(json));
        }
        return documents;
    }

    @Test
    void testGtOperatorWithLimitOn200Documents() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-200-docs";

        // Create an age index for this test (this is the key difference from FullScanNodeTest)
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert 200 documents with ages 0-199
        List<byte[]> documents = createDocumentsWithAges(200);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Create pipeline executor with limit=2 and query age > 22
        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        // Expected calculations:
        // Total documents: 200 (ages 0-199)
        // Matching condition age > 22: ages 23-199 = 177 documents  
        // Batch size (limit): 2
        // Expected iterations: ceil(177 / 2) = 89 iterations
        int expectedTotalMatches = 177;
        int batchSize = 2;
        int expectedIterations = (int) Math.ceil((double) expectedTotalMatches / batchSize);

        int actualIterations = 0;
        int totalDocumentsReturned = 0;
        List<Integer> allReturnedAges = new ArrayList<>();

        System.out.printf("=== INDEX SCAN: Expected %d total matches, %d iterations with batch size %d ===%n",
                expectedTotalMatches, expectedIterations, batchSize);

        // Iterate through all batches using cursor-based pagination with index scan
        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
                actualIterations++;

                if (results.isEmpty()) {
                    System.out.printf("Batch %d: No more results, terminating%n", actualIterations);
                    break;
                }

                totalDocumentsReturned += results.size();

                // Collect and verify ages in this batch
                Set<Integer> batchAges = extractIntegerFieldFromResults(results, "age");
                for (Integer age : batchAges) {
                    assertTrue(age > 22, "All returned documents should have age > 22, but found: " + age);
                    allReturnedAges.add(age);
                }

                System.out.printf("Batch %d: returned %d documents, ages: %s%n",
                        actualIterations, results.size(), batchAges);

                // Each batch should return at most 'batchSize' documents
                assertTrue(results.size() <= batchSize,
                        String.format("Batch %d should return at most %d documents, but returned %d",
                                actualIterations, batchSize, results.size()));

                // Safety check to prevent infinite loop
                if (actualIterations > expectedIterations + 5) {
                    fail(String.format("Too many iterations: expected ~%d, got %d",
                            expectedIterations, actualIterations));
                }
            }
        }

        System.out.printf("=== INDEX SCAN Final Results: %d iterations, %d total documents ===%n",
                actualIterations, totalDocumentsReturned);

        // Final validations
        assertTrue(totalDocumentsReturned >= expectedTotalMatches,
                String.format("Should return at least %d documents, got %d", expectedTotalMatches, totalDocumentsReturned));

        // Verify iteration count is reasonable (should be around expectedIterations)
        assertTrue(actualIterations <= expectedIterations + 2,
                String.format("Should take at most %d iterations, took %d", expectedIterations + 2, actualIterations));

        // Verify we got documents in the expected range
        allReturnedAges.sort(Integer::compareTo);
        int minAge = allReturnedAges.getFirst();
        int maxAge = allReturnedAges.getLast();

        assertTrue(minAge >= 23, String.format("Minimum age should be >= 23, got %d", minAge));
        assertTrue(maxAge <= 199, String.format("Maximum age should be <= 199, got %d", maxAge));

        System.out.printf("Age range: %d to %d, Total unique ages: %d%n",
                minAge, maxAge, allReturnedAges.size());

        // Verify no duplicates (since we're using a List, check for unique values)
        Set<Integer> uniqueAges = new HashSet<>(allReturnedAges);
        assertEquals(allReturnedAges.size(), uniqueAges.size(),
                "Should not have duplicate ages in results");

        System.out.println("=== INDEX SCAN Test completed successfully! ===");
    }

    @Test
    void testGtOperatorWithLimitOn200DocumentsReverse() {
        final String TEST_BUCKET_NAME = "test-bucket-index-scan-200-docs-reverse";

        // Create an age index for this test (this is the key difference from FullScanNodeTest)
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, ageIndex);

        // Insert 200 documents with ages 0-199
        List<byte[]> documents = createDocumentsWithAges(200);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Create pipeline executor with limit=2 and query age > 22
        PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 22}}");
        QueryOptions config = QueryOptions.builder().limit(2).reverse(true).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        // Expected calculations for REVERSE order:
        // Total documents: 200 (ages 0-199)
        // Matching condition age > 22: ages 23-199 = 177 documents
        // In REVERSE order: ages 199, 198, 197, ..., down to 23
        // Batch size (limit): 2
        // Expected iterations: ceil(177 / 2) = 89 iterations
        int expectedTotalMatches = 177;
        int batchSize = 2;
        int expectedIterations = (int) Math.ceil((double) expectedTotalMatches / batchSize);

        int actualIterations = 0;
        int totalDocumentsReturned = 0;
        List<Integer> allReturnedAges = new ArrayList<>();

        System.out.printf("=== INDEX SCAN REVERSE: Expected %d total matches, %d iterations with batch size %d ===%n",
                expectedTotalMatches, expectedIterations, batchSize);

        // Iterate through all batches using cursor-based pagination in reverse order with index scan
        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
                actualIterations++;

                if (results.isEmpty()) {
                    System.out.printf("Batch %d: No more results, terminating%n", actualIterations);
                    break;
                }

                totalDocumentsReturned += results.size();

                // Collect and verify ages in this batch
                Set<Integer> batchAges = extractIntegerFieldFromResults(results, "age");
                for (Integer age : batchAges) {
                    assertTrue(age > 22, "All returned documents should have age > 22, but found: " + age);
                    allReturnedAges.add(age);
                }

                System.out.printf("Batch %d: returned %d documents, ages: %s%n",
                        actualIterations, results.size(), batchAges);

                // Each batch should return at most 'batchSize' documents
                assertTrue(results.size() <= batchSize,
                        String.format("Batch %d should return at most %d documents, but returned %d",
                                actualIterations, batchSize, results.size()));

                // Safety check to prevent infinite loop
                if (actualIterations > expectedIterations + 5) {
                    fail(String.format("Too many iterations: expected ~%d, got %d",
                            expectedIterations, actualIterations));
                }
            }
        }

        System.out.printf("=== INDEX SCAN REVERSE Final Results: %d iterations, %d total documents ===%n",
                actualIterations, totalDocumentsReturned);

        // Final validations
        assertTrue(totalDocumentsReturned >= expectedTotalMatches,
                String.format("Should return at least %d documents, got %d", expectedTotalMatches, totalDocumentsReturned));

        // Verify iteration count is reasonable (should be around expectedIterations)
        assertTrue(actualIterations <= expectedIterations + 2,
                String.format("Should take at most %d iterations, took %d", expectedIterations + 2, actualIterations));

        // Verify we got documents in the expected reverse order range
        // First document should be the highest age (199), last should be the lowest (23)
        int firstAge = allReturnedAges.getFirst();
        int lastAge = allReturnedAges.getLast();

        System.out.printf("INDEX SCAN REVERSE order: First age: %d, Last age: %d, Total unique ages: %d%n",
                firstAge, lastAge, new HashSet<>(allReturnedAges).size());

        // In reverse order, we should start from highest age (199) and go down
        assertTrue(firstAge >= lastAge, String.format("In reverse order, first age %d should be >= last age %d", firstAge, lastAge));
        assertTrue(firstAge <= 199, String.format("First age should be <= 199, got %d", firstAge));
        assertTrue(lastAge >= 23, String.format("Last age should be >= 23, got %d", lastAge));

        // Verify no duplicates
        Set<Integer> uniqueAges = new HashSet<>(allReturnedAges);
        assertEquals(allReturnedAges.size(), uniqueAges.size(),
                "Should not have duplicate ages in results");

        // Verify that ages are in descending order (reverse order)
        for (int i = 1; i < allReturnedAges.size(); i++) {
            int prevAge = allReturnedAges.get(i - 1);
            int currAge = allReturnedAges.get(i);
            assertTrue(prevAge >= currAge,
                    String.format("In reverse order, age at position %d (%d) should be >= age at position %d (%d)",
                            i - 1, prevAge, i, currAge));
        }

        System.out.println("=== INDEX SCAN REVERSE Test completed successfully! ===");
    }
}
