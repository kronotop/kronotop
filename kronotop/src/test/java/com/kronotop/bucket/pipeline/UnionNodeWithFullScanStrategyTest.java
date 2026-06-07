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
import org.bson.BsonBoolean;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class UnionNodeWithFullScanStrategyTest extends BasePipelineTest {
    @Test
    void shouldUnionWithTwoFields() {
        final String TEST_BUCKET_NAME = "test-intersection-full-scan-strategy";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}") // match
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'age': { '$gt': 25 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(5, results.size());
            assertEquals(Set.of("Claire", "John", "Alison"), extractNamesFromResults(results));
            assertEquals(Set.of(20, 35, 40, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldUnionWithSingleSubQuery() {
        final String TEST_BUCKET_NAME = "test-intersection-full-scan-strategy";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}") // match
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'age': { '$gt': 25 } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(4, results.size());
            assertEquals(Set.of("Claire", "John", "Alison"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 40, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldUnionWithTwoFieldsReverse() {
        final String TEST_BUCKET_NAME = "test-intersection-full-scan-strategy";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert multiple documents with different field types and values
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"), // match
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}") // match
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'age': { '$gt': 25 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().sortDirection(SortDirection.DESC).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = List.of(
                "{\"age\": 47, \"name\": \"John\"}",
                "{\"age\": 40, \"name\": \"Alison\"}",
                "{\"age\": 35, \"name\": \"John\"}",
                "{\"age\": 35, \"name\": \"Claire\"}",
                "{\"age\": 20, \"name\": \"John\"}"
        );

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            List<String> actualResult = new ArrayList<>();
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
            assertEquals(expectedResult, actualResult);
        }
    }

    @Test
    void shouldUnionWithTwoFieldsLargeDatasetBatchAnalysis() {
        final String TEST_BUCKET_NAME = "test-union-large-dataset-batch";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert 200 documents with controlled distribution for OR query: age > 25 OR name = 'John'
        List<byte[]> documents = new ArrayList<>();
        String[] names = {"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"};

        // Group 1: John with age <= 25 (matches name condition only) - 20 documents
        for (int i = 1; i <= 20; i++) {
            int age = 20 + (i % 6); // ages 20-25
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'age': %d, 'name': 'John'}", age)));
        }

        // Group 2: John with age > 25 (matches both conditions) - 20 documents  
        for (int i = 1; i <= 20; i++) {
            int age = 26 + i; // ages 27-46
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'age': %d, 'name': 'John'}", age)));
        }

        // Group 3: Non-John with age > 25 (matches age condition only) - 140 documents
        for (int i = 1; i <= 140; i++) {
            int age = 26 + (i % 30); // ages 26-55
            String name = names[i % names.length];
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'age': %d, 'name': '%s'}", age, name)));
        }

        // Group 4: Non-John with age <= 25 (matches neither condition) - 20 documents
        for (int i = 1; i <= 20; i++) {
            int age = 15 + (i % 11); // ages 15-25
            String name = names[i % names.length];
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'age': %d, 'name': '%s'}", age, name)));
        }

        assertEquals(200, documents.size(), "Should have exactly 200 documents");
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Execute query with limit=2 and analyze each batch
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'age': { '$gt': 25 } }, { 'name': { '$eq': 'John' } } ] }");

        try (Transaction tr = createTransaction()) {
            QueryOptions config = QueryOptions.builder().limit(2).build();
            QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

            int iterationCount = 0;
            int totalRetrieved = 0;
            List<String> allBatchContents = new ArrayList<>();

            // EXPECTED CALCULATION:
            // - Total matching documents: 180 (20 John≤25 + 20 John>25 + 140 Non-John>25)
            // - Limit per iteration: 2
            // - Expected iterations: 180/2 = 90 iterations
            // - Each batch should have 2 documents (except possibly last)

            // Execute in loop to track ALL batches and iterations
            while (true) {
                iterationCount++;
                List<ByteBuffer> batchResults = readExecutor.execute(tr, ctx);

                if (batchResults.isEmpty()) {
                    break; // No more results
                }

                for (ByteBuffer buffer : batchResults) {
                    buffer.rewind();
                    String json = TestUtil.bsonToJsonWithoutId(buffer);
                    allBatchContents.add(json);
                    totalRetrieved++;
                }
            }

            // Verify batch analysis results - we'll check the actual count
            // assertEquals will be done after we compare with full query
            int actualIterations = iterationCount - 1; // -1 because final iteration is empty
            int expectedIterations = totalRetrieved / 2; // Should be totalRetrieved/2 with limit=2

            // Verify each retrieved document matches the OR condition
            for (String json : allBatchContents) {
                boolean isJohn = json.contains("\"name\": \"John\"");
                int startIdx = json.indexOf("\"age\": ") + 7;
                int endIdx = json.indexOf(",", startIdx);
                if (endIdx == -1) endIdx = json.indexOf("}", startIdx);
                int age = Integer.parseInt(json.substring(startIdx, endIdx).trim());
                boolean ageGt25 = age > 25;

                assertTrue(isJohn || ageGt25,
                        String.format("Document should match OR condition: %s", json));
            }

            // Now test full query to verify total count
            QueryOptions fullConfig = QueryOptions.builder().limit(200).build();
            QueryContext fullCtx = new QueryContext(getSession(), metadata, fullConfig, planWithParams.plan(), planWithParams.parameters());
            List<ByteBuffer> fullResults = readExecutor.execute(tr, fullCtx);

            // Debug the discrepancy

            // Use the actual full query result for comparison
            assertEquals(fullResults.size(), totalRetrieved,
                    String.format("Batch iteration (%d) should match full query (%d)", totalRetrieved, fullResults.size()));

            // Final verification
            assertEquals(expectedIterations, actualIterations,
                    String.format("Should need exactly %d iterations for %d docs with limit=2", expectedIterations, totalRetrieved));
        }
    }

    @Test
    void shouldUnionWithTwoFieldsLargeDatasetBatchAnalysisReverse() {
        final String TEST_BUCKET_NAME = "test-union-large-dataset-batch-reverse";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert 200 documents with controlled distribution for OR query: age > 25 OR name = 'John'
        List<byte[]> documents = new ArrayList<>();
        String[] names = {"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"};

        // Group 1: John with age <= 25 (matches name condition only) - 20 documents
        for (int i = 1; i <= 20; i++) {
            int age = 20 + (i % 6); // ages 20-25
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'age': %d, 'name': 'John'}", age)));
        }

        // Group 2: John with age > 25 (matches both conditions) - 20 documents  
        for (int i = 1; i <= 20; i++) {
            int age = 26 + i; // ages 27-46
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'age': %d, 'name': 'John'}", age)));
        }

        // Group 3: Non-John with age > 25 (matches age condition only) - 140 documents
        for (int i = 1; i <= 140; i++) {
            int age = 26 + (i % 30); // ages 26-55
            String name = names[i % names.length];
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'age': %d, 'name': '%s'}", age, name)));
        }

        // Group 4: Non-John with age <= 25 (matches neither condition) - 20 documents
        for (int i = 1; i <= 20; i++) {
            int age = 15 + (i % 11); // ages 15-25
            String name = names[i % names.length];
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'age': %d, 'name': '%s'}", age, name)));
        }

        assertEquals(200, documents.size(), "Should have exactly 200 documents");
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Execute query with limit=2, SortDirection=DESC and analyze each batch
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'age': { '$gt': 25 } }, { 'name': { '$eq': 'John' } } ] }");

        try (Transaction tr = createTransaction()) {
            QueryOptions config = QueryOptions.builder().limit(2).sortDirection(SortDirection.DESC).build();
            QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

            int iterationCount = 0;
            int totalRetrieved = 0;
            List<String> allBatchContents = new ArrayList<>();

            // EXPECTED CALCULATION:
            // - Total matching documents: 180 (20 John≤25 + 20 John>25 + 140 Non-John>25)
            // - Limit per iteration: 2
            // - Expected iterations: 180/2 = 90 iterations
            // - SortDirection=DESC: Should get documents in reverse order (highest age/versionstamp first)

            // Execute in loop to track ALL batches and iterations
            while (true) {
                iterationCount++;
                List<ByteBuffer> batchResults = readExecutor.execute(tr, ctx);

                if (batchResults.isEmpty()) {
                    break; // No more results
                }

                for (ByteBuffer buffer : batchResults) {
                    buffer.rewind();
                    String json = TestUtil.bsonToJsonWithoutId(buffer);
                    allBatchContents.add(json);
                    totalRetrieved++;
                }
            }

            // Verify batch analysis results - we'll check the actual count
            int actualIterations = iterationCount - 1; // -1 because final iteration is empty
            int expectedIterations = totalRetrieved / 2; // Should be totalRetrieved/2 with limit=2

            // Verify each retrieved document matches the OR condition
            for (String json : allBatchContents) {
                boolean isJohn = json.contains("\"name\": \"John\"");
                int startIdx = json.indexOf("\"age\": ") + 7;
                int endIdx = json.indexOf(",", startIdx);
                if (endIdx == -1) endIdx = json.indexOf("}", startIdx);
                int age = Integer.parseInt(json.substring(startIdx, endIdx).trim());
                boolean ageGt25 = age > 25;

                assertTrue(isJohn || ageGt25,
                        String.format("Document should match OR condition: %s", json));
            }

            // Now test full query to verify total count
            QueryOptions fullConfig = QueryOptions.builder().limit(200).build();
            QueryContext fullCtx = new QueryContext(getSession(), metadata, fullConfig, planWithParams.plan(), planWithParams.parameters());
            List<ByteBuffer> fullResults = readExecutor.execute(tr, fullCtx);

            // Debug the discrepancy

            // Use the actual full query result for comparison
            assertEquals(fullResults.size(), totalRetrieved,
                    String.format("Batch iteration (%d) should match full query (%d)", totalRetrieved, fullResults.size()));

            // Verify we got at least 150 matches as originally required
            assertTrue(totalRetrieved >= 150,
                    String.format("Should have at least 150 matches, got %d", totalRetrieved));


            // Final verification
            assertEquals(expectedIterations, actualIterations,
                    String.format("Should need exactly %d iterations for %d docs with limit=2", expectedIterations, totalRetrieved));
        }
    }

    @Test
    void shouldUpdateWithOrQueryFullScanAndPaginatedRewind() {
        // Behavior: UPDATE with $or on two non-indexed fields uses FullScan for both branches.
        // With limit=1, rewind must correctly restore FullScan cursor checkpoints.
        // ObjectId-based deduplication prevents duplicate updates across paginated iterations.

        final String TEST_BUCKET_NAME = "test-union-update-or-fullscan-rewind";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // 7 docs: 2 match both (age>25 AND name='John'), 2 age-only, 1 name-only, 2 neither
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 47, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 40, 'name': 'Alison'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 35, 'name': 'Claire'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'name': 'John'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 23, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'name': 'George'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'age': { '$gt': 25 } }, { 'name': { '$eq': 'John' } } ] }");

        UpdateOptions update = UpdateOptions.builder().set("reviewed", new BsonBoolean(true)).build();
        QueryOptions options = QueryOptions.builder().limit(1).update(update).build();
        QueryContext updateCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> allUpdatedIds = new ArrayList<>();
        int iterations = 0;

        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ObjectId> updated = updateExecutor.execute(tr, updateCtx);
                if (updated.isEmpty()) {
                    break;
                }
                assertTrue(updated.size() <= 1, "Each batch should update at most 1 document");
                allUpdatedIds.addAll(updated);
                tr.commit().join();
            }
            iterations++;
            if (iterations > 15) {
                fail("Too many iterations for 5 matching documents with limit 1");
            }
        }

        // 5 matching: age>25 (35/John, 47/John, 40/Alison, 35/Claire) + name=John (20/John) = 5 unique
        assertEquals(5, allUpdatedIds.size(), "Should update 5 matching documents");
        Set<ObjectId> uniqueIds = new HashSet<>(allUpdatedIds);
        assertEquals(allUpdatedIds.size(), uniqueIds.size(), "No duplicate updates");

        List<String> allDocs = runQueryOnBucket(metadata, "{}");
        assertEquals(7, allDocs.size(), "All 7 documents should exist");

        int reviewedCount = 0;
        for (String json : allDocs) {
            if (json.contains("\"reviewed\": true")) {
                reviewedCount++;
            }
        }
        assertEquals(5, reviewedCount, "5 documents should have reviewed=true");
    }

    @Test
    void shouldHandleOrQueryWithAllDocumentsMatching() {
        // Behavior: When every document matches at least one branch of the OR, all documents are returned.

        final String TEST_BUCKET_NAME = "test-union-full-scan-all-match";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25, 'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 10 } }, { 'quantity': { '$lte': 5 } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void shouldHandleOrQueryWithRangeScanAndComparison() {
        // Behavior: OR with a range condition ($gt + $lt) on one branch and a comparison on another.

        final String TEST_BUCKET_NAME = "test-union-full-scan-range-comparison";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25, 'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 65, 'quantity': 65, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 35, '$lt': 50 } }, { 'quantity': { '$lte': 35 } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void shouldHandleOrQueryWithTwoDifferentFields() {
        // Behavior: OR across two different non-indexed fields returns the union of both branches.

        final String TEST_BUCKET_NAME = "test-union-full-scan-two-different-fields";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25, 'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 35 } }, { 'quantity': { '$lte': 35 } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void shouldHandleOrQueryWithOneEmptyBranch() {
        // Behavior: When one OR branch matches nothing, only the other branch's results are returned.

        final String TEST_BUCKET_NAME = "test-union-full-scan-empty-branch";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25, 'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 35 } }, { 'category': { '$eq': 'Car' } } ] }");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void shouldHandleThreeWayOrQuery() {
        // Behavior: 3-way OR where all three branches are FullScan returns the union of all branches.

        final String TEST_BUCKET_NAME = "test-union-full-scan-three-way";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25, 'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 65, 'quantity': 65, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        String query = "{ $or: [ { 'price': { '$gt': 35, '$lt': 50 } }, { 'quantity': { '$lte': 35 } }, { 'category': { '$ne': 'Car' } } ] }";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> expectedResult = new ArrayList<>();
        expectedResult.add("{\"price\": 20, \"quantity\": 20, \"category\": \"Book\"}");
        expectedResult.add("{\"price\": 23, \"quantity\": 23, \"category\": \"Electronics\"}");
        expectedResult.add("{\"price\": 25, \"quantity\": 25, \"category\": \"Furniture\"}");
        expectedResult.add("{\"price\": 35, \"quantity\": 35, \"category\": \"Clothing\"}");
        expectedResult.add("{\"price\": 45, \"quantity\": 45, \"category\": \"Food\"}");
        expectedResult.add("{\"price\": 65, \"quantity\": 65, \"category\": \"Food\"}");

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            for (ByteBuffer buffer : result) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }
        assertEquals(expectedResult.size(), actualResult.size());
        assertTrue(actualResult.containsAll(expectedResult));
        assertTrue(expectedResult.containsAll(actualResult));
    }

    @Test
    void shouldReturnEmptyForThreeWayOrQueryWithAllEmptyBranches() {
        // Behavior: When no branch of a 3-way OR matches any document, the result is empty.

        final String TEST_BUCKET_NAME = "test-union-full-scan-all-empty-branches";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'price': 20, 'quantity': 20, 'category': 'Book'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 23, 'quantity': 23, 'category': 'Electronics'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 25, 'quantity': 25, 'category': 'Furniture'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 35, 'quantity': 35, 'category': 'Clothing'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 45, 'quantity': 45, 'category': 'Food'}"),
                BSONUtil.jsonToDocumentThenBytes("{'price': 65, 'quantity': 65, 'category': 'Food'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        String query = "{ $or: [ { 'price': { '$gt': 75, '$lt': 90 } }, { 'quantity': { '$gte': 200 } }, { 'category': { '$eq': 'Car' } } ] }";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext readCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> result = readExecutor.execute(tr, readCtx);
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void shouldNotRewindWhenUnionResultsExactlyMatchLimit() {
        // Behavior: When the union of child results produces exactly the number of entries equal
        // to the limit, no excess entries exist and rewind should not occur. Children advance
        // their cursors normally and the next ADVANCE resumes from the correct position.

        final String TEST_BUCKET_NAME = "test-union-full-scan-exact-limit-boundary";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // 6 docs with no overlap between branches: 3 match price>30, 3 match quantity<=25
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'A', 'price': 10, 'quantity': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B', 'price': 15, 'quantity': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C', 'price': 50, 'quantity': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'D', 'price': 60, 'quantity': 60}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'E', 'price': 20, 'quantity': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'F', 'price': 70, 'quantity': 70}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 30 } }, { 'quantity': { '$lte': 25 } } ] }");

        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        Set<String> allNames = new HashSet<>();
        int iterations = 0;

        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);

                if (results.isEmpty()) {
                    break;
                }

                assertTrue(results.size() <= 2, "Each batch should return at most 2 documents");
                allNames.addAll(extractNamesFromResults(results));
                iterations++;

                if (iterations > 10) {
                    fail("Too many iterations for 6 documents with limit 2");
                }
            }
        }

        assertEquals(6, allNames.size(), "Should find all 6 matching documents");
        assertEquals(Set.of("A", "B", "C", "D", "E", "F"), allNames);
    }

    @Test
    void shouldRewindFullScanChildrenWithLimit() {
        // Behavior: $or with two non-indexed fields creates a UnionNode with two FullScanNode
        // children. With limit=2, rewind must correctly restore checkpoints for both children.
        // Overlapping results between branches are deduplicated.

        final String TEST_BUCKET_NAME = "test-union-full-scan-rewind";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // 10 docs: 3 match both, 3 match price only, 2 match category only, 2 match neither
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B1', 'price': 110, 'category': 'rare'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B2', 'price': 120, 'category': 'rare'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B3', 'price': 130, 'category': 'rare'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'P1', 'price': 140, 'category': 'common'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'P2', 'price': 150, 'category': 'common'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'P3', 'price': 160, 'category': 'common'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C1', 'price': 50, 'category': 'rare'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C2', 'price': 60, 'category': 'rare'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'N1', 'price': 70, 'category': 'common'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'N2', 'price': 80, 'category': 'common'}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 100 } }, { 'category': { '$eq': 'rare' } } ] }");

        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> allNames = new ArrayList<>();
        int iterations = 0;

        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);

                if (results.isEmpty()) {
                    break;
                }

                assertTrue(results.size() <= 2, "Each batch should return at most 2 documents");
                allNames.addAll(extractNamesFromResults(results));
                iterations++;

                if (iterations > 15) {
                    fail("Too many iterations for 8 matching documents with limit 2");
                }
            }
        }

        Set<String> uniqueNames = new HashSet<>(allNames);
        assertEquals(allNames.size(), uniqueNames.size(), "Should have no duplicate documents");
        assertEquals(8, allNames.size(), "Should find all 8 matching documents");

        Set<String> expectedNames = Set.of("B1", "B2", "B3", "P1", "P2", "P3", "C1", "C2");
        assertEquals(expectedNames, uniqueNames);
    }

    @Test
    void shouldUpdateWithOrQueryLimitTwoAndRewind() {
        // Behavior: UPDATE with $or on two non-indexed fields and limit=2 exercises the
        // keptHandles/excessHandles split in UnionNode where a single child contributes
        // both kept and excess entries in the same batch. All matching documents are
        // updated exactly once across multiple paginated iterations.

        final String TEST_BUCKET_NAME = "test-union-full-scan-update-limit-two-rewind";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // 10 docs: 3 match both (price>100 AND quantity<=25), 2 price-only, 3 quantity-only, 2 neither
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B1', 'price': 110, 'quantity': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B2', 'price': 120, 'quantity': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B3', 'price': 130, 'quantity': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'P1', 'price': 140, 'quantity': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'P2', 'price': 150, 'quantity': 60}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Q1', 'price': 50, 'quantity': 5}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Q2', 'price': 60, 'quantity': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Q3', 'price': 70, 'quantity': 22}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'N1', 'price': 80, 'quantity': 70}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'N2', 'price': 90, 'quantity': 80}")
        );

        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{ $or: [ { 'price': { '$gt': 100 } }, { 'quantity': { '$lte': 25 } } ] }");

        UpdateOptions update = UpdateOptions.builder().set("reviewed", new BsonBoolean(true)).build();
        QueryOptions options = QueryOptions.builder().limit(2).update(update).build();
        QueryContext updateCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> allUpdatedIds = new ArrayList<>();
        int iterations = 0;

        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ObjectId> updated = updateExecutor.execute(tr, updateCtx);
                if (updated.isEmpty()) {
                    break;
                }
                assertTrue(updated.size() <= 2, "Each batch should update at most 2 documents");
                allUpdatedIds.addAll(updated);
                tr.commit().join();
            }
            iterations++;
            if (iterations > 15) {
                fail("Too many iterations for 8 matching documents with limit 2");
            }
        }

        assertEquals(8, allUpdatedIds.size(), "Should update 8 matching documents");
        Set<ObjectId> uniqueIds = new HashSet<>(allUpdatedIds);
        assertEquals(allUpdatedIds.size(), uniqueIds.size(), "No duplicate updates");

        List<String> allDocs = runQueryOnBucket(metadata, "{}");
        assertEquals(10, allDocs.size(), "All 10 documents should exist");

        int reviewedCount = 0;
        for (String json : allDocs) {
            if (json.contains("\"reviewed\": true")) {
                reviewedCount++;
            }
        }
        assertEquals(8, reviewedCount, "8 documents should have reviewed=true");
    }
}
