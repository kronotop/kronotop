package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import org.bson.BsonBinaryReader;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class FullScanTest extends BasePipelineTest {

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

        PipelineExecutor executor = createPipelineExecutorForQuery(metadata, "{'age': {'$gt': 22}}");
        PipelineContext ctx = createPipelineContext(metadata);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr, ctx);

            // Should return 3 documents with age > 22 (ages 23, 25, 35)
            assertEquals(3, results.size(), "Should return exactly 3 documents with age > 22");

            // Verify the content of each returned document
            assertEquals(Set.of("Alice", "George", "Claire"), extractNamesFromResults(results));
            assertEquals(Set.of(23, 25, 35), extractAgesFromResults(results));
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

        PipelineExecutor executor = createPipelineExecutorForQuery(metadata, "{'age': {'$gt': 22}}");
        PipelineContext ctx = createPipelineContext(metadata);
        ctx.setReverse(true);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr, ctx);

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
        final String TEST_BUCKET_NAME = "test-bucket-full-scan-200-docs";

        // Create bucket without indexes (full scan scenario)
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert 200 documents with ages 0-199
        List<byte[]> documents = createDocumentsWithAges(200);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Create pipeline executor with limit=2 and query age > 22
        PipelineExecutor executor = createPipelineExecutorForQuery(metadata, "{'age': {'$gt': 22}}");
        PipelineContext ctx = createPipelineContext(metadata);

        // Set limit to 2
        ctx.setLimit(2);

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

        System.out.printf("=== Expected: %d total matches, %d iterations with batch size %d ===%n",
                expectedTotalMatches, expectedIterations, batchSize);

        // Iterate through all batches using cursor-based pagination
        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Map<?, ByteBuffer> results = executor.execute(tr, ctx);
                actualIterations++;

                if (results.isEmpty()) {
                    System.out.printf("Batch %d: No more results, terminating%n", actualIterations);
                    break;
                }

                totalDocumentsReturned += results.size();

                // Collect and verify ages in this batch
                Set<Integer> batchAges = extractAgesFromResults(results);
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

        System.out.printf("=== Final Results: %d iterations, %d total documents ===%n",
                actualIterations, totalDocumentsReturned);

        // Final validations
        assertTrue(totalDocumentsReturned >= expectedTotalMatches,
                String.format("Should return at least %d documents, got %d", expectedTotalMatches, totalDocumentsReturned));

        // Verify iteration count is reasonable (should be around expectedIterations)
        assertTrue(actualIterations <= expectedIterations + 2,
                String.format("Should take at most %d iterations, took %d", expectedIterations + 2, actualIterations));

        // Verify we got documents in the expected range
        allReturnedAges.sort(Integer::compareTo);
        int minAge = allReturnedAges.get(0);
        int maxAge = allReturnedAges.get(allReturnedAges.size() - 1);

        assertTrue(minAge >= 23, String.format("Minimum age should be >= 23, got %d", minAge));
        assertTrue(maxAge <= 199, String.format("Maximum age should be <= 199, got %d", maxAge));

        System.out.printf("Age range: %d to %d, Total unique ages: %d%n",
                minAge, maxAge, allReturnedAges.size());

        // Verify no duplicates (since we're using a List, check for unique values)
        Set<Integer> uniqueAges = new HashSet<>(allReturnedAges);
        assertEquals(allReturnedAges.size(), uniqueAges.size(),
                "Should not have duplicate ages in results");

        System.out.println("=== Test completed successfully! ===");
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

        PipelineExecutor executor = createPipelineExecutorForQuery(metadata, "{'age': {'$ne': 23}}");
        PipelineContext ctx = createPipelineContext(metadata);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr, ctx);

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
            Set<Integer> returnedAges = extractAgesFromResults(results);

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

        PipelineExecutor executor = createPipelineExecutorForQuery(metadata, "{'age': {'$eq': 25}}");
        PipelineContext ctx = createPipelineContext(metadata);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr, ctx);

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
            Set<Integer> returnedAges = extractAgesFromResults(results);

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
    void testGtOperatorWithLimitOn200DocumentsReverse() {
        final String TEST_BUCKET_NAME = "test-bucket-full-scan-200-docs-reverse";

        // Create bucket without indexes (full scan scenario)
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert 200 documents with ages 0-199
        List<byte[]> documents = createDocumentsWithAges(200);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Create pipeline executor with limit=2 and query age > 22
        PipelineExecutor executor = createPipelineExecutorForQuery(metadata, "{'age': {'$gt': 22}}");
        PipelineContext ctx = createPipelineContext(metadata);

        // Set limit to 2 and enable reverse sorting
        ctx.setLimit(2);
        ctx.setReverse(true);

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

        System.out.printf("=== REVERSE: Expected %d total matches, %d iterations with batch size %d ===%n",
                expectedTotalMatches, expectedIterations, batchSize);

        // Iterate through all batches using cursor-based pagination in reverse order
        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Map<?, ByteBuffer> results = executor.execute(tr, ctx);
                actualIterations++;

                if (results.isEmpty()) {
                    System.out.printf("Batch %d: No more results, terminating%n", actualIterations);
                    break;
                }

                totalDocumentsReturned += results.size();

                // Collect and verify ages in this batch
                Set<Integer> batchAges = extractAgesFromResults(results);
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

        System.out.printf("=== REVERSE Final Results: %d iterations, %d total documents ===%n",
                actualIterations, totalDocumentsReturned);

        // Final validations
        assertTrue(totalDocumentsReturned >= expectedTotalMatches,
                String.format("Should return at least %d documents, got %d", expectedTotalMatches, totalDocumentsReturned));

        // Verify iteration count is reasonable (should be around expectedIterations)
        assertTrue(actualIterations <= expectedIterations + 2,
                String.format("Should take at most %d iterations, took %d", expectedIterations + 2, actualIterations));

        // Verify we got documents in the expected reverse order range
        // First document should be the highest age (199), last should be the lowest (23)
        int firstAge = allReturnedAges.get(0);
        int lastAge = allReturnedAges.get(allReturnedAges.size() - 1);

        System.out.printf("REVERSE order: First age: %d, Last age: %d, Total unique ages: %d%n",
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

        System.out.println("=== REVERSE Test completed successfully! ===");
    }
}
