package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.SortOrder;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
}
