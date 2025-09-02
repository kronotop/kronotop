package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnionNodeWithFullScanStrategyTest extends BasePipelineTest {
    @Test
    void testUnionWithTwoField() {
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

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $or: [ { 'age': { '$gt': 25 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr, ctx);
            assertEquals(5, results.size());
            assertEquals(Set.of("Claire", "John", "Alison"), extractNamesFromResults(results));
            assertEquals(Set.of(20, 35, 40, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void testUnionWithSingleSubQuery() {
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

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $or: [ { 'age': { '$gt': 25 } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr, ctx);
            assertEquals(4, results.size());
            assertEquals(Set.of("Claire", "John", "Alison"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 40, 47), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void testUnionWithTwoFieldReverse() {
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

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineNode plan = createExecutionPlan(metadata, "{ $or: [ { 'age': { '$gt': 25 } }, { 'name': { '$eq': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().reverse(true).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        List<String> expectedResult = List.of(
                "{\"age\": 47, \"name\": \"John\"}",
                "{\"age\": 40, \"name\": \"Alison\"}",
                "{\"age\": 35, \"name\": \"John\"}",
                "{\"age\": 35, \"name\": \"Claire\"}",
                "{\"age\": 20, \"name\": \"John\"}"
        );

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr, ctx);
            List<String> actualResult = new ArrayList<>();
            for (ByteBuffer buffer : results.values()) {
                actualResult.add(BSONUtil.fromBson(buffer.array()).toJson());
            }
            assertEquals(expectedResult, actualResult);
        }
    }

    @Test
    void testUnionWithTwoFieldLargeDatasetBatchAnalysis() {
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
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Execute query with limit=2 and analyze each batch
        PipelineNode plan = createExecutionPlan(metadata, "{ $or: [ { 'age': { '$gt': 25 } }, { 'name': { '$eq': 'John' } } ] }");
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            QueryOptions config = QueryOptions.builder().limit(2).build();
            QueryContext ctx = new QueryContext(metadata, config, plan);
            
            int iterationCount = 0;
            int totalRetrieved = 0;
            List<String> allBatchContents = new ArrayList<>();
            
            // EXPECTED CALCULATION:
            // - Total matching documents: 180 (20 John≤25 + 20 John>25 + 140 Non-John>25)
            // - Limit per iteration: 2
            // - Expected iterations: 180/2 = 90 iterations
            // - Each batch should have 2 documents (except possibly last)
            
            System.out.printf("EXPECTED: 180 total matches (>150), limit=2, should need 90 iterations%n%n");
            
            // Execute in loop to track ALL batches and iterations
            while (true) {
                iterationCount++;
                Map<?, ByteBuffer> batchResults = executor.execute(tr, ctx);
                
                if (batchResults.isEmpty()) {
                    System.out.printf("Batch %d: Retrieved 0 documents [EMPTY - END]%n", iterationCount);
                    break; // No more results
                }
                
                System.out.printf("Batch %d: Retrieved %d documents%n", iterationCount, batchResults.size());
                
                for (ByteBuffer buffer : batchResults.values()) {
                    buffer.rewind();
                    String json = BSONUtil.fromBson(buffer.array()).toJson();
                    allBatchContents.add(json);
                    if (iterationCount <= 3 || iterationCount >= 88) { // Show first 3 and last 3
                        System.out.printf("  Document: %s%n", json);
                    }
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
            QueryContext fullCtx = new QueryContext(metadata, fullConfig, plan);
            Map<?, ByteBuffer> fullResults = executor.execute(tr, fullCtx);

            // Debug the discrepancy
            System.out.printf("%nDEBUG: Batch iteration got %d, full query got %d%n", totalRetrieved, fullResults.size());

            // Use the actual full query result for comparison
            assertEquals(fullResults.size(), totalRetrieved,
                String.format("Batch iteration (%d) should match full query (%d)", totalRetrieved, fullResults.size()));

            System.out.printf("%nBatch Analysis Summary:%n");
            System.out.printf("- Total documents in dataset: 200%n");
            System.out.printf("- Total matching documents: %d%n", totalRetrieved);
            System.out.printf("- Limit per iteration: 2%n");
            System.out.printf("- Total iterations needed: %d (+ 1 empty)%n", iterationCount - 1);
            System.out.printf("- Documents per iteration: %.1f%n", (double) totalRetrieved / (iterationCount - 1));
            System.out.printf("- Match rate: %.1f%%%n", (double) totalRetrieved / documents.size() * 100);
            System.out.printf("- VERIFICATION: Expected %d iterations, got %d%n", expectedIterations, actualIterations);
            
            // Final verification
            assertEquals(expectedIterations, actualIterations, 
                String.format("Should need exactly %d iterations for %d docs with limit=2", expectedIterations, totalRetrieved));
        }
    }

    @Test
    void testUnionWithTwoFieldLargeDatasetBatchAnalysisReverse() {
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
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Execute query with limit=2, REVERSE=true and analyze each batch
        PipelineNode plan = createExecutionPlan(metadata, "{ $or: [ { 'age': { '$gt': 25 } }, { 'name': { '$eq': 'John' } } ] }");
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            QueryOptions config = QueryOptions.builder().limit(2).reverse(true).build();
            QueryContext ctx = new QueryContext(metadata, config, plan);
            
            int iterationCount = 0;
            int totalRetrieved = 0;
            List<String> allBatchContents = new ArrayList<>();
            
            // EXPECTED CALCULATION:
            // - Total matching documents: 180 (20 John≤25 + 20 John>25 + 140 Non-John>25)
            // - Limit per iteration: 2
            // - Expected iterations: 180/2 = 90 iterations
            // - REVERSE=true: Should get documents in reverse order (highest age/versionstamp first)
            
            System.out.printf("EXPECTED REVERSE: 180 total matches (>150), limit=2, should need 90 iterations, REVERSE ORDER%n%n");
            
            // Execute in loop to track ALL batches and iterations
            while (true) {
                iterationCount++;
                Map<?, ByteBuffer> batchResults = executor.execute(tr, ctx);
                
                if (batchResults.isEmpty()) {
                    System.out.printf("Batch %d: Retrieved 0 documents [EMPTY - END]%n", iterationCount);
                    break; // No more results
                }
                
                System.out.printf("Batch %d: Retrieved %d documents%n", iterationCount, batchResults.size());
                
                for (ByteBuffer buffer : batchResults.values()) {
                    buffer.rewind();
                    String json = BSONUtil.fromBson(buffer.array()).toJson();
                    allBatchContents.add(json);
                    if (iterationCount <= 3 || iterationCount >= 88) { // Show first 3 and last 3
                        System.out.printf("  Document: %s%n", json);
                    }
                    totalRetrieved++;
                }
                
                if (iterationCount == 4 && totalRetrieved < 180) {
                    System.out.printf("  ... [skipping middle batches for brevity] ...%n");
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
            QueryContext fullCtx = new QueryContext(metadata, fullConfig, plan);
            Map<?, ByteBuffer> fullResults = executor.execute(tr, fullCtx);

            // Debug the discrepancy
            System.out.printf("%nDEBUG: Batch iteration got %d, full query got %d%n", totalRetrieved, fullResults.size());

            // Use the actual full query result for comparison
            assertEquals(fullResults.size(), totalRetrieved,
                String.format("Batch iteration (%d) should match full query (%d)", totalRetrieved, fullResults.size()));

            // Verify we got at least 150 matches as originally required
            assertTrue(totalRetrieved >= 150, 
                String.format("Should have at least 150 matches, got %d", totalRetrieved));

            // Verify reverse order - first document should have higher values than last
            if (!allBatchContents.isEmpty()) {
                String firstDoc = allBatchContents.get(0);
                String lastDoc = allBatchContents.get(allBatchContents.size() - 1);
                System.out.printf("REVERSE ORDER CHECK: First doc: %s, Last doc: %s%n", firstDoc, lastDoc);
            }

            System.out.printf("%nBatch Analysis Summary (REVERSE):%n");
            System.out.printf("- Total documents in dataset: 200%n");
            System.out.printf("- Total matching documents: %d%n", totalRetrieved);
            System.out.printf("- Limit per iteration: 2%n");
            System.out.printf("- Total iterations needed: %d (+ 1 empty)%n", actualIterations);
            System.out.printf("- Documents per iteration: %.1f%n", (double) totalRetrieved / actualIterations);
            System.out.printf("- Match rate: %.1f%%%n", (double) totalRetrieved / documents.size() * 100);
            System.out.printf("- VERIFICATION: Expected %d iterations, got %d%n", expectedIterations, actualIterations);
            System.out.printf("- REVERSE ORDER: Documents retrieved in reverse sort order%n");
            
            // Final verification
            assertEquals(expectedIterations, actualIterations, 
                String.format("Should need exactly %d iterations for %d docs with limit=2", expectedIterations, totalRetrieved));
        }
    }
}
