package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import org.bson.BsonBinaryReader;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IntersectionNodeWithFullScanStrategyTest extends BasePipelineTest {

    @Test
    void testIntersectionWithTwoField() {
        final String TEST_BUCKET_NAME = "test-intersection-full-scan-strategy";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

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

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineExecutor executor = createPipelineExecutorForQuery(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John' } } ] }");
        PipelineContext ctx = createPipelineContext(metadata);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                System.out.println(BSONUtil.fromBson(buffer.array()).toJson());
            }

            assertEquals(2, results.size(), "Should return exactly 3 documents with age > 22");

            assertEquals(Set.of("John"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 47), extractAgesFromResults(results));
        }
    }

    @Test
    void testIntersectionWithTwoHundredDocumentsBatchProcessing() {
        final String TEST_BUCKET_NAME = "test-intersection-200-docs";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Create 200 documents where exactly 150 match the condition (age >= 10 AND category = 'electronics')
        List<byte[]> documents = new ArrayList<>();
        
        // Create 150 matching documents (age 10-159, category 'electronics')
        for (int i = 0; i < 150; i++) {
            String doc = String.format("{'age': %d, 'category': 'electronics'}", i + 10);
            documents.add(BSONUtil.jsonToDocumentThenBytes(doc));
        }
        
        // Create 50 non-matching documents
        // 25 with age < 10 and different category
        for (int i = 0; i < 25; i++) {
            String doc = String.format("{'age': %d, 'category': 'books'}", i); // age < 10 AND wrong category
            documents.add(BSONUtil.jsonToDocumentThenBytes(doc));
        }
        // 25 with age >= 10 but wrong category
        for (int i = 0; i < 25; i++) {
            String doc = String.format("{'age': %d, 'category': 'books'}", i + 160); // age >= 10 BUT wrong category
            documents.add(BSONUtil.jsonToDocumentThenBytes(doc));
        }

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        PipelineExecutor executor = createPipelineExecutorForQuery(metadata, "{ $and: [ { 'age': { '$gte': 10 } }, { 'category': { '$eq': 'electronics' } } ] }");
        PipelineContext ctx = createPipelineContext(metadata);
        ctx.setLimit(2); // Set limit to 2 to force multiple iterations

        Map<Object, ByteBuffer> allResults = new LinkedHashMap<>();
        int iterationCount = 0;
        int totalBatchSize = 0;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            while (true) {
                iterationCount++;
                Map<?, ByteBuffer> batchResults = executor.execute(tr, ctx);
                
                if (batchResults.isEmpty()) {
                    break;
                }
                
                // Track batch size for verification
                totalBatchSize += batchResults.size();
                allResults.putAll(batchResults);
                
                // Log batch content for verification
                System.out.println("Iteration " + iterationCount + ", batch size: " + batchResults.size());
                for (ByteBuffer buffer : batchResults.values()) {
                    System.out.println("  Document: " + BSONUtil.fromBson(buffer.array()).toJson());
                }
            }
        }

        // Verify the results
        assertEquals(150, allResults.size(), "Should return exactly 150 matching documents");
        
        // Verify all returned documents match the condition
        Set<String> categories = extractCategoriesFromResults(allResults);
        Set<Integer> ages = extractAgesFromResults(allResults);
        
        assertEquals(Set.of("electronics"), categories, "All documents should have category 'electronics'");
        assertTrue(ages.stream().allMatch(age -> age >= 10), "All ages should be >= 10");
        
        // Verify batch processing worked correctly
        assertEquals(150, totalBatchSize, "Total batch size should equal result count");
        
        // The iteration count includes the final empty iteration, so it should be one more than 150/2
        int expectedIterations = (150 / 2) + 1; // 75 + 1 = 76 iterations (including final empty check)
        assertEquals(expectedIterations, iterationCount, "Should take " + expectedIterations + " iterations (including final empty iteration)");
        
        System.out.println("Total iterations: " + iterationCount);
        System.out.println("Total results: " + allResults.size());
        System.out.println("Age range: " + ages.stream().min(Integer::compareTo).orElse(0) + " to " + ages.stream().max(Integer::compareTo).orElse(0));
    }

    // Helper method to extract categories from results
    Set<String> extractCategoriesFromResults(Map<Object, ByteBuffer> results) {
        Set<String> categories = new HashSet<>();
        for (ByteBuffer documentBuffer : results.values()) {
            documentBuffer.rewind();
            try (BsonBinaryReader reader = new BsonBinaryReader(documentBuffer)) {
                reader.readStartDocument();
                while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if ("category".equals(fieldName)) {
                        categories.add(reader.readString());
                    } else {
                        reader.skipValue();
                    }
                }
                reader.readEndDocument();
            }
        }
        return categories;
    }
}
