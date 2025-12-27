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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class IntersectionNodeBatchAnalysisTest extends BasePipelineTest {

    @Test
    @Disabled
    void shouldProcessIntersectionWith50DocumentsAndLimit() {
        final String TEST_BUCKET_NAME = "test-bucket-intersection-50-docs";

        // Create indexes for price and quantity
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32);
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        // Generate and insert 50 documents
        List<byte[]> documents = generateTestDocuments();
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: price > 25 AND quantity > 150 with limit=2
        String query = "{ 'price': { '$gt': 25 }, 'quantity': { '$gt': 150 } }";
        PipelineNode plan = createExecutionPlan(metadata, query);
        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        Set<Integer> seenDocumentIds = new HashSet<>();
        int batchCount = 0;
        int totalDocumentsReturned = 0;

        while (true) {
            batchCount++;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

                // Should return at most 2 results due to limit
                assertTrue(results.size() <= 2, "Should return at most 2 results due to limit");

                // Process each document in this batch
                for (ByteBuffer buffer : results.values()) {
                    String json = BSONUtil.fromBson(buffer.array()).toJson();
                    // Extract document values for validation
                    int price = extractIntValue(json, "price");
                    int quantity = extractIntValue(json, "quantity");
                    int id = extractIntValue(json, "id");

                    // Verify document matches query criteria
                    assertTrue(price > 25, "Document price (" + price + ") should be > 25");
                    assertTrue(quantity > 150, "Document quantity (" + quantity + ") should be > 150");

                    // Ensure no duplicate documents across batches
                    assertFalse(seenDocumentIds.contains(id), "Document with id " + id + " already seen in previous batch");
                    seenDocumentIds.add(id);

                    totalDocumentsReturned++;
                }

                System.out.println("Batch " + batchCount + ":");
                System.out.println("- Results in this batch: " + results.size());
                System.out.println("- Total accumulated results: " + totalDocumentsReturned);

                if (results.isEmpty()) {
                    System.out.println("DONE - No more results");
                    break;
                }
            }
        }

        // Final validation: all 50 generated documents should match criteria, so we expect all 50
        assertEquals(50, totalDocumentsReturned,
                "Expected all 50 documents to match criteria (price > 25 AND quantity > 150)");

        // Verify uniqueness
        assertEquals(totalDocumentsReturned, seenDocumentIds.size(),
                "All returned documents should be unique");

        System.out.println("Test completed successfully:");
        System.out.println("- Generated 50 documents");
        System.out.println("- Query: price > 25 AND quantity > 150");
        System.out.println("- Limit per batch: 2");
        System.out.println("- Total batches: " + batchCount);
        System.out.println("- Total documents returned: " + totalDocumentsReturned);
        System.out.println("- All documents verified against expected criteria");
    }

    @Test
    @Disabled
    void shouldProcessIntersectionWith350DocumentsAndLimit() {
        final String TEST_BUCKET_NAME = "test-bucket-intersection-350-docs";

        // Create indexes for price and quantity
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32);
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);

        // Generate and insert 350 documents
        List<byte[]> documents = generate350TestDocuments();
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query: price > 25 AND quantity > 150 with limit=2
        String query = "{ 'price': { '$gt': 25 }, 'quantity': { '$gt': 150 } }";
        PipelineNode plan = createExecutionPlan(metadata, query);
        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        Set<Integer> seenDocumentIds = new HashSet<>();
        int batchCount = 0;
        int totalDocumentsReturned = 0;

        while (true) {
            batchCount++;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);

                // Should return at most 2 results due to limit
                assertTrue(results.size() <= 2, "Should return at most 2 results due to limit");

                // Process each document in this batch
                for (ByteBuffer buffer : results.values()) {
                    String json = BSONUtil.fromBson(buffer.array()).toJson();
                    // Extract document values for validation
                    int price = extractIntValue(json, "price");
                    int quantity = extractIntValue(json, "quantity");
                    int id = extractIntValue(json, "id");

                    // Verify document matches query criteria
                    assertTrue(price > 25, "Document price (" + price + ") should be > 25");
                    assertTrue(quantity > 150, "Document quantity (" + quantity + ") should be > 150");

                    // Ensure no duplicate documents across batches
                    assertFalse(seenDocumentIds.contains(id), "Document with id " + id + " already seen in previous batch");
                    seenDocumentIds.add(id);

                    totalDocumentsReturned++;
                }

                System.out.println("Batch " + batchCount + ":");
                System.out.println("- Results in this batch: " + results.size());
                System.out.println("- Total accumulated results: " + totalDocumentsReturned);

                if (results.isEmpty()) {
                    System.out.println("DONE - No more results");
                    break;
                }
            }
        }

        // Final validation: all 350 generated documents should match criteria, so we expect all 350
        assertEquals(350, totalDocumentsReturned,
                "Expected all 350 documents to match criteria (price > 25 AND quantity > 150)");

        // Verify uniqueness
        assertEquals(totalDocumentsReturned, seenDocumentIds.size(),
                "All returned documents should be unique");

        System.out.println("Test completed successfully:");
        System.out.println("- Generated 350 documents");
        System.out.println("- Query: price > 25 AND quantity > 150");
        System.out.println("- Limit per batch: 2");
        System.out.println("- Total batches: " + batchCount);
        System.out.println("- Total documents returned: " + totalDocumentsReturned);
        System.out.println("- All documents verified against expected criteria");
    }

    /**
     * Generates 350 test documents with predictable data that will match our query
     */
    private List<byte[]> generate350TestDocuments() {
        List<byte[]> documents = new ArrayList<>(350);

        for (int i = 0; i < 350; i++) {
            // Create documents that will definitely match our criteria (price > 25, quantity > 150)
            int price = 30 + (i % 50);  // Price between 30-79
            int quantity = 160 + (i % 80); // Quantity between 160-239

            String json = String.format("{'price': %d, 'quantity': %d, 'id': %d}", price, quantity, i);
            System.out.println(json);
            documents.add(BSONUtil.jsonToDocumentThenBytes(json));
        }

        return documents;
    }

    /**
     * Generates 50 test documents with predictable data that will match our query
     */
    private List<byte[]> generateTestDocuments() {
        List<byte[]> documents = new ArrayList<>(50);

        for (int i = 0; i < 50; i++) {
            // Create documents that will definitely match our criteria (price > 25, quantity > 150)
            int price = 30 + (i % 20);  // Price between 30-49
            int quantity = 160 + (i % 40); // Quantity between 160-199

            String json = String.format("{'price': %d, 'quantity': %d, 'id': %d}", price, quantity, i);
            documents.add(BSONUtil.jsonToDocumentThenBytes(json));
        }

        return documents;
    }

    /**
     * Extracts integer value from JSON string for a given field
     */
    private int extractIntValue(String json, String fieldName) {
        // Try different patterns that might be used in JSON formatting
        String[] patterns = {
                "\"" + fieldName + "\" : ",
                "\"" + fieldName + "\": ",
                "\"" + fieldName + "\":",
                fieldName + " : ",
                fieldName + ": ",
                fieldName + ":"
        };

        for (String pattern : patterns) {
            int startIndex = json.indexOf(pattern);
            if (startIndex != -1) {
                startIndex += pattern.length();

                // Skip any whitespace
                while (startIndex < json.length() && Character.isWhitespace(json.charAt(startIndex))) {
                    startIndex++;
                }

                // Find the end of the number
                int endIndex = startIndex;
                while (endIndex < json.length() &&
                        (Character.isDigit(json.charAt(endIndex)) || json.charAt(endIndex) == '-')) {
                    endIndex++;
                }

                if (endIndex > startIndex) {
                    try {
                        return Integer.parseInt(json.substring(startIndex, endIndex));
                    } catch (NumberFormatException e) {
                        // Continue to next pattern
                    }
                }
            }
        }

        // Fallback: return 0 if field not found
        return 0;
    }
}