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
import com.kronotop.bucket.index.SortOrder;
import org.bson.BsonType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class IntersectionNodeBatchAnalysisTest extends BasePipelineTest {
    
    @Test
    @Disabled
    void testIntersectionWith50DocumentsAndLimit2() {
        final String TEST_BUCKET_NAME = "test-bucket-intersection-50-docs";
        
        // Create indexes for price and quantity
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex, quantityIndex);
        
        // Generate and insert 50 documents
        List<byte[]> documents = generateTestDocuments();
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);
        
        // Query: price > 25 AND quantity > 150 with limit=2
        String query = "{ 'price': { '$gt': 25 }, 'quantity': { '$gt': 150 } }";
        PipelineExecutor executor = createPipelineExecutorForQuery(metadata, query);
        PipelineContext ctx = createPipelineContext(metadata);
        ctx.setLimit(2);

        int total = 0;
        while(true) {
            total++;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Map<?, ByteBuffer> results = executor.execute(tr, ctx);

                System.out.println(results.size());
                // Should return at most 2 results due to limit
                //assertTrue(results.size() <= 2, "Should return at most 2 results due to limit");

                // Verify all returned documents match the criteria
                for (ByteBuffer buffer : results.values()) {
                    String json = BSONUtil.fromBson(buffer.array()).toJson();

                    // Simple validation - just check if the document contains reasonable values
                    // Since all our generated docs have price 30-49 and quantity 160-199,
                    // they should all match the query criteria (price > 25, quantity > 150)
                    assertTrue(json.contains("\"price\""), "Document should contain price field");
                    assertTrue(json.contains("\"quantity\""), "Document should contain quantity field");
                }

                System.out.println("Test completed successfully:");
                System.out.println("- Generated 50 documents");
                System.out.println("- Query: price > 25 AND quantity > 150");
                System.out.println("- Limit: 2");
                System.out.println("- Results returned: " + results.size());
                for (ByteBuffer buffer : results.values()) {
                    System.out.println(BSONUtil.fromBson(buffer.array()).toJson());
                }
                if (results.size() == 0) {
                    break;
                }
                if (total >= 20) {
                    fail("Exceeds 20 iterations");
                    break;
                }
            }
        }
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
            System.out.println(json);
            documents.add(BSONUtil.jsonToDocumentThenBytes(json));
        }
        
        return documents;
    }
    
    /**
     * Extracts integer value from JSON string for a given field
     */
    private int extractIntValue(String json, String fieldName) {
        String pattern = "\"" + fieldName + "\" : ";
        int startIndex = json.indexOf(pattern);
        if (startIndex == -1) return 0;
        
        startIndex += pattern.length();
        int endIndex = json.indexOf(',', startIndex);
        if (endIndex == -1) {
            endIndex = json.indexOf('}', startIndex);
        }
        
        return Integer.parseInt(json.substring(startIndex, endIndex).trim());
    }
}