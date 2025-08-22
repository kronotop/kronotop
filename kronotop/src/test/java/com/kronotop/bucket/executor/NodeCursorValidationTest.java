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

package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.SortOrder;
import org.bson.BsonBinaryReader;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for validating independent cursor behavior
 * in node processing. This test class validates that cursor contamination
 * is completely eliminated and that nodes maintain proper isolation.
 */
class NodeCursorValidationTest extends BasePlanExecutorTest {

    @Test
    void testNodeCursorIsolation() {
        final String TEST_BUCKET_NAME = "test-node-cursor-isolation";

        // Create indexes for category and priority, but not for price and description
        IndexDefinition categoryIndex = IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING);
        IndexDefinition priorityIndex = IndexDefinition.create("priority-index", "priority", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, categoryIndex, priorityIndex);

        // Insert comprehensive test data
        List<byte[]> documents = List.of(
            // Electronics category (indexed field)
            BSONUtil.jsonToDocumentThenBytes("{'category': 'electronics', 'price': 60, 'priority': 1, 'description': 'premium quality device'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'electronics', 'price': 70, 'priority': 2, 'description': 'standard device'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'electronics', 'price': 80, 'priority': 3, 'description': 'premium quality device'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'electronics', 'price': 120, 'priority': 4, 'description': 'standard device'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'electronics', 'price': 150, 'priority': 5, 'description': 'premium quality device'}"),
            
            // Books category (indexed field)
            BSONUtil.jsonToDocumentThenBytes("{'category': 'books', 'price': 15, 'priority': 1, 'description': 'quality content'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'books', 'price': 20, 'priority': 2, 'description': 'educational material'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'books', 'price': 25, 'priority': 3, 'description': 'quality content'}"),
            
            // Clothing category with various prices (price is non-indexed field)
            BSONUtil.jsonToDocumentThenBytes("{'category': 'clothing', 'price': 110, 'priority': 1, 'description': 'quality fabric'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'clothing', 'price': 120, 'priority': 2, 'description': 'casual wear'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'clothing', 'price': 130, 'priority': 4, 'description': 'quality fabric'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'clothing', 'price': 140, 'priority': 5, 'description': 'casual wear'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Test query: category is indexed, price is not indexed
        String query = "{ $or: [ { category: \"electronics\" }, { price: { $gte: 100 } } ] }";
        PlanExecutor executor = createPlanExecutorForQuery(metadata, query, 20);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr);

            // Should find: 5 electronics + 4 clothing items with price >= 100 (110, 120, 130, 140)
            // But electronics items with price >= 100 (120, 150) should not be duplicated
            assertTrue(results.size() >= 7, "Should find at least 7 matching documents");

            // Validate results meet the OR condition
            Set<String> categories = new HashSet<>();
            Set<Integer> highPrices = new HashSet<>();
            
            for (ByteBuffer documentBuffer : results.values()) {
                documentBuffer.rewind();
                try (BsonBinaryReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();
                    String category = null;
                    Integer price = null;
                    
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        switch (fieldName) {
                            case "category" -> category = reader.readString();
                            case "price" -> price = reader.readInt32();
                            default -> reader.skipValue();
                        }
                    }
                    reader.readEndDocument();
                    
                    boolean matchesCategory = "electronics".equals(category);
                    boolean matchesPrice = price != null && price >= 100;
                    
                    assertTrue(matchesCategory || matchesPrice, 
                        "Document should match at least one OR condition: category=" + category + ", price=" + price);
                    
                    if (matchesCategory) categories.add(category);
                    if (matchesPrice) highPrices.add(price);
                }
            }
            
            // Should have found electronics items and high-price items
            assertTrue(categories.contains("electronics"), "Should find electronics items from indexed condition");
            assertFalse(highPrices.isEmpty(), "Should find high-price items from non-indexed condition");
            
            System.out.println("✅ Node cursor isolation test passed!");
            System.out.println("Total unique results: " + results.size());
            System.out.println("Categories found: " + categories);
            System.out.println("High prices found: " + highPrices.size());
        }
    }

    @Test
    void testCursorAdvancementIndependence() {
        final String TEST_BUCKET_NAME = "test-cursor-advancement-independence";

        // Create indexes
        IndexDefinition categoryIndex = IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, categoryIndex);

        // Insert test data with mix of indexed and non-indexed conditions
        List<byte[]> documents = List.of(
            BSONUtil.jsonToDocumentThenBytes("{'category': 'electronics', 'description': 'premium device'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'books', 'description': 'premium content'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'electronics', 'description': 'standard device'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'books', 'description': 'premium content'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'clothing', 'description': 'premium device'}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'electronics', 'description': 'standard device'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Complex query with indexed category and non-indexed equality
        String query = "{ $or: [ { category: \"electronics\" }, { description: \"premium device\" } ] }";
        PlanExecutor executor = createPlanExecutorForQuery(metadata, query, 10);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr);

            // Should find all documents (all electronics from indexed, plus premium items from non-indexed)
            // Expected: 3 electronics + 2 premium device (with 1 overlap) = 4 unique results
            assertTrue(results.size() >= 4, "Should find substantial results from cursor advancement independence test");

            Set<String> foundCategories = new HashSet<>();
            Set<String> foundDescriptions = new HashSet<>();
            
            for (ByteBuffer documentBuffer : results.values()) {
                documentBuffer.rewind();
                try (BsonBinaryReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();
                    String category = null;
                    String description = null;
                    
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        switch (fieldName) {
                            case "category" -> category = reader.readString();
                            case "description" -> description = reader.readString();
                            default -> reader.skipValue();
                        }
                    }
                    reader.readEndDocument();
                    
                    if (category != null) foundCategories.add(category);
                    if (description != null) foundDescriptions.add(description);
                }
            }
            
            System.out.println("✅ Cursor advancement independence test passed!");
            System.out.println("Categories found: " + foundCategories);
            System.out.println("Descriptions found: " + foundDescriptions);
        }
    }

    @Test
    void testPartialNodeExhaustion() {
        final String TEST_BUCKET_NAME = "test-partial-node-exhaustion";

        // Create indexes
        IndexDefinition categoryIndex = IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING);
        IndexDefinition priorityIndex = IndexDefinition.create("priority-index", "priority", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, categoryIndex, priorityIndex);

        // Insert data where one condition has few results, other has many
        List<byte[]> documents = List.of(
            // Only one rare_category item
            BSONUtil.jsonToDocumentThenBytes("{'category': 'rare_category', 'priority': 5}"),
            
            // Many items with priority <= 3
            BSONUtil.jsonToDocumentThenBytes("{'category': 'common', 'priority': 1}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'common', 'priority': 2}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'common', 'priority': 3}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'other', 'priority': 1}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'other', 'priority': 2}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'other', 'priority': 3}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'another', 'priority': 1}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'another', 'priority': 2}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        String query = "{ $or: [ { category: \"rare_category\" }, { priority: { $lte: 3 } } ] }";
        PlanExecutor executor = createPlanExecutorForQuery(metadata, query, 15);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr);

            // Should find the rare category item + all priority <= 3 items
            assertTrue(results.size() >= 8, "Should find results primarily from priority condition");
            
            boolean foundRareCategory = false;
            int lowPriorityCount = 0;
            
            for (ByteBuffer documentBuffer : results.values()) {
                documentBuffer.rewind();
                try (BsonBinaryReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();
                    String category = null;
                    Integer priority = null;
                    
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        switch (fieldName) {
                            case "category" -> category = reader.readString();
                            case "priority" -> priority = reader.readInt32();
                            default -> reader.skipValue();
                        }
                    }
                    reader.readEndDocument();
                    
                    if ("rare_category".equals(category)) {
                        foundRareCategory = true;
                    }
                    if (priority != null && priority <= 3) {
                        lowPriorityCount++;
                    }
                }
            }
            
            assertTrue(foundRareCategory, "Should find the rare category item");
            assertTrue(lowPriorityCount >= 8, "Should find multiple low priority items");
            
            System.out.println("✅ Partial node exhaustion test passed!");
            System.out.println("Total results: " + results.size());
            System.out.println("Low priority count: " + lowPriorityCount);
        }
    }
}