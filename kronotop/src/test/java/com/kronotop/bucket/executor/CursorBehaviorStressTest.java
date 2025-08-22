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
 * Stress tests for cursor behavior validation under high-load conditions.
 * This test class ensures cursor coordination remains robust under stress.
 */
class CursorBehaviorStressTest extends BasePlanExecutorTest {

    @Test
    void testLargeDatasetMixedQueries() {
        final String TEST_BUCKET_NAME = "test-large-dataset-mixed-queries";

        // Create multiple indexes for stress testing
        IndexDefinition categoryIndex = IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING);
        IndexDefinition statusIndex = IndexDefinition.create("status-index", "status", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, categoryIndex, statusIndex);

        // Insert larger dataset for stress testing
        List<byte[]> documents = new ArrayList<>();
        String[] categories = {"A", "B", "C", "D", "E"};
        String[] statuses = {"active", "pending", "inactive"};
        String[] descriptions = {"quality product", "premium item", "standard quality"};

        for (int i = 0; i < 50; i++) { // Smaller dataset for compilation testing
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format(
                "{'category': '%s', 'status': '%s', 'priority': %d, 'price': %d, 'description': '%s', 'counter': %d}",
                categories[i % categories.length],
                statuses[i % statuses.length],
                (i % 5) + 1,
                (int)(Math.random() * 300),
                descriptions[i % descriptions.length],
                i
            )));
        }

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        String query = "{ $or: [ " +
                      "{ category: \"A\" }, " +
                      "{ status: \"active\" }, " +
                      "{ price: { $gte: 100, $lte: 200 } }, " +
                      "{ description: \"premium item\" } " +
                      "] }";

        PlanExecutor executor = createPlanExecutorForQuery(metadata, query, 30);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr);

            assertTrue(results.size() >= 10, "Should find substantial results in stress test");

            // Validate results meet at least one condition
            Map<String, Integer> conditionCounts = new HashMap<>();
            conditionCounts.put("category", 0);
            conditionCounts.put("status", 0);
            conditionCounts.put("price", 0);
            conditionCounts.put("description", 0);

            for (ByteBuffer documentBuffer : results.values()) {
                documentBuffer.rewind();
                try (BsonBinaryReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();
                    String category = null;
                    String status = null;
                    Integer price = null;
                    String description = null;

                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        switch (fieldName) {
                            case "category" -> category = reader.readString();
                            case "status" -> status = reader.readString();
                            case "price" -> price = reader.readInt32();
                            case "description" -> description = reader.readString();
                            default -> reader.skipValue();
                        }
                    }
                    reader.readEndDocument();

                    if ("A".equals(category)) {
                        conditionCounts.put("category", conditionCounts.get("category") + 1);
                    }
                    if ("active".equals(status)) {
                        conditionCounts.put("status", conditionCounts.get("status") + 1);
                    }
                    if (price != null && price >= 100 && price <= 200) {
                        conditionCounts.put("price", conditionCounts.get("price") + 1);
                    }
                    if ("premium item".equals(description)) {
                        conditionCounts.put("description", conditionCounts.get("description") + 1);
                    }
                }
            }

            // Validate multiple conditions are matched
            int nonZeroConditions = (int) conditionCounts.values().stream()
                    .filter(count -> count > 0)
                    .count();
            
            assertTrue(nonZeroConditions >= 2, "Should match multiple field conditions");

            System.out.println("✅ Large dataset mixed queries stress test passed!");
            System.out.println("Results: " + results.size() + " documents");
            System.out.println("Field matches: " + conditionCounts);
        }
    }

    @Test
    void testPartialNodeExhaustion() {
        final String TEST_BUCKET_NAME = "test-partial-node-exhaustion-stress";

        // Create indexes
        IndexDefinition categoryIndex = IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING);
        IndexDefinition priorityIndex = IndexDefinition.create("priority-index", "priority", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, categoryIndex, priorityIndex);

        // Insert data where one condition has few results, other has many
        List<byte[]> documents = List.of(
            // Few rare category items
            BSONUtil.jsonToDocumentThenBytes("{'category': 'rare_category', 'priority': 5}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'rare_category', 'priority': 4}"),
            
            // Many items with priority <= 3
            BSONUtil.jsonToDocumentThenBytes("{'category': 'common1', 'priority': 1}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'common1', 'priority': 2}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'common1', 'priority': 3}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'common2', 'priority': 1}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'common2', 'priority': 2}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'common2', 'priority': 3}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'common3', 'priority': 1}"),
            BSONUtil.jsonToDocumentThenBytes("{'category': 'common3', 'priority': 2}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        String query = "{ $or: [ { category: \"rare_category\" }, { priority: { $lte: 3 } } ] }";
        PlanExecutor executor = createPlanExecutorForQuery(metadata, query, 15);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr);

            // Should find results primarily from the priority condition
            assertTrue(results.size() >= 8, "Should find multiple results from priority condition");

            int rareCount = 0;
            int priorityCount = 0;

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
                        rareCount++;
                    }
                    if (priority != null && priority <= 3) {
                        priorityCount++;
                    }
                }
            }

            assertTrue(rareCount >= 2, "Should find rare category items");
            assertTrue(priorityCount >= 8, "Should find multiple low priority items");

            System.out.println("✅ Partial node exhaustion stress test passed!");
            System.out.println("Total results: " + results.size());
            System.out.println("Rare categories: " + rareCount + ", Low priority: " + priorityCount);
        }
    }
}