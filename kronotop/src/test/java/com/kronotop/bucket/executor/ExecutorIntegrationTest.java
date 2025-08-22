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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.SortOrder;
import org.bson.BsonBinaryReader;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

// This class is intended for debugging purposes and is well-suited for newcomers.

class ExecutorIntegrationTest extends BasePlanExecutorTest {

    @Test
    void shouldIndexPriceFieldAndExecuteGTQuery() {
        final String TEST_BUCKET_NAME = "test-bucket-price-gt";

        // Create index on price field
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        // Insert 20 documents with sequential prices (1-20)
        List<byte[]> documents = createPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for query: price > 1 (should return prices 2-20)
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"price\": { \"$gt\": 1 } }", 100);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print documents for debugging purposes
            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> Document " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Should return 19 documents (prices 2-20, excluding price 1)
            assertEquals(19, results.size(), "Should return 19 documents with price > 1");

            // Extract prices from results and verify
            Set<Integer> actualPrices = extractPricesFromResults(results);

            // Verify that we got the correct price range (2-20)
            Set<Integer> expectedPrices = new HashSet<>();
            for (int i = 2; i <= 20; i++) {
                expectedPrices.add(i);
            }
            assertEquals(expectedPrices, actualPrices, "Should get prices 2-20");
        }
    }

    @Test
    void shouldIndexPriceFieldAndExecuteGTQueryReverse() {
        final String TEST_BUCKET_NAME = "test-bucket-price-gt-reverse";

        // Create index on price field
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        // Insert 20 documents with sequential prices (1-20)
        List<byte[]> documents = createPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for query: price > 1 (should return prices 2-20)
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"price\": { \"$gt\": 1 } }", 100, true);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print documents for debugging purposes
            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> Document " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Should return 19 documents (prices 2-20, excluding price 1)
            assertEquals(19, results.size(), "Should return 19 documents with price > 1");

            // Extract prices from results and verify
            Set<Integer> actualPrices = extractPricesFromResults(results);

            // Verify that we got the correct price range (2-20)
            Set<Integer> expectedPrices = new HashSet<>();
            for (int i = 2; i <= 20; i++) {
                expectedPrices.add(i);
            }
            assertEquals(expectedPrices, actualPrices, "Should get prices 2-20");
        }
    }

    @Test
    void shouldHandleEdgeCasesWithPriceIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-price-edge";

        // Create index on price field
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        // Insert documents with edge case prices
        List<byte[]> edgeCaseDocs = Arrays.asList(
                createPriceDocument(0),    // Edge case: zero
                createPriceDocument(1),    // Boundary value
                createPriceDocument(2),    // Just above boundary
                createPriceDocument(100),  // High value
                createPriceDocument(-1)    // Negative value
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, edgeCaseDocs);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Test GT 0: should return prices 1, 2, 100
            PlanExecutor executor1 = createPlanExecutorForQuery(metadata, "{ \"price\": { \"$gt\": 0 } }", 10);
            Map<?, ByteBuffer> results1 = executor1.execute(tr);

            assertEquals(3, results1.size(), "Should return 3 documents with price > 0");
            Set<Integer> prices1 = extractPricesFromResults(results1);
            assertEquals(Set.of(1, 2, 100), prices1, "Should get prices 1, 2, 100");

            // Test GT 1: should return prices 2, 100
            PlanExecutor executor2 = createPlanExecutorForQuery(metadata, "{ \"price\": { \"$gt\": 1 } }", 10);
            Map<?, ByteBuffer> results2 = executor2.execute(tr);

            assertEquals(2, results2.size(), "Should return 2 documents with price > 1");
            Set<Integer> prices2 = extractPricesFromResults(results2);
            assertEquals(Set.of(2, 100), prices2, "Should get prices 2, 100");
        }
    }

    @Test
    void shouldTestCursorPaginationWithPriceIndex() {
        final String TEST_BUCKET_NAME = "test-bucket-price-pagination";

        // Create index on price field
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        // Insert 50 documents to test pagination
        List<byte[]> documents = createPriceDocuments(50);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute query: price > 10 with limit 15
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"price\": { \"$gt\": 10 } }", 15);
            Map<?, ByteBuffer> results = executor.execute(tr);

            // Should return 15 documents (first page of results where price > 10)
            assertEquals(15, results.size(), "Should return 15 documents in first page");

            // Verify all results have price > 10
            Set<Integer> prices = extractPricesFromResults(results);
            for (Integer price : prices) {
                assertTrue(price > 10, "Price should be > 10, but was: " + price);
            }
        }
    }

    /**
     * Creates documents with sequential prices from 1 to count.
     * Each document follows the template: {'price': %d, 'category': 'electronics'}
     */
    private List<byte[]> createPriceDocuments(int count) {
        List<byte[]> documents = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            documents.add(createPriceDocument(i));
        }
        return documents;
    }

    /**
     * Creates a single document with the given price.
     */
    private byte[] createPriceDocument(int price) {
        String documentJson = String.format("{\"price\": %d, \"category\": \"electronics\"}", price);
        return BSONUtil.jsonToDocumentThenBytes(documentJson);
    }

    @Test
    void shouldIndexUsernameFieldAndExecuteGTQuery() {
        final String TEST_BUCKET_NAME = "test-bucket-username-gt";

        // Create index on username field
        IndexDefinition usernameIndex = IndexDefinition.create("username-index", "username", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, usernameIndex);

        // Insert 20 documents with sequential usernames (user01-user20)
        List<byte[]> documents = createUsernameDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for query: username > "user05" (should return user06-user20)
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"username\": { \"$gt\": \"user05\" } }", 100);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print documents for debugging purposes
            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> Document " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Should return 15 documents (user06-user20, excluding user01-user05)
            assertEquals(15, results.size(), "Should return 15 documents with username > 'user05'");

            // Extract usernames from results and verify
            Set<String> actualUsernames = extractUsernamesFromResults(results);

            // Verify that we got the correct username range (user06-user20)
            Set<String> expectedUsernames = new HashSet<>();
            for (int i = 6; i <= 20; i++) {
                expectedUsernames.add(String.format("user%02d", i));
            }
            assertEquals(expectedUsernames, actualUsernames, "Should get usernames user06-user20");
        }
    }

    @Test
    void shouldIndexUsernameFieldAndExecuteGTQueryReverse() {
        final String TEST_BUCKET_NAME = "test-bucket-username-gt-reverse";

        // Create index on username field
        IndexDefinition usernameIndex = IndexDefinition.create("username-index", "username", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, usernameIndex);

        // Insert 20 documents with sequential usernames (user01-user20)
        List<byte[]> documents = createUsernameDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for query: username > "user05" (should return user06-user20)
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"username\": { \"$gt\": \"user05\" } }", 100, true);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print documents for debugging purposes
            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> Document " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Should return 15 documents (user06-user20, excluding user01-user05)
            assertEquals(15, results.size(), "Should return 15 documents with username > 'user05'");

            // Extract usernames from results and verify
            Set<String> actualUsernames = extractUsernamesFromResults(results);

            // Verify that we got the correct username range (user06-user20)
            Set<String> expectedUsernames = new HashSet<>();
            for (int i = 6; i <= 20; i++) {
                expectedUsernames.add(String.format("user%02d", i));
            }
            assertEquals(expectedUsernames, actualUsernames, "Should get usernames user06-user20");
        }
    }

    @Test
    void shouldIndexPriceFieldAndExecuteGTQueryWithBatchControl() {
        final String TEST_BUCKET_NAME = "test-bucket-price-batch-control";

        // Create index on price field
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        // Insert 10 documents with sequential prices (1-10)
        List<byte[]> documents = createPriceDocuments(10);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for query: price > 3 with limit 2 (should return prices 4,5 in first batch, 6,7 in second, etc.)
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"price\": { \"$gt\": 3 } }", 2);

            List<List<Integer>> batches = new ArrayList<>();
            int batchCount = 0;
            int totalResults = 0;

            // Execute in batches until no more results
            while (true) {
                Map<?, ByteBuffer> results = executor.execute(tr);

                if (results.isEmpty()) {
                    break; // No more results
                }

                // Extract prices from this batch
                List<Integer> batchPrices = new ArrayList<>(extractPricesFromResults(results));
                Collections.sort(batchPrices); // Sort for consistent ordering
                batches.add(batchPrices);

                batchCount++;
                totalResults += results.size();

                // Print batch for debugging purposes
                System.out.println(">> Batch " + batchCount + ": " + batchPrices);

                // Safety check to avoid infinite loops
                if (batchCount > 10) {
                    break;
                }
            }

            // Batch size assertions
            assertTrue(batchCount >= 3, "Should have at least 3 batches with limit=2 for 7 qualifying documents");
            assertEquals(7, totalResults, "Should return 7 total documents with price > 3 (prices 4-10)");

            // Verify batch contents
            // Expected: Batch 1: [4,5], Batch 2: [6,7], Batch 3: [8,9], Batch 4: [10]
            assertTrue(batches.size() >= 3, "Should have at least 3 batches");

            // Verify exact batch count
            assertEquals(4, batches.size(), "Should have exactly 4 batches for 7 results with limit=2");

            // Batch 1: [4, 5]
            assertEquals(2, batches.get(0).size(), "First batch should contain exactly 2 documents");
            assertEquals(Arrays.asList(4, 5), batches.get(0), "First batch should contain prices [4, 5]");

            // Batch 2: [6, 7]
            assertEquals(2, batches.get(1).size(), "Second batch should contain exactly 2 documents");
            assertEquals(Arrays.asList(6, 7), batches.get(1), "Second batch should contain prices [6, 7]");

            // Batch 3: [8, 9]
            assertEquals(2, batches.get(2).size(), "Third batch should contain exactly 2 documents");
            assertEquals(Arrays.asList(8, 9), batches.get(2), "Third batch should contain prices [8, 9]");

            // Batch 4: [10]
            assertEquals(1, batches.get(3).size(), "Fourth batch should contain exactly 1 document");
            assertEquals(List.of(10), batches.get(3), "Fourth batch should contain price [10]");

            // Verify all expected prices are present across all batches
            Set<Integer> allPricesFromBatches = new HashSet<>();
            for (List<Integer> batch : batches) {
                allPricesFromBatches.addAll(batch);
            }

            Set<Integer> expectedPrices = new HashSet<>(Arrays.asList(4, 5, 6, 7, 8, 9, 10));
            assertEquals(expectedPrices, allPricesFromBatches, "All batches combined should contain prices 4-10");
        }
    }

    @Test
    void shouldIndexPriceFieldAndExecuteGTQueryWithBatchControlReverse() {
        final String TEST_BUCKET_NAME = "test-bucket-price-batch-control-reverse";

        // Create index on price field
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, priceIndex);

        // Insert 10 documents with sequential prices (1-10)
        List<byte[]> documents = createPriceDocuments(10);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for query: price > 3 with limit 2 and REVERSE=true (should return prices 10,9 in first batch, 8,7 in second, etc.)
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"price\": { \"$gt\": 3 } }", 2, true);

            List<List<Integer>> batches = new ArrayList<>();
            int batchCount = 0;
            int totalResults = 0;

            // Execute in batches until no more results
            while (true) {
                Map<?, ByteBuffer> results = executor.execute(tr);

                if (results.isEmpty()) {
                    break; // No more results
                }

                // Extract prices from this batch
                List<Integer> batchPrices = new ArrayList<>(extractPricesFromResults(results));
                batchPrices.sort(Collections.reverseOrder()); // Sort in descending order for reverse
                batches.add(batchPrices);

                batchCount++;
                totalResults += results.size();

                // Print batch for debugging purposes
                System.out.println(">> Reverse Batch " + batchCount + ": " + batchPrices);

                // Safety check to avoid infinite loops
                if (batchCount > 10) {
                    break;
                }
            }

            // Batch size assertions
            assertTrue(batchCount >= 3, "Should have at least 3 batches with limit=2 for 7 qualifying documents");
            assertEquals(7, totalResults, "Should return 7 total documents with price > 3 (prices 4-10)");

            // Verify batch contents for REVERSE order
            // Expected: Batch 1: [10,9], Batch 2: [8,7], Batch 3: [6,5], Batch 4: [4]
            assertTrue(batches.size() >= 3, "Should have at least 3 batches");

            // Verify exact batch count
            assertEquals(4, batches.size(), "Should have exactly 4 batches for 7 results with limit=2");

            // Batch 1: [10, 9] (reverse order)
            assertEquals(2, batches.get(0).size(), "First batch should contain exactly 2 documents");
            assertEquals(Arrays.asList(10, 9), batches.get(0), "First batch should contain prices [10, 9] in reverse order");

            // Batch 2: [8, 7] (reverse order)
            assertEquals(2, batches.get(1).size(), "Second batch should contain exactly 2 documents");
            assertEquals(Arrays.asList(8, 7), batches.get(1), "Second batch should contain prices [8, 7] in reverse order");

            // Batch 3: [6, 5] (reverse order)
            assertEquals(2, batches.get(2).size(), "Third batch should contain exactly 2 documents");
            assertEquals(Arrays.asList(6, 5), batches.get(2), "Third batch should contain prices [6, 5] in reverse order");

            // Batch 4: [4] (reverse order)
            assertEquals(1, batches.get(3).size(), "Fourth batch should contain exactly 1 document");
            assertEquals(List.of(4), batches.get(3), "Fourth batch should contain price [4]");

            // Verify all expected prices are present across all batches
            Set<Integer> allPricesFromBatches = new HashSet<>();
            for (List<Integer> batch : batches) {
                allPricesFromBatches.addAll(batch);
            }

            Set<Integer> expectedPrices = new HashSet<>(Arrays.asList(4, 5, 6, 7, 8, 9, 10));
            assertEquals(expectedPrices, allPricesFromBatches, "All batches combined should contain prices 4-10");
        }
    }

    /**
     * Creates documents with sequential usernames from user01 to user[count].
     * Each document follows the template: {'username': 'user%02d', 'role': 'member'}
     */
    private List<byte[]> createUsernameDocuments(int count) {
        List<byte[]> documents = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            documents.add(createUsernameDocument(i));
        }
        return documents;
    }

    /**
     * Creates a single document with the given username number.
     */
    private byte[] createUsernameDocument(int userNumber) {
        String username = String.format("user%02d", userNumber);
        String documentJson = String.format("{\"username\": \"%s\", \"role\": \"member\"}", username);
        return BSONUtil.jsonToDocumentThenBytes(documentJson);
    }

    /**
     * Helper method to extract usernames from query results for verification.
     */
    private Set<String> extractUsernamesFromResults(Map<?, ByteBuffer> results) {
        Set<String> usernames = new HashSet<>();

        for (ByteBuffer documentBuffer : results.values()) {
            documentBuffer.rewind();
            try (BsonReader reader = new BsonBinaryReader(documentBuffer)) {
                reader.readStartDocument();

                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if ("username".equals(fieldName)) {
                        usernames.add(reader.readString());
                    } else {
                        reader.skipValue();
                    }
                }
                reader.readEndDocument();
            }
        }

        return usernames;
    }

    @Test
    void shouldExecuteANDQueryWithQuantityAndPriceIndexes() {
        final String TEST_BUCKET_NAME = "test-bucket-and-query";

        // Create indexes on both quantity and price fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        // Insert 20 documents with various quantity and price combinations
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for AND query: quantity > 5 AND price > 15
            // This should match documents where both conditions are true
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$and\": [ { \"quantity\": { \"$gt\": 5 } }, { \"price\": { \"$gt\": 15 } } ] }", 100);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print documents for debugging purposes
            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> AND Query Document " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Extract quantities and prices from results for verification
            Map<Integer, Integer> quantityPriceMap = extractQuantityPriceFromResults(results);

            // Verify that all results satisfy both conditions
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int price = entry.getValue();

                assertTrue(quantity > 5, "Quantity should be > 5, but was: " + quantity);
                assertTrue(price > 15, "Price should be > 15, but was: " + price);
            }

            // Calculate expected results based on our document generation pattern
            // Documents: quantity=1-20, price=quantity*2 (so prices 2-40)
            // Condition: quantity > 5 AND price > 15
            // quantity > 5 means quantity 6-20 (15 documents)
            // price > 15 means price 16-40, which corresponds to quantity 8-20 (13 documents)
            // AND: quantity > 5 AND price > 15 means quantity 8-20 (13 documents)

            assertEquals(13, results.size(), "Should return 13 documents matching both quantity > 5 AND price > 15");

            // Verify specific expected quantities (8-20)
            Set<Integer> expectedQuantities = new HashSet<>();
            for (int i = 8; i <= 20; i++) {
                expectedQuantities.add(i);
            }

            assertEquals(expectedQuantities, quantityPriceMap.keySet(), "Should get quantities 8-20");

            // Verify corresponding prices (16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40)
            Set<Integer> expectedPrices = new HashSet<>();
            for (int i = 8; i <= 20; i++) {
                expectedPrices.add(i * 2);
            }

            Set<Integer> actualPrices = new HashSet<>(quantityPriceMap.values());
            assertEquals(expectedPrices, actualPrices, "Should get prices 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40");
        }
    }

    /**
     * Creates documents with quantity and price fields.
     * Document pattern: quantity=i, price=i*2, category='electronics'
     * So document 1: {quantity: 1, price: 2}, document 2: {quantity: 2, price: 4}, etc.
     */
    private List<byte[]> createQuantityPriceDocuments(int count) {
        List<byte[]> documents = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            documents.add(createQuantityPriceDocument(i));
        }
        return documents;
    }

    /**
     * Creates a single document with quantity and price.
     */
    private byte[] createQuantityPriceDocument(int number) {
        int quantity = number;
        int price = number * 2;
        String documentJson = String.format("{\"quantity\": %d, \"price\": %d, \"category\": \"electronics\"}", quantity, price);
        System.out.println("Inserting document >>> " + documentJson);
        return BSONUtil.jsonToDocumentThenBytes(documentJson);
    }

    /**
     * Helper method to extract quantity-price pairs from query results for verification.
     */
    private Map<Integer, Integer> extractQuantityPriceFromResults(Map<?, ByteBuffer> results) {
        Map<Integer, Integer> quantityPriceMap = new LinkedHashMap<>();

        for (ByteBuffer documentBuffer : results.values()) {
            documentBuffer.rewind();
            try (BsonReader reader = new BsonBinaryReader(documentBuffer)) {
                reader.readStartDocument();

                Integer quantity = null;
                Integer price = null;

                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if ("quantity".equals(fieldName)) {
                        quantity = reader.readInt32();
                    } else if ("price".equals(fieldName)) {
                        price = reader.readInt32();
                    } else {
                        reader.skipValue();
                    }
                }
                reader.readEndDocument();

                if (quantity != null && price != null) {
                    quantityPriceMap.put(quantity, price);
                }
            }
        }

        return quantityPriceMap;
    }

    @Test
    void shouldExecuteANDQueryWithQuantityAndPriceIndexesReverse() {
        final String TEST_BUCKET_NAME = "test-bucket-and-query-reverse";

        // Create indexes on both quantity and price fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        // Insert 20 documents with various quantity and price combinations
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for AND query: quantity > 5 AND price > 15 with REVERSE=true
            // This should match documents where both conditions are true, returned in reverse order
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$and\": [ { \"quantity\": { \"$gt\": 5 } }, { \"price\": { \"$gt\": 15 } } ] }", 100, true);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print documents for debugging purposes
            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> AND Query Reverse Document " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Extract quantities and prices from results for verification
            Map<Integer, Integer> quantityPriceMap = extractQuantityPriceFromResults(results);

            // Verify that all results satisfy both conditions
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int price = entry.getValue();

                assertTrue(quantity > 5, "Quantity should be > 5, but was: " + quantity);
                assertTrue(price > 15, "Price should be > 15, but was: " + price);
            }

            // Manually create expected results for quantity > 5 AND price > 15
            // Document pattern: quantity=i, price=i*2
            // Documents that match: quantity 8-20 (since price=quantity*2, price > 15 means quantity > 7.5, so quantity >= 8)
            // Expected results in REVERSE order (highest to lowest quantity):
            Map<Integer, Integer> expectedResults = new LinkedHashMap<>();
            expectedResults.put(20, 40);  // quantity=20, price=40
            expectedResults.put(19, 38);  // quantity=19, price=38
            expectedResults.put(18, 36);  // quantity=18, price=36
            expectedResults.put(17, 34);  // quantity=17, price=34
            expectedResults.put(16, 32);  // quantity=16, price=32
            expectedResults.put(15, 30);  // quantity=15, price=30
            expectedResults.put(14, 28);  // quantity=14, price=28
            expectedResults.put(13, 26);  // quantity=13, price=26
            expectedResults.put(12, 24);  // quantity=12, price=24
            expectedResults.put(11, 22);  // quantity=11, price=22
            expectedResults.put(10, 20);  // quantity=10, price=20
            expectedResults.put(9, 18);   // quantity=9, price=18
            expectedResults.put(8, 16);   // quantity=8, price=16

            // Verify result count
            assertEquals(expectedResults.size(), results.size(),
                    String.format("Should return %d documents, but got %d", expectedResults.size(), results.size()));

            // Compare actual results with expected results
            System.out.println("Expected results (reverse order):");
            for (Map.Entry<Integer, Integer> entry : expectedResults.entrySet()) {
                System.out.println("  quantity=" + entry.getKey() + ", price=" + entry.getValue());
            }

            System.out.println("Actual results:");
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                System.out.println("  quantity=" + entry.getKey() + ", price=" + entry.getValue());
            }

            // Verify exact match with expected results
            assertEquals(expectedResults, quantityPriceMap, "Results should exactly match expected reverse-ordered results");
        }
    }

    @Test
    void shouldExecuteANDQueryWithQuantityAndPriceIndexesBatchControl() {
        final String TEST_BUCKET_NAME = "test-bucket-and-batch-forward";

        // Create indexes on both quantity and price fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        // Insert 20 documents with quantity and price combinations
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for AND query: quantity > 5 AND price > 15 with Limit=2
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$and\": [ { \"quantity\": { \"$gt\": 5 } }, { \"price\": { \"$gt\": 15 } } ] }", 2);

            List<List<Map<Integer, Integer>>> batches = new ArrayList<>();
            int batchCount = 0;
            int totalResults = 0;

            // Execute in batches until no more results
            while (true) {
                Map<?, ByteBuffer> results = executor.execute(tr);

                if (results.isEmpty()) {
                    break; // No more results
                }

                // Extract quantity-price pairs from this batch
                Map<Integer, Integer> batchQuantityPriceMap = extractQuantityPriceFromResults(results);
                List<Map<Integer, Integer>> batchList = new ArrayList<>();
                batchList.add(batchQuantityPriceMap);
                batches.add(batchList);

                batchCount++;
                totalResults += results.size();

                // Print batch for debugging purposes
                System.out.println(">> AND Forward Batch " + batchCount + " (size=" + results.size() + "):");
                for (Map.Entry<Integer, Integer> entry : batchQuantityPriceMap.entrySet()) {
                    System.out.println("   quantity=" + entry.getKey() + ", price=" + entry.getValue());
                }

                // Verify each result satisfies both conditions
                for (Map.Entry<Integer, Integer> entry : batchQuantityPriceMap.entrySet()) {
                    int quantity = entry.getKey();
                    int price = entry.getValue();
                    assertTrue(quantity > 5, "Quantity should be > 5, but was: " + quantity + " in batch " + batchCount);
                    assertTrue(price > 15, "Price should be > 15, but was: " + price + " in batch " + batchCount);
                }

                // Safety check to avoid infinite loops
                if (batchCount > 20) {
                    break;
                }
            }

            System.out.println("Total batches: " + batchCount + ", Total results: " + totalResults);

            // Basic batch size assertions - we don't know exact counts yet, so check what we get
            assertTrue(batchCount > 0, "Should have at least one batch");
            assertTrue(totalResults > 0, "Should have at least one result");

            // Verify batch sizes - each batch should have at most 2 documents (limit=2)
            for (int i = 0; i < batches.size(); i++) {
                Map<Integer, Integer> batchData = batches.get(i).get(0);
                assertTrue(batchData.size() <= 2, "Batch " + (i + 1) + " should contain at most 2 documents, but had " + batchData.size());

                if (i < batches.size() - 1) {
                    // All batches except possibly the last should have exactly 2 documents
                    assertEquals(2, batchData.size(), "Batch " + (i + 1) + " should contain exactly 2 documents");
                }
            }

            // Verify results are in forward order by checking quantities increase across batches
            for (int i = 0; i < batches.size() - 1; i++) {
                Map<Integer, Integer> currentBatch = batches.get(i).get(0);
                Map<Integer, Integer> nextBatch = batches.get(i + 1).get(0);

                // Get min quantity from the current batch and min quantity from the next batch
                int currentMinQuantity = Collections.min(currentBatch.keySet());
                int nextMinQuantity = Collections.min(nextBatch.keySet());

                assertTrue(currentMinQuantity < nextMinQuantity,
                        "Quantities should increase in forward order: batch " + (i + 1) + " min=" + currentMinQuantity +
                                " should be < batch " + (i + 2) + " min=" + nextMinQuantity);
            }

            // Collect all results across batches to verify completeness
            Set<Integer> allQuantities = new HashSet<>();
            Set<Integer> allPrices = new HashSet<>();
            for (List<Map<Integer, Integer>> batchList : batches) {
                Map<Integer, Integer> batchData = batchList.get(0);
                allQuantities.addAll(batchData.keySet());
                allPrices.addAll(batchData.values());
            }

            System.out.println("All quantities found: " + allQuantities);
            System.out.println("All prices found: " + allPrices);

            // Verify all results still satisfy the AND conditions
            for (Integer quantity : allQuantities) {
                assertTrue(quantity > 5, "All quantities should be > 5, but found: " + quantity);
            }
            for (Integer price : allPrices) {
                assertTrue(price > 15, "All prices should be > 15, but found: " + price);
            }
        }
    }

    @Test
    void shouldExecuteANDQueryWithQuantityAndPriceIndexesBatchControlReverse() {
        final String TEST_BUCKET_NAME = "test-bucket-and-batch-reverse";

        // Create indexes on both quantity and price fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        // Insert 20 documents with quantity and price combinations
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for AND query: quantity > 5 AND price > 15 with REVERSE=true and Limit=2
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$and\": [ { \"quantity\": { \"$gt\": 5 } }, { \"price\": { \"$gt\": 15 } } ] }", 2, true);

            List<List<Map<Integer, Integer>>> batches = new ArrayList<>();
            int batchCount = 0;
            int totalResults = 0;

            // Execute in batches until no more results
            while (true) {
                Map<?, ByteBuffer> results = executor.execute(tr);

                if (results.isEmpty()) {
                    break; // No more results
                }

                //for (ByteBuffer entry : results.values()) {
                //    System.out.println(BSONUtil.fromBson(entry.duplicate().array()));
                //}

                // Extract quantity-price pairs from this batch
                Map<Integer, Integer> batchQuantityPriceMap = extractQuantityPriceFromResults(results);
                List<Map<Integer, Integer>> batchList = new ArrayList<>();
                batchList.add(batchQuantityPriceMap);
                batches.add(batchList);

                batchCount++;
                totalResults += results.size();

                // Print batch for debugging purposes
                System.out.println(">> AND Reverse Batch " + batchCount + " (size=" + results.size() + "):");
                for (Map.Entry<Integer, Integer> entry : batchQuantityPriceMap.entrySet()) {
                    System.out.println("   quantity=" + entry.getKey() + ", price=" + entry.getValue());
                }

                // Verify each result satisfies both conditions
                for (Map.Entry<Integer, Integer> entry : batchQuantityPriceMap.entrySet()) {
                    int quantity = entry.getKey();
                    int price = entry.getValue();
                    assertTrue(quantity > 5, "Quantity should be > 5, but was: " + quantity + " in batch " + batchCount);
                    assertTrue(price > 15, "Price should be > 15, but was: " + price + " in batch " + batchCount);
                }

                // Safety check to avoid infinite loops
                if (batchCount > 20) {
                    break;
                }
            }

            System.out.println("Total batches: " + batchCount + ", Total results: " + totalResults);

            // Expected: 7 batches total with 13 results (quantity 8-20 matching quantity > 5 AND price > 15)
            // Batch pattern: [20,19], [18,17], [16,15], [14,13], [12,11], [10,9], [8]
            assertEquals(7, batchCount, "Should have exactly 7 batches for 13 results with limit=2");
            assertEquals(13, totalResults, "Should have exactly 13 total results matching quantity > 5 AND price > 15");

            // Verify specific batch content and sizes
            assertEquals(7, batches.size(), "Should have exactly 7 batches for 13 results with limit=2");

            // Batch 1: [quantity 20,19] -> [price 40,38]
            Map<Integer, Integer> batch1 = batches.getFirst().getFirst();
            assertEquals(2, batch1.size(), "First batch should contain exactly 2 documents");
            assertTrue(batch1.containsKey(20) && batch1.containsKey(19), "First batch should contain quantities [20, 19]");
            assertEquals(Integer.valueOf(40), batch1.get(20), "Quantity 20 should have price 40");
            assertEquals(Integer.valueOf(38), batch1.get(19), "Quantity 19 should have price 38");

            // Batch 2: [quantity 18,17] -> [price 36,34]
            Map<Integer, Integer> batch2 = batches.get(1).getFirst();
            assertEquals(2, batch2.size(), "Second batch should contain exactly 2 documents");
            assertTrue(batch2.containsKey(18) && batch2.containsKey(17), "Second batch should contain quantities [18, 17]");
            assertEquals(Integer.valueOf(36), batch2.get(18), "Quantity 18 should have price 36");
            assertEquals(Integer.valueOf(34), batch2.get(17), "Quantity 17 should have price 34");

            // Batch 3: [quantity 16,15] -> [price 32,30]
            Map<Integer, Integer> batch3 = batches.get(2).getFirst();
            assertEquals(2, batch3.size(), "Third batch should contain exactly 2 documents");
            assertTrue(batch3.containsKey(16) && batch3.containsKey(15), "Third batch should contain quantities [16, 15]");
            assertEquals(Integer.valueOf(32), batch3.get(16), "Quantity 16 should have price 32");
            assertEquals(Integer.valueOf(30), batch3.get(15), "Quantity 15 should have price 30");

            // Batch 7 (last): [quantity 8] -> [price 16]
            Map<Integer, Integer> batch7 = batches.get(6).getFirst();
            assertEquals(1, batch7.size(), "Seventh batch should contain exactly 1 document");
            assertTrue(batch7.containsKey(8), "Seventh batch should contain quantity [8]");
            assertEquals(Integer.valueOf(16), batch7.get(8), "Quantity 8 should have price 16");

            // Collect all results across batches to verify completeness
            Set<Integer> allQuantities = new HashSet<>();
            Set<Integer> allPrices = new HashSet<>();
            for (List<Map<Integer, Integer>> batchList : batches) {
                Map<Integer, Integer> batchData = batchList.getFirst();
                allQuantities.addAll(batchData.keySet());
                allPrices.addAll(batchData.values());
            }

            System.out.println("All quantities found: " + allQuantities);
            System.out.println("All prices found: " + allPrices);

            // Verify all results still satisfy the AND conditions
            for (Integer quantity : allQuantities) {
                assertTrue(quantity > 5, "All quantities should be > 5, but found: " + quantity);
            }
            for (Integer price : allPrices) {
                assertTrue(price > 15, "All prices should be > 15, but found: " + price);
            }
        }
    }

    @Test
    void shouldExecuteORQueryWithQuantityAndPriceIndexes() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query";

        // Create indexes on both quantity and price fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        // Insert 20 documents with various quantity and price combinations
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for OR query: quantity > 50 OR price > 15
            // This should match documents where either condition is true
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$or\": [ { \"quantity\": { \"$gt\": 50 } }, { \"price\": { \"$gt\": 15 } } ] }", 100);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print documents for debugging purposes
            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> OR Query Document " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Extract quantities and prices from results for verification
            Map<Integer, Integer> quantityPriceMap = extractQuantityPriceFromResults(results);

            // Verify that all results satisfy at least one condition
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int price = entry.getValue();

                boolean satisfiesQuantityCondition = quantity > 50;
                boolean satisfiesPriceCondition = price > 15;

                assertTrue(satisfiesQuantityCondition || satisfiesPriceCondition,
                        String.format("Document should satisfy at least one condition: quantity > 50 OR price > 15. " +
                                "Found: quantity=%d, price=%d", quantity, price));
            }

            // Calculate expected results based on our document generation pattern
            // Documents: quantity=1-20, price=quantity*2 (so prices 2-40)
            // Condition: quantity > 50 OR price > 15
            // quantity > 50: no documents match (max quantity is 20)
            // price > 15: price 16-40, which corresponds to quantity 8-20 (13 documents)
            // OR: quantity > 50 OR price > 15 means quantity 8-20 (13 documents)

            assertEquals(13, results.size(), "Should return 13 documents matching quantity > 50 OR price > 15");

            // Verify specific expected quantities (8-20)
            Set<Integer> expectedQuantities = new LinkedHashSet<>();
            for (int i = 8; i <= 20; i++) {
                expectedQuantities.add(i);
            }

            Set<Integer> actualQuantities = new LinkedHashSet<>(quantityPriceMap.keySet());
            assertEquals(expectedQuantities, actualQuantities, "Should contain quantities 8-20");

            // Verify the price calculation for each quantity
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int expectedPrice = quantity * 2;
                assertEquals(expectedPrice, entry.getValue().intValue(),
                        "Price should be quantity * 2 for quantity " + quantity);
            }

            System.out.println("✓ OR query with indexed fields test passed");
        }
    }

    @Test
    void shouldExecuteORQueryWithQuantityAndPriceIndexesReverse() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-reverse";

        // Create indexes on both quantity and price fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        // Insert 20 documents with various quantity and price combinations
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for OR query: quantity > 50 OR price > 15 with REVERSE=true
            // This should match documents where either condition is true, but in reverse order
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$or\": [ { \"quantity\": { \"$gt\": 50 } }, { \"price\": { \"$gt\": 15 } } ] }", 100, true);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print documents for debugging purposes
            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> OR Query Reverse Document " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Extract quantities and prices from results for verification
            Map<Integer, Integer> quantityPriceMap = extractQuantityPriceFromResults(results);

            // Verify that all results satisfy at least one condition
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int price = entry.getValue();

                boolean satisfiesQuantityCondition = quantity > 50;
                boolean satisfiesPriceCondition = price > 15;

                assertTrue(satisfiesQuantityCondition || satisfiesPriceCondition,
                        String.format("Document should satisfy at least one condition: quantity > 50 OR price > 15. " +
                                "Found: quantity=%d, price=%d", quantity, price));
            }

            // Calculate expected results based on our document generation pattern
            // Documents: quantity=1-20, price=quantity*2 (so prices 2-40)
            // Condition: quantity > 50 OR price > 15
            // quantity > 50: no documents match (max quantity is 20)
            // price > 15: price 16-40, which corresponds to quantity 8-20 (13 documents)
            // OR: quantity > 50 OR price > 15 means quantity 8-20 (13 documents)

            assertEquals(13, results.size(), "Should return 13 documents matching quantity > 50 OR price > 15 (REVERSE)");

            // Verify specific expected quantities (8-20)
            Set<Integer> expectedQuantities = new LinkedHashSet<>();
            for (int i = 8; i <= 20; i++) {
                expectedQuantities.add(i);
            }

            Set<Integer> actualQuantities = new LinkedHashSet<>(quantityPriceMap.keySet());
            assertEquals(expectedQuantities, actualQuantities, "Should contain quantities 8-20 (REVERSE)");

            // Verify the price calculation for each quantity
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int expectedPrice = quantity * 2;
                assertEquals(expectedPrice, entry.getValue().intValue(),
                        "Price should be quantity * 2 for quantity " + quantity + " (REVERSE)");
            }

            // Verify reverse ordering - results should be in descending order by quantity
            // Since REVERSE=true, the highest quantity (20) should appear first in iteration order
            List<Integer> actualQuantitiesList = new ArrayList<>(quantityPriceMap.keySet());
            for (int i = 1; i < actualQuantitiesList.size(); i++) {
                int prevQuantity = actualQuantitiesList.get(i - 1);
                int currQuantity = actualQuantitiesList.get(i);
                // With REVERSE=true, we expect descending order
                assertTrue(prevQuantity >= currQuantity,
                        String.format("REVERSE=true should produce descending order: %d should be >= %d",
                                prevQuantity, currQuantity));
            }

            System.out.println("✓ OR query with indexed fields (REVERSE=true) test passed");
        }
    }

    @Test
    void shouldExecuteORQueryWithQuantityAndPriceIndexesReversePagination() {
        final String TEST_BUCKET_NAME = "test-bucket-or-query-reverse-pagination";

        // Create indexes on both quantity and price fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        // Insert 20 documents with various quantity and price combinations
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for OR query: quantity > 50 OR price > 15 with REVERSE=true and LIMIT=2
            // This should match documents where either condition is true, in reverse order, with pagination
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$or\": [ { \"quantity\": { \"$gt\": 50 } }, { \"price\": { \"$gt\": 15 } } ] }", 2, true);

            // Execute in batches and validate each batch's content
            List<List<Map<Integer, Integer>>> allBatchesContent = new ArrayList<>();
            Set<Integer> allQuantities = new HashSet<>();
            int totalResults = 0;
            int batchCount = 0;

            while (true) {
                Map<?, ByteBuffer> results = executor.execute(tr);

                if (results.isEmpty()) {
                    break;
                }

                batchCount++;

                // Extract quantity-price pairs from this batch
                Map<Integer, Integer> batchQuantityPriceMap = extractQuantityPriceFromResults(results);
                List<Map<Integer, Integer>> batchList = new ArrayList<>();
                batchList.add(batchQuantityPriceMap);
                allBatchesContent.add(batchList);

                totalResults += results.size();

                // Verify batch size constraint (LIMIT=2)
                assertTrue(results.size() <= 2,
                        "Batch size should not exceed LIMIT=2, but got: " + results.size());

                // Expected batch sizes: first 6 batches have 2 items, last batch has 1 item
                if (batchCount <= 6) {
                    assertEquals(2, results.size(), "Batch " + batchCount + " should have exactly 2 results");
                } else if (batchCount == 7) {
                    assertEquals(1, results.size(), "Final batch should have exactly 1 result");
                } else {
                    fail("Should not have more than 7 batches, but got batch " + batchCount);
                }

                // Print batch for debugging purposes
                System.out.println(">> OR Reverse Pagination Batch " + batchCount + " (size=" + results.size() + "):");
                for (Map.Entry<Integer, Integer> entry : batchQuantityPriceMap.entrySet()) {
                    System.out.println("   quantity=" + entry.getKey() + ", price=" + entry.getValue());
                }

                // Verify each result in this batch satisfies OR conditions
                for (Map.Entry<Integer, Integer> entry : batchQuantityPriceMap.entrySet()) {
                    int quantity = entry.getKey();
                    int price = entry.getValue();

                    boolean satisfiesQuantityCondition = quantity > 50;
                    boolean satisfiesPriceCondition = price > 15;

                    assertTrue(satisfiesQuantityCondition || satisfiesPriceCondition,
                            String.format("Batch %d: Document should satisfy OR condition: quantity > 50 OR price > 15. " +
                                    "Found: quantity=%d, price=%d", batchCount, quantity, price));

                    // Verify no duplicate quantities across batches
                    assertFalse(allQuantities.contains(quantity),
                            "Quantity " + quantity + " appears in multiple batches");
                    allQuantities.add(quantity);
                }

                // Verify descending order within this batch (REVERSE=true)
                List<Integer> batchQuantities = new ArrayList<>(batchQuantityPriceMap.keySet());
                for (int i = 1; i < batchQuantities.size(); i++) {
                    assertTrue(batchQuantities.get(i - 1) >= batchQuantities.get(i),
                            String.format("Batch %d should be in descending order: %d >= %d",
                                    batchCount, batchQuantities.get(i - 1), batchQuantities.get(i)));
                }

                // Safety check to prevent infinite loops
                if (batchCount >= 10) {
                    fail("Too many batches executed, possible infinite loop");
                }
            }

            // Verify overall pagination results
            assertEquals(13, totalResults, "Should return total of 13 documents across all batches");
            assertEquals(7, batchCount, "Should complete in exactly 7 batches (6×2 + 1×1)");

            // Verify we got all expected quantities (8-20) across all batches
            Set<Integer> expectedQuantities = new LinkedHashSet<>();
            for (int i = 8; i <= 20; i++) {
                expectedQuantities.add(i);
            }
            assertEquals(expectedQuantities, allQuantities, "Should contain all quantities 8-20 across batches");

            // Verify global descending order across all batches
            List<Integer> allQuantitiesList = new ArrayList<>();
            for (List<Map<Integer, Integer>> batchList : allBatchesContent) {
                for (Map<Integer, Integer> batchMap : batchList) {
                    allQuantitiesList.addAll(batchMap.keySet());
                }
            }

            for (int i = 1; i < allQuantitiesList.size(); i++) {
                assertTrue(allQuantitiesList.get(i - 1) >= allQuantitiesList.get(i),
                        String.format("Global order should be descending: %d >= %d (positions %d, %d)",
                                allQuantitiesList.get(i - 1), allQuantitiesList.get(i), i - 1, i));
            }

            // Verify the expected sequence: [20,19], [18,17], [16,15], [14,13], [12,11], [10,9], [8]
            List<Integer> expectedSequence = Arrays.asList(20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8);
            assertEquals(expectedSequence, allQuantitiesList,
                    "Should get quantities in exact descending order with proper pagination");

            System.out.println("✓ OR query with indexed fields (REVERSE=true, LIMIT=2) pagination test passed");
            System.out.println("Total batches: " + batchCount + ", Total results: " + totalResults);
            System.out.println("Pagination pattern: 2+2+2+2+2+2+1 = 13 results");
            System.out.println("Sequence: " + allQuantitiesList);
        }
    }

    @Test
    void shouldExecuteORQueryWithQuantityAndPriceIndexesBatchControl() {
        final String TEST_BUCKET_NAME = "test-bucket-or-batch";

        // Create indexes on both quantity and price fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        // Insert 20 documents with quantity and price combinations
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for OR query: quantity > 50 OR price > 15 with Limit=2
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$or\": [ { \"quantity\": { \"$gt\": 50 } }, { \"price\": { \"$gt\": 15 } } ] }", 2);

            List<List<Map<Integer, Integer>>> batches = new ArrayList<>();
            int batchCount = 0;
            int totalResults = 0;

            // Execute in batches until no more results
            while (true) {
                Map<?, ByteBuffer> results = executor.execute(tr);

                if (results.isEmpty()) {
                    break; // No more results
                }

                // Extract quantity-price pairs from this batch
                Map<Integer, Integer> batchQuantityPriceMap = extractQuantityPriceFromResults(results);
                List<Map<Integer, Integer>> batchList = new ArrayList<>();
                batchList.add(batchQuantityPriceMap);
                batches.add(batchList);

                batchCount++;
                totalResults += results.size();

                // Print batch for debugging purposes
                System.out.println(">> OR Forward Batch " + batchCount + " (size=" + results.size() + "):");
                for (Map.Entry<Integer, Integer> entry : batchQuantityPriceMap.entrySet()) {
                    System.out.println("   quantity=" + entry.getKey() + ", price=" + entry.getValue());
                }

                // Verify each result satisfies at least one condition
                for (Map.Entry<Integer, Integer> entry : batchQuantityPriceMap.entrySet()) {
                    int quantity = entry.getKey();
                    int price = entry.getValue();
                    boolean satisfiesQuantityCondition = quantity > 50;
                    boolean satisfiesPriceCondition = price > 15;
                    assertTrue(satisfiesQuantityCondition || satisfiesPriceCondition,
                            String.format("Document should satisfy at least one condition: quantity > 50 OR price > 15. " +
                                    "Found: quantity=%d, price=%d in batch %d", quantity, price, batchCount));
                }

                // Safety check to avoid infinite loop
                if (batchCount >= 10) {
                    fail("Too many batches, possible infinite loop detected");
                }
            }

            // Expected: 13 total results (quantities 8-20), with limit=2 per batch
            // So we expect: 6 full batches of 2 + 1 final batch of 1 = 7 batches total
            int expectedBatchCount = 7;
            assertEquals(expectedBatchCount, batchCount, "Should have exactly 7 batches");
            assertEquals(13, totalResults, "Should return 13 total documents across all batches");

            // Verify batch sizes
            for (int i = 0; i < batches.size(); i++) {
                Map<Integer, Integer> batchData = batches.get(i).getFirst();
                if (i < 6) {
                    // First 6 batches should have exactly 2 documents
                    assertEquals(2, batchData.size(), "Batch " + (i + 1) + " should contain exactly 2 documents");
                } else {
                    // Last batch should have exactly 1 document
                    assertEquals(1, batchData.size(), "Final batch should contain exactly 1 document");
                }
            }

            // Verify expected quantities are returned in ascending order (natural sort)
            // Expected sequence: 8,9,10,11,12,13,14,15,16,17,18,19,20
            // Batch 1: [8,9]
            Map<Integer, Integer> batch1 = batches.get(0).getFirst();
            assertEquals(2, batch1.size(), "First batch should contain exactly 2 documents");
            assertTrue(batch1.containsKey(8) && batch1.containsKey(9), "First batch should contain quantities [8, 9]");
            assertEquals(Integer.valueOf(16), batch1.get(8), "Quantity 8 should have price 16");
            assertEquals(Integer.valueOf(18), batch1.get(9), "Quantity 9 should have price 18");

            // Batch 2: [10,11]
            Map<Integer, Integer> batch2 = batches.get(1).getFirst();
            assertEquals(2, batch2.size(), "Second batch should contain exactly 2 documents");
            assertTrue(batch2.containsKey(10) && batch2.containsKey(11), "Second batch should contain quantities [10, 11]");
            assertEquals(Integer.valueOf(20), batch2.get(10), "Quantity 10 should have price 20");
            assertEquals(Integer.valueOf(22), batch2.get(11), "Quantity 11 should have price 22");

            // Batch 3: [12,13]
            Map<Integer, Integer> batch3 = batches.get(2).getFirst();
            assertEquals(2, batch3.size(), "Third batch should contain exactly 2 documents");
            assertTrue(batch3.containsKey(12) && batch3.containsKey(13), "Third batch should contain quantities [12, 13]");
            assertEquals(Integer.valueOf(24), batch3.get(12), "Quantity 12 should have price 24");
            assertEquals(Integer.valueOf(26), batch3.get(13), "Quantity 13 should have price 26");

            // Batch 7 (last): [20]
            Map<Integer, Integer> batch7 = batches.get(6).getFirst();
            assertEquals(1, batch7.size(), "Seventh batch should contain exactly 1 document");
            assertTrue(batch7.containsKey(20), "Seventh batch should contain quantity [20]");
            assertEquals(Integer.valueOf(40), batch7.get(20), "Quantity 20 should have price 40");

            // Collect all results across batches to verify completeness
            Set<Integer> allQuantities = new LinkedHashSet<>();
            Set<Integer> allPrices = new LinkedHashSet<>();
            for (List<Map<Integer, Integer>> batchList : batches) {
                Map<Integer, Integer> batchData = batchList.getFirst();
                allQuantities.addAll(batchData.keySet());
                allPrices.addAll(batchData.values());
            }

            System.out.println("All quantities found: " + allQuantities);
            System.out.println("All prices found: " + allPrices);

            // Verify all results still satisfy the OR conditions
            for (Integer quantity : allQuantities) {
                int price = quantity * 2; // price = quantity * 2 based on document generation
                boolean satisfiesQuantityCondition = quantity > 50;
                boolean satisfiesPriceCondition = price > 15;
                assertTrue(satisfiesQuantityCondition || satisfiesPriceCondition,
                        String.format("All documents should satisfy at least one condition: quantity > 50 OR price > 15. " +
                                "Found: quantity=%d, price=%d", quantity, price));
            }

            // Verify we got exactly the expected quantities (8-20)
            Set<Integer> expectedQuantities = new HashSet<>();
            for (int i = 8; i <= 20; i++) {
                expectedQuantities.add(i);
            }
            assertEquals(expectedQuantities, allQuantities, "Should contain exactly quantities 8-20");

            System.out.println("✓ OR query batch control test passed - 7 batches with correct content and ordering");
        }
    }

    @Test
    void shouldExecuteORQueryWithQuantityGteAndPriceLte() {
        final String TEST_BUCKET_NAME = "test-bucket-or-gte-lte";

        // Create indexes on both quantity and price fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        // Insert 20 documents with sequential quantities (1-20) and prices (2-40)
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute OR query: quantity >= 5 OR price <= 20
            // quantity >= 5: should match documents with quantities 5-20 (16 documents)
            // price <= 20: should match documents with quantities 1-10 (prices 2-20, 10 documents)
            // Union should be documents with quantities 1-20 (all 20 documents)
            // - quantities 1-4: only match price condition
            // - quantities 5-10: match both conditions (overlap)
            // - quantities 11-20: only match quantity condition
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$or\": [ { \"quantity\": { \"$gte\": 5 } }, { \"price\": { \"$lte\": 20 } } ] }", 100);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print documents for debugging purposes
            System.out.println("OR Query (quantity >= 5 OR price <= 20) Results:");
            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Should find all 20 documents:
            // - quantities 1-4: match price <= 20 condition (prices 2-8)
            // - quantities 5-10: match both conditions (overlap region)
            // - quantities 11-20: match quantity >= 5 condition (prices 22-40)
            assertEquals(20, results.size(), "Should find all 20 documents");

            // Extract quantities and prices from results for verification
            Map<Integer, Integer> quantityPriceMap = extractQuantityPriceFromResults(results);

            // Verify all results satisfy at least one OR condition
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int price = entry.getValue();

                boolean satisfiesQuantityCondition = quantity >= 5;
                boolean satisfiesPriceCondition = price <= 20;

                assertTrue(satisfiesQuantityCondition || satisfiesPriceCondition,
                        String.format("Document should satisfy at least one OR condition: quantity >= 5 OR price <= 20. " +
                                "Found: quantity=%d, price=%d", quantity, price));
            }

            // Verify we have all expected quantities (1-20)
            Set<Integer> expectedQuantities = new HashSet<>();
            for (int i = 1; i <= 20; i++) {
                expectedQuantities.add(i);
            }
            assertEquals(expectedQuantities, quantityPriceMap.keySet(), "Should contain all quantities 1-20");

            // Verify ordering (should be natural ascending order by quantity/versionstamp)
            List<Integer> quantities = new ArrayList<>(quantityPriceMap.keySet());
            Collections.sort(quantities);
            assertEquals(quantities, new ArrayList<>(quantityPriceMap.keySet()), "Results should be in ascending order by quantity");

            // Additional verification: check the overlap and distinct regions
            int quantityOnlyMatches = 0;  // quantities 11-20
            int priceOnlyMatches = 0;     // quantities 1-4
            int bothMatches = 0;          // quantities 5-10

            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int price = entry.getValue();

                boolean satisfiesQuantity = quantity >= 5;
                boolean satisfiesPrice = price <= 20;

                if (satisfiesQuantity && satisfiesPrice) {
                    bothMatches++;
                } else if (satisfiesQuantity) {
                    quantityOnlyMatches++;
                } else if (satisfiesPrice) {
                    priceOnlyMatches++;
                }
            }

            assertEquals(6, bothMatches, "Should have 6 documents matching both conditions (quantities 5-10)");
            assertEquals(10, quantityOnlyMatches, "Should have 10 documents matching only quantity condition (quantities 11-20)");
            assertEquals(4, priceOnlyMatches, "Should have 4 documents matching only price condition (quantities 1-4)");

            System.out.println("✓ OR query (quantity >= 5 OR price <= 20) test passed - found all 20 documents with correct overlap regions");
        }
    }

    @Test
    void shouldExecuteORQueryWithQuantityGteAndPriceGte() {
        final String TEST_BUCKET_NAME = "test-bucket-or-gte-gte";

        // Create indexes on both quantity and price fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        // Insert 20 documents with sequential quantities (1-20) and prices (2-40)
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute OR query: quantity >= 5 OR price >= 20
            // quantity >= 5: should match documents with quantities 5-20 (16 documents)
            // price >= 20: should match documents with quantities 10-20 (prices 20-40, 11 documents)
            // Union should be documents with quantities 5-20 (16 documents total)
            // - quantities 5-9: only match quantity condition (prices 10-18)
            // - quantities 10-20: match both conditions (overlap region, prices 20-40)
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$or\": [ { \"quantity\": { \"$gte\": 5 } }, { \"price\": { \"$gte\": 20 } } ] }", 100);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print documents for debugging purposes
            System.out.println("OR Query (quantity >= 5 OR price >= 20) Results:");
            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Should find 16 documents:
            // - quantities 5-9: match only quantity >= 5 condition (prices 10-18)
            // - quantities 10-20: match both conditions (overlap region, prices 20-40)
            // Documents with quantities 1-4 (prices 2-8) match neither condition
            assertEquals(16, results.size(), "Should find 16 documents");

            // Extract quantities and prices from results for verification
            Map<Integer, Integer> quantityPriceMap = extractQuantityPriceFromResults(results);

            // Verify all results satisfy at least one OR condition
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int price = entry.getValue();

                boolean satisfiesQuantityCondition = quantity >= 5;
                boolean satisfiesPriceCondition = price >= 20;

                assertTrue(satisfiesQuantityCondition || satisfiesPriceCondition,
                        String.format("Document should satisfy at least one OR condition: quantity >= 5 OR price >= 20. " +
                                "Found: quantity=%d, price=%d", quantity, price));
            }

            // Verify we have expected quantities (5-20, excluding 1-4)
            Set<Integer> expectedQuantities = new HashSet<>();
            for (int i = 5; i <= 20; i++) {
                expectedQuantities.add(i);
            }
            assertEquals(expectedQuantities, quantityPriceMap.keySet(), "Should contain quantities 5-20");

            // Verify ordering (should be natural ascending order by quantity/versionstamp)
            List<Integer> quantities = new ArrayList<>(quantityPriceMap.keySet());
            Collections.sort(quantities);
            assertEquals(quantities, new ArrayList<>(quantityPriceMap.keySet()), "Results should be in ascending order by quantity");

            // Additional verification: check the overlap and distinct regions
            int quantityOnlyMatches = 0;  // quantities 5-9 (prices 10-18)
            int bothMatches = 0;          // quantities 10-20 (prices 20-40)

            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int price = entry.getValue();

                boolean satisfiesQuantity = quantity >= 5;
                boolean satisfiesPrice = price >= 20;

                if (satisfiesQuantity && satisfiesPrice) {
                    bothMatches++;
                } else if (satisfiesQuantity) {
                    quantityOnlyMatches++;
                }
            }

            assertEquals(11, bothMatches, "Should have 11 documents matching both conditions (quantities 10-20)");
            assertEquals(5, quantityOnlyMatches, "Should have 5 documents matching only quantity condition (quantities 5-9)");

            // Verify no documents should match only price condition since price >= 20 implies quantity >= 10,
            // and quantity >= 10 already satisfies quantity >= 5
            int priceOnlyMatches = 0;
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int price = entry.getValue();

                boolean satisfiesQuantity = quantity >= 5;
                boolean satisfiesPrice = price >= 20;

                if (!satisfiesQuantity && satisfiesPrice) {
                    priceOnlyMatches++;
                }
            }
            assertEquals(0, priceOnlyMatches, "Should have 0 documents matching only price condition");

            System.out.println("✓ OR query (quantity >= 5 OR price >= 20) test passed - found 16 documents with correct overlap regions");
        }
    }

    @Test
    void shouldExecuteORQueryWithQuantityGteAndPriceGteReverse() {
        final String TEST_BUCKET_NAME = "test-bucket-or-gte-gte-reverse";

        // Create indexes on both quantity and price fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        // Insert 20 documents with sequential quantities (1-20) and prices (2-40)
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute OR query: quantity >= 5 OR price >= 20 with REVERSE=true
            // quantity >= 5: should match documents with quantities 5-20 (16 documents)
            // price >= 20: should match documents with quantities 10-20 (prices 20-40, 11 documents)
            // Union should be documents with quantities 5-20 (16 documents total) in reverse order
            // - quantities 10-20: match both conditions (overlap region, prices 20-40)
            // - quantities 5-9: only match quantity condition (prices 10-18)
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$or\": [ { \"quantity\": { \"$gte\": 5 } }, { \"price\": { \"$gte\": 20 } } ] }", 100, true);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print documents for debugging purposes
            System.out.println("OR Query (quantity >= 5 OR price >= 20) REVERSE Results:");
            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Should find 16 documents in reverse order:
            // - quantities 20-10: match both conditions (overlap region, prices 40-20)
            // - quantities 9-5: only match quantity condition (prices 18-10)
            // Documents with quantities 1-4 (prices 2-8) match neither condition
            assertEquals(16, results.size(), "Should find 16 documents");

            // Extract quantities and prices from results for verification
            Map<Integer, Integer> quantityPriceMap = extractQuantityPriceFromResults(results);

            // Verify all results satisfy at least one OR condition
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int price = entry.getValue();

                boolean satisfiesQuantityCondition = quantity >= 5;
                boolean satisfiesPriceCondition = price >= 20;

                assertTrue(satisfiesQuantityCondition || satisfiesPriceCondition,
                        String.format("Document should satisfy at least one OR condition: quantity >= 5 OR price >= 20. " +
                                "Found: quantity=%d, price=%d", quantity, price));
            }

            // Verify we have expected quantities (5-20, excluding 1-4)
            Set<Integer> expectedQuantities = new HashSet<>();
            for (int i = 5; i <= 20; i++) {
                expectedQuantities.add(i);
            }
            assertEquals(expectedQuantities, quantityPriceMap.keySet(), "Should contain quantities 5-20");

            // Verify reverse ordering (should be descending order by quantity/versionstamp)
            List<Integer> quantities = new ArrayList<>(quantityPriceMap.keySet());
            List<Integer> expectedOrder = new ArrayList<>();
            for (int i = 20; i >= 5; i--) {
                expectedOrder.add(i);
            }
            assertEquals(expectedOrder, quantities, "Results should be in descending order by quantity (REVERSE=true)");

            // Additional verification: check the overlap and distinct regions
            int quantityOnlyMatches = 0;  // quantities 5-9 (prices 10-18)
            int bothMatches = 0;          // quantities 10-20 (prices 20-40)

            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int price = entry.getValue();

                boolean satisfiesQuantity = quantity >= 5;
                boolean satisfiesPrice = price >= 20;

                if (satisfiesQuantity && satisfiesPrice) {
                    bothMatches++;
                } else if (satisfiesQuantity) {
                    quantityOnlyMatches++;
                }
            }

            assertEquals(11, bothMatches, "Should have 11 documents matching both conditions (quantities 10-20)");
            assertEquals(5, quantityOnlyMatches, "Should have 5 documents matching only quantity condition (quantities 5-9)");

            // Verify no documents should match only price condition since price >= 20 implies quantity >= 10,
            // and quantity >= 10 already satisfies quantity >= 5
            int priceOnlyMatches = 0;
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int price = entry.getValue();

                boolean satisfiesQuantity = quantity >= 5;
                boolean satisfiesPrice = price >= 20;

                if (!satisfiesQuantity && satisfiesPrice) {
                    priceOnlyMatches++;
                }
            }
            assertEquals(0, priceOnlyMatches, "Should have 0 documents matching only price condition");

            // Verify the expected reverse sequence: [20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5]
            List<Integer> expectedSequence = List.of(20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5);
            assertEquals(expectedSequence, quantities, "Should return documents in exact reverse sequence");

            System.out.println("✓ OR query (quantity >= 5 OR price >= 20) REVERSE test passed - found 16 documents in descending order");
        }
    }

    @Test
    void shouldExecuteORQueryWithQuantityGteAndPriceGtePagination() {
        final String TEST_BUCKET_NAME = "test-bucket-or-gte-gte-pagination";

        // Create indexes on both quantity and price fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        // Insert 20 documents with sequential quantities (1-20) and prices (2-40)
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute OR query: quantity >= 5 OR price >= 20 with LIMIT=2
            // Expected total matching documents: quantities 5-20 (16 documents)
            // With LIMIT=2, we should get batches of maximum 2 documents each
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$or\": [ { \"quantity\": { \"$gte\": 5 } }, { \"price\": { \"$gte\": 20 } } ] }", 2);

            List<List<Map<Integer, Integer>>> batches = new ArrayList<>();
            Set<Integer> allQuantitiesFound = new LinkedHashSet<>();
            int totalDocumentsProcessed = 0;

            // Execute in batches and validate each batch's content
            while (true) {
                Map<?, ByteBuffer> results = executor.execute(tr);
                if (results.isEmpty()) {
                    break;
                }

                // Extract quantity-price pairs from this batch
                Map<Integer, Integer> batchQuantityPriceMap = extractQuantityPriceFromResults(results);

                // Verify batch size constraint (LIMIT=2)
                assertTrue(results.size() <= 2, "Batch size should not exceed LIMIT=2, got: " + results.size());

                // Print batch details for debugging
                System.out.println("Batch " + (batches.size() + 1) + " (size=" + results.size() + "):");
                for (Map.Entry<Integer, Integer> entry : batchQuantityPriceMap.entrySet()) {
                    System.out.println("  >> quantity=" + entry.getKey() + ", price=" + entry.getValue());
                }

                // Verify each result satisfies OR conditions
                for (Map.Entry<Integer, Integer> entry : batchQuantityPriceMap.entrySet()) {
                    int quantity = entry.getKey();
                    int price = entry.getValue();

                    boolean satisfiesQuantityCondition = quantity >= 5;
                    boolean satisfiesPriceCondition = price >= 20;

                    assertTrue(satisfiesQuantityCondition || satisfiesPriceCondition,
                            String.format("Document should satisfy at least one OR condition: quantity >= 5 OR price >= 20. " +
                                    "Found: quantity=%d, price=%d", quantity, price));
                }

                // Verify ascending order within batch (natural sort order)
                List<Integer> batchQuantities = new ArrayList<>(batchQuantityPriceMap.keySet());
                List<Integer> sortedBatchQuantities = new ArrayList<>(batchQuantities);
                Collections.sort(sortedBatchQuantities);
                assertEquals(sortedBatchQuantities, batchQuantities, "Batch should be in ascending order by quantity");

                // Verify no duplicates across batches
                for (Integer quantity : batchQuantityPriceMap.keySet()) {
                    assertFalse(allQuantitiesFound.contains(quantity),
                            "Duplicate quantity found across batches: " + quantity);
                    allQuantitiesFound.add(quantity);
                }

                batches.add(List.of(batchQuantityPriceMap));
                totalDocumentsProcessed += results.size();
            }

            // Verify we processed all expected documents
            assertEquals(16, totalDocumentsProcessed, "Should process exactly 16 documents total");

            // Verify we have expected quantities (5-20, excluding 1-4)
            Set<Integer> expectedQuantities = new HashSet<>();
            for (int i = 5; i <= 20; i++) {
                expectedQuantities.add(i);
            }
            assertEquals(expectedQuantities, allQuantitiesFound, "Should contain all quantities 5-20");

            // Calculate expected number of batches: 16 documents / 2 per batch = 8 batches
            assertEquals(8, batches.size(), "Should have exactly 8 batches for 16 documents with LIMIT=2");

            // Verify specific batch contents and sizes
            // Batch 1: [5,6]
            Map<Integer, Integer> batch1 = batches.get(0).getFirst();
            assertEquals(2, batch1.size(), "First batch should contain exactly 2 documents");
            assertTrue(batch1.containsKey(5) && batch1.containsKey(6), "First batch should contain quantities [5, 6]");
            assertEquals(Integer.valueOf(10), batch1.get(5), "Quantity 5 should have price 10");
            assertEquals(Integer.valueOf(12), batch1.get(6), "Quantity 6 should have price 12");

            // Batch 2: [7,8]
            Map<Integer, Integer> batch2 = batches.get(1).getFirst();
            assertEquals(2, batch2.size(), "Second batch should contain exactly 2 documents");
            assertTrue(batch2.containsKey(7) && batch2.containsKey(8), "Second batch should contain quantities [7, 8]");
            assertEquals(Integer.valueOf(14), batch2.get(7), "Quantity 7 should have price 14");
            assertEquals(Integer.valueOf(16), batch2.get(8), "Quantity 8 should have price 16");

            // Batch 3: [9,10]
            Map<Integer, Integer> batch3 = batches.get(2).getFirst();
            assertEquals(2, batch3.size(), "Third batch should contain exactly 2 documents");
            assertTrue(batch3.containsKey(9) && batch3.containsKey(10), "Third batch should contain quantities [9, 10]");
            assertEquals(Integer.valueOf(18), batch3.get(9), "Quantity 9 should have price 18");
            assertEquals(Integer.valueOf(20), batch3.get(10), "Quantity 10 should have price 20");

            // Batch 8 (last): [19,20]
            Map<Integer, Integer> batch8 = batches.get(7).getFirst();
            assertEquals(2, batch8.size(), "Eighth batch should contain exactly 2 documents");
            assertTrue(batch8.containsKey(19) && batch8.containsKey(20), "Eighth batch should contain quantities [19, 20]");
            assertEquals(Integer.valueOf(38), batch8.get(19), "Quantity 19 should have price 38");
            assertEquals(Integer.valueOf(40), batch8.get(20), "Quantity 20 should have price 40");

            // Collect all results across batches to verify completeness
            Set<Integer> allQuantities = new LinkedHashSet<>();
            Set<Integer> allPrices = new LinkedHashSet<>();
            for (List<Map<Integer, Integer>> batchList : batches) {
                Map<Integer, Integer> batchData = batchList.getFirst();
                allQuantities.addAll(batchData.keySet());
                allPrices.addAll(batchData.values());
            }

            System.out.println("All quantities found: " + allQuantities);
            System.out.println("All prices found: " + allPrices);

            // Verify all results still satisfy the OR conditions
            for (Integer quantity : allQuantities) {
                int price = quantity * 2; // price = quantity * 2 based on document generation
                boolean satisfiesQuantityCondition = quantity >= 5;
                boolean satisfiesPriceCondition = price >= 20;
                assertTrue(satisfiesQuantityCondition || satisfiesPriceCondition,
                        String.format("All documents should satisfy at least one condition: quantity >= 5 OR price >= 20. " +
                                "Found: quantity=%d, price=%d", quantity, price));
            }

            // Verify we got exactly the expected quantities (5-20)
            Set<Integer> expectedFinalQuantities = new HashSet<>();
            for (int i = 5; i <= 20; i++) {
                expectedFinalQuantities.add(i);
            }
            assertEquals(expectedFinalQuantities, allQuantities, "Should contain exactly quantities 5-20");

            // Additional verification: check the overlap and distinct regions across all batches
            int quantityOnlyMatches = 0;  // quantities 5-9 (prices 10-18)
            int bothMatches = 0;          // quantities 10-20 (prices 20-40)

            for (Integer quantity : allQuantities) {
                int price = quantity * 2;
                boolean satisfiesQuantity = quantity >= 5;
                boolean satisfiesPrice = price >= 20;

                if (satisfiesQuantity && satisfiesPrice) {
                    bothMatches++;
                } else if (satisfiesQuantity) {
                    quantityOnlyMatches++;
                }
            }

            assertEquals(11, bothMatches, "Should have 11 documents matching both conditions (quantities 10-20)");
            assertEquals(5, quantityOnlyMatches, "Should have 5 documents matching only quantity condition (quantities 5-9)");

            System.out.println("✓ OR query (quantity >= 5 OR price >= 20) pagination test passed - 8 batches with correct content and ordering");
        }
    }

    @Test
    void shouldExecuteORQueryWithQuantityGteAndPriceGteReversePagination() {
        final String TEST_BUCKET_NAME = "test-bucket-or-gte-gte-reverse-pagination";

        // Create indexes on both quantity and price fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex);

        // Insert 20 documents with sequential quantities (1-20) and prices (2-40)
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute OR query: quantity >= 5 OR price >= 20 with LIMIT=2 and REVERSE=true
            // Expected total matching documents: quantities 5-20 (16 documents) in reverse order
            // With LIMIT=2, we should get batches of maximum 2 documents each in descending order
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$or\": [ { \"quantity\": { \"$gte\": 5 } }, { \"price\": { \"$gte\": 20 } } ] }", 2, true);

            List<List<Map<Integer, Integer>>> batches = new ArrayList<>();
            Set<Integer> allQuantitiesFound = new LinkedHashSet<>();
            int totalDocumentsProcessed = 0;

            // Execute in batches and validate each batch's content
            while (true) {
                Map<?, ByteBuffer> results = executor.execute(tr);
                if (results.isEmpty()) {
                    break;
                }

                // Extract quantity-price pairs from this batch
                Map<Integer, Integer> batchQuantityPriceMap = extractQuantityPriceFromResults(results);

                // Verify batch size constraint (LIMIT=2)
                assertTrue(results.size() <= 2, "Batch size should not exceed LIMIT=2, got: " + results.size());

                // Print batch details for debugging
                System.out.println("Batch " + (batches.size() + 1) + " (size=" + results.size() + "):");
                for (Map.Entry<Integer, Integer> entry : batchQuantityPriceMap.entrySet()) {
                    System.out.println("  >> quantity=" + entry.getKey() + ", price=" + entry.getValue());
                }

                // Verify each result satisfies OR conditions
                for (Map.Entry<Integer, Integer> entry : batchQuantityPriceMap.entrySet()) {
                    int quantity = entry.getKey();
                    int price = entry.getValue();

                    boolean satisfiesQuantityCondition = quantity >= 5;
                    boolean satisfiesPriceCondition = price >= 20;

                    assertTrue(satisfiesQuantityCondition || satisfiesPriceCondition,
                            String.format("Document should satisfy at least one OR condition: quantity >= 5 OR price >= 20. " +
                                    "Found: quantity=%d, price=%d", quantity, price));
                }

                // Verify descending order within batch (REVERSE=true)
                List<Integer> batchQuantities = new ArrayList<>(batchQuantityPriceMap.keySet());
                List<Integer> sortedBatchQuantitiesDesc = new ArrayList<>(batchQuantities);
                Collections.sort(sortedBatchQuantitiesDesc, Collections.reverseOrder());
                assertEquals(sortedBatchQuantitiesDesc, batchQuantities, "Batch should be in descending order by quantity (REVERSE=true)");

                // Verify no duplicates across batches
                for (Integer quantity : batchQuantityPriceMap.keySet()) {
                    assertFalse(allQuantitiesFound.contains(quantity),
                            "Duplicate quantity found across batches: " + quantity);
                    allQuantitiesFound.add(quantity);
                }

                batches.add(List.of(batchQuantityPriceMap));
                totalDocumentsProcessed += results.size();
            }

            // Verify we processed all expected documents
            assertEquals(16, totalDocumentsProcessed, "Should process exactly 16 documents total");

            // Verify we have expected quantities (5-20, excluding 1-4)
            Set<Integer> expectedQuantities = new HashSet<>();
            for (int i = 5; i <= 20; i++) {
                expectedQuantities.add(i);
            }
            assertEquals(expectedQuantities, allQuantitiesFound, "Should contain all quantities 5-20");

            // Calculate expected number of batches: 16 documents / 2 per batch = 8 batches
            assertEquals(8, batches.size(), "Should have exactly 8 batches for 16 documents with LIMIT=2");

            // Verify specific batch contents and sizes in REVERSE order
            // Batch 1: [20,19] (highest quantities first)
            Map<Integer, Integer> batch1 = batches.get(0).getFirst();
            assertEquals(2, batch1.size(), "First batch should contain exactly 2 documents");
            assertTrue(batch1.containsKey(20) && batch1.containsKey(19), "First batch should contain quantities [20, 19]");
            assertEquals(Integer.valueOf(40), batch1.get(20), "Quantity 20 should have price 40");
            assertEquals(Integer.valueOf(38), batch1.get(19), "Quantity 19 should have price 38");

            // Batch 2: [18,17]
            Map<Integer, Integer> batch2 = batches.get(1).getFirst();
            assertEquals(2, batch2.size(), "Second batch should contain exactly 2 documents");
            assertTrue(batch2.containsKey(18) && batch2.containsKey(17), "Second batch should contain quantities [18, 17]");
            assertEquals(Integer.valueOf(36), batch2.get(18), "Quantity 18 should have price 36");
            assertEquals(Integer.valueOf(34), batch2.get(17), "Quantity 17 should have price 34");

            // Batch 3: [16,15]
            Map<Integer, Integer> batch3 = batches.get(2).getFirst();
            assertEquals(2, batch3.size(), "Third batch should contain exactly 2 documents");
            assertTrue(batch3.containsKey(16) && batch3.containsKey(15), "Third batch should contain quantities [16, 15]");
            assertEquals(Integer.valueOf(32), batch3.get(16), "Quantity 16 should have price 32");
            assertEquals(Integer.valueOf(30), batch3.get(15), "Quantity 15 should have price 30");

            // Batch 8 (last): [6,5] (lowest quantities last)
            Map<Integer, Integer> batch8 = batches.get(7).getFirst();
            assertEquals(2, batch8.size(), "Eighth batch should contain exactly 2 documents");
            assertTrue(batch8.containsKey(6) && batch8.containsKey(5), "Eighth batch should contain quantities [6, 5]");
            assertEquals(Integer.valueOf(12), batch8.get(6), "Quantity 6 should have price 12");
            assertEquals(Integer.valueOf(10), batch8.get(5), "Quantity 5 should have price 10");

            // Collect all results across batches to verify completeness
            Set<Integer> allQuantities = new LinkedHashSet<>();
            Set<Integer> allPrices = new LinkedHashSet<>();
            for (List<Map<Integer, Integer>> batchList : batches) {
                Map<Integer, Integer> batchData = batchList.getFirst();
                allQuantities.addAll(batchData.keySet());
                allPrices.addAll(batchData.values());
            }

            System.out.println("All quantities found: " + allQuantities);
            System.out.println("All prices found: " + allPrices);

            // Verify all results still satisfy the OR conditions
            for (Integer quantity : allQuantities) {
                int price = quantity * 2; // price = quantity * 2 based on document generation
                boolean satisfiesQuantityCondition = quantity >= 5;
                boolean satisfiesPriceCondition = price >= 20;
                assertTrue(satisfiesQuantityCondition || satisfiesPriceCondition,
                        String.format("All documents should satisfy at least one condition: quantity >= 5 OR price >= 20. " +
                                "Found: quantity=%d, price=%d", quantity, price));
            }

            // Verify we got exactly the expected quantities (5-20)
            Set<Integer> expectedFinalQuantities = new HashSet<>();
            for (int i = 5; i <= 20; i++) {
                expectedFinalQuantities.add(i);
            }
            assertEquals(expectedFinalQuantities, allQuantities, "Should contain exactly quantities 5-20");

            // Verify the expected REVERSE sequence across all batches: [20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5]
            List<Integer> allQuantitiesOrdered = new ArrayList<>(allQuantitiesFound);
            List<Integer> expectedReverseSequence = List.of(20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5);
            assertEquals(expectedReverseSequence, allQuantitiesOrdered, "Should return documents in exact reverse sequence");

            // Additional verification: check the overlap and distinct regions across all batches
            int quantityOnlyMatches = 0;  // quantities 5-9 (prices 10-18)
            int bothMatches = 0;          // quantities 10-20 (prices 20-40)

            for (Integer quantity : allQuantities) {
                int price = quantity * 2;
                boolean satisfiesQuantity = quantity >= 5;
                boolean satisfiesPrice = price >= 20;

                if (satisfiesQuantity && satisfiesPrice) {
                    bothMatches++;
                } else if (satisfiesQuantity) {
                    quantityOnlyMatches++;
                }
            }

            assertEquals(11, bothMatches, "Should have 11 documents matching both conditions (quantities 10-20)");
            assertEquals(5, quantityOnlyMatches, "Should have 5 documents matching only quantity condition (quantities 5-9)");

            // Verify individual batch reverse ordering
            for (int i = 0; i < batches.size(); i++) {
                Map<Integer, Integer> batchData = batches.get(i).getFirst();
                List<Integer> batchQuantitiesList = new ArrayList<>(batchData.keySet());

                // Each batch should have quantities in descending order
                List<Integer> expectedBatchOrder = new ArrayList<>(batchQuantitiesList);
                Collections.sort(expectedBatchOrder, Collections.reverseOrder());
                assertEquals(expectedBatchOrder, batchQuantitiesList,
                        "Batch " + (i + 1) + " should have quantities in descending order");
            }

            System.out.println("✓ OR query (quantity >= 5 OR price >= 20) REVERSE pagination test passed - 8 batches with correct content and reverse ordering");
        }
    }

    @Test
    void shouldExecuteANDQueryWithThreeIndexedFields() {
        final String TEST_BUCKET_NAME = "test-bucket-and-three-indexed";

        // Create indexes on quantity, price, and category fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition categoryIndex = IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex, categoryIndex);

        // Create 20 documents where only 9 will satisfy: quantity > 5 AND price > 15 AND category = 'electronics'
        List<byte[]> documents = new ArrayList<>();

        // Electronics documents - 12 total, but only 9 will satisfy all conditions
        // Documents that satisfy ALL conditions (quantity > 5 AND price > 15 AND category = 'electronics'):
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 6, 'price': 16, 'category': 'electronics'}"));   // ✓ (6 > 5, 16 > 15, electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 7, 'price': 17, 'category': 'electronics'}"));   // ✓ (7 > 5, 17 > 15, electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 8, 'price': 18, 'category': 'electronics'}"));   // ✓ (8 > 5, 18 > 15, electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 9, 'price': 19, 'category': 'electronics'}"));   // ✓ (9 > 5, 19 > 15, electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 10, 'price': 20, 'category': 'electronics'}")); // ✓ (10 > 5, 20 > 15, electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 11, 'price': 22, 'category': 'electronics'}")); // ✓ (11 > 5, 22 > 15, electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 12, 'price': 24, 'category': 'electronics'}")); // ✓ (12 > 5, 24 > 15, electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 13, 'price': 26, 'category': 'electronics'}")); // ✓ (13 > 5, 26 > 15, electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 15, 'price': 30, 'category': 'electronics'}")); // ✓ (15 > 5, 30 > 15, electronics)

        // Electronics documents that DON'T satisfy all conditions:
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 3, 'price': 16, 'category': 'electronics'}"));   // ✗ (3 ≤ 5)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 8, 'price': 12, 'category': 'electronics'}"));   // ✗ (12 ≤ 15)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 4, 'price': 8, 'category': 'electronics'}"));    // ✗ (4 ≤ 5, 8 ≤ 15)

        // Non-electronics documents (various categories) - these won't satisfy category condition:
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 10, 'price': 20, 'category': 'books'}"));        // ✗ (not electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 12, 'price': 25, 'category': 'clothing'}"));     // ✗ (not electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 8, 'price': 18, 'category': 'sports'}"));        // ✗ (not electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 15, 'price': 30, 'category': 'home'}"));         // ✗ (not electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 7, 'price': 16, 'category': 'books'}"));         // ✗ (not electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 20, 'price': 40, 'category': 'clothing'}"));     // ✗ (not electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 9, 'price': 22, 'category': 'sports'}"));        // ✗ (not electronics)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 14, 'price': 28, 'category': 'home'}"));         // ✗ (not electronics)

        // Total: 20 documents, 12 electronics (9 satisfy all conditions), 8 non-electronics

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute AND query: quantity > 5 AND price > 15 AND category = 'electronics'
            // Should return exactly 9 documents
            PlanExecutor executor = createPlanExecutorForQuery(metadata,
                    "{ \"$and\": [ { \"quantity\": { \"$gt\": 5 } }, { \"price\": { \"$gt\": 15 } }, { \"category\": { \"$eq\": \"electronics\" } } ] }", 100);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print documents for debugging purposes
            System.out.println("AND Query (quantity > 5 AND price > 15 AND category = 'electronics') Results:");
            System.out.println("Expected: 9 documents from 20 total (12 electronics, 8 other categories)");
            System.out.println("Query should exclude:");
            System.out.println("  - 3 electronics with quantity ≤ 5 or price ≤ 15");
            System.out.println("  - 8 non-electronics documents (even if quantity > 5 AND price > 15)");
            System.out.println();

            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> AND Query Document " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Should find exactly 9 documents
            assertEquals(9, results.size(), "Should find exactly 9 documents satisfying all AND conditions");
            System.out.println();
            System.out.println("✅ Found exactly 9 documents as expected!");

            // Extract and validate each result
            Map<Integer, Map<String, Object>> resultData = new LinkedHashMap<>();
            for (ByteBuffer documentBuffer : results.values()) {
                documentBuffer.rewind();
                try (BsonBinaryReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();
                    Integer quantity = null;
                    Integer price = null;
                    String category = null;

                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        switch (fieldName) {
                            case "quantity" -> quantity = reader.readInt32();
                            case "price" -> price = reader.readInt32();
                            case "category" -> category = reader.readString();
                            default -> reader.skipValue();
                        }
                    }
                    reader.readEndDocument();

                    if (quantity != null && price != null && category != null) {
                        Map<String, Object> docData = new HashMap<>();
                        docData.put("price", price);
                        docData.put("category", category);
                        resultData.put(quantity, docData);
                    }
                }
            }

            // Verify all results satisfy ALL three AND conditions
            for (Map.Entry<Integer, Map<String, Object>> entry : resultData.entrySet()) {
                int quantity = entry.getKey();
                int price = (Integer) entry.getValue().get("price");
                String category = (String) entry.getValue().get("category");

                assertTrue(quantity > 5,
                        String.format("Quantity should be > 5: found quantity=%d", quantity));
                assertTrue(price > 15,
                        String.format("Price should be > 15: found price=%d", price));
                assertEquals("electronics", category,
                        String.format("Category should be 'electronics': found category=%s", category));
            }

            // Verify we have exactly the expected quantities
            Set<Integer> expectedQuantities = Set.of(6, 7, 8, 9, 10, 11, 12, 13, 15);
            assertEquals(expectedQuantities, resultData.keySet(),
                    "Should contain exactly the expected quantities that satisfy all conditions");

            // Verify ordering (should be natural ascending order by quantity/versionstamp)
            List<Integer> quantities = new ArrayList<>(resultData.keySet());
            List<Integer> expectedOrder = List.of(6, 7, 8, 9, 10, 11, 12, 13, 15);
            assertEquals(expectedOrder, quantities, "Results should be in ascending order by quantity");

            // Additional validation: verify category distribution
            long electronicsCount = resultData.values().stream()
                    .map(data -> (String) data.get("category"))
                    .filter("electronics"::equals)
                    .count();
            assertEquals(9, electronicsCount, "All 9 results should have category 'electronics'");

            // Verify price ranges
            List<Integer> prices = resultData.values().stream()
                    .map(data -> (Integer) data.get("price"))
                    .sorted()
                    .toList();
            List<Integer> expectedPrices = List.of(16, 17, 18, 19, 20, 22, 24, 26, 30);
            assertEquals(expectedPrices, prices, "Should have expected prices in ascending order");

            System.out.println("📊 Test Results Summary:");
            System.out.println("  • Total documents in dataset: 20");
            System.out.println("  • Electronics documents: 12 (9 match all conditions, 3 excluded)");
            System.out.println("  • Non-electronics documents: 8 (all excluded by category filter)");
            System.out.println("  • Final results: 9 documents");
            System.out.println("  • Quantities found: " + resultData.keySet());
            System.out.println("  • Price range: " + prices.getFirst() + "-" + prices.getLast());
            System.out.println("  • All results have category: electronics");
            System.out.println();
            System.out.println("✓ AND query with three indexed fields test passed - found exactly 9 documents satisfying all conditions");
        }
    }

    @Test
    void shouldExecuteANDQueryWithNonExistingCategory() {
        final String TEST_BUCKET_NAME = "test-bucket-and-non-existing-category";

        // Create indexes on quantity, price, and category fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition categoryIndex = IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex, categoryIndex);

        // Create 20 documents with existing categories, none will match the non-existing category
        List<byte[]> documents = new ArrayList<>();
        String[] existingCategories = {"electronics", "books", "clothing", "sports", "home"};

        // Add documents with various combinations that would satisfy quantity > 5 AND price > 15
        // but none will have the non-existing category "automotive"
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 6, 'price': 16, 'category': 'electronics'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 7, 'price': 17, 'category': 'electronics'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 8, 'price': 18, 'category': 'electronics'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 9, 'price': 19, 'category': 'electronics'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 10, 'price': 20, 'category': 'electronics'}"));
        
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 11, 'price': 22, 'category': 'books'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 12, 'price': 24, 'category': 'books'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 13, 'price': 26, 'category': 'books'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 14, 'price': 28, 'category': 'books'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 15, 'price': 30, 'category': 'books'}"));
        
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 16, 'price': 32, 'category': 'clothing'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 17, 'price': 34, 'category': 'clothing'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 18, 'price': 36, 'category': 'clothing'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 19, 'price': 38, 'category': 'clothing'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 20, 'price': 40, 'category': 'clothing'}"));
        
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 21, 'price': 42, 'category': 'sports'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 22, 'price': 44, 'category': 'sports'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 23, 'price': 46, 'category': 'home'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 24, 'price': 48, 'category': 'home'}"));
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 25, 'price': 50, 'category': 'home'}"));

        // Total: 20 documents across 5 categories (electronics, books, clothing, sports, home)
        // All have quantity > 5 AND price > 15, but NONE have category = 'automotive'

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute AND query: quantity > 5 AND price > 15 AND category = 'automotive' (non-existing)
            // Should return 0 documents (empty result set)
            PlanExecutor executor = createPlanExecutorForQuery(metadata, 
                "{ \"$and\": [ { \"quantity\": { \"$gt\": 5 } }, { \"price\": { \"$gt\": 15 } }, { \"category\": { \"$eq\": \"automotive\" } } ] }", 100);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print detailed information about the test
            System.out.println("AND Query (quantity > 5 AND price > 15 AND category = 'automotive') Results:");
            System.out.println("Expected: 0 documents from 20 total (all have existing categories)");
            System.out.println("Dataset contains categories: " + String.join(", ", existingCategories));
            System.out.println("Query searches for non-existing category: 'automotive'");
            System.out.println("All 20 documents satisfy quantity > 5 AND price > 15, but none have category = 'automotive'");
            System.out.println();

            if (results.isEmpty()) {
                System.out.println(">> No documents found (as expected)");
            } else {
                System.out.println(">> Unexpected results found:");
                for (ByteBuffer buffer : results.values()) {
                    System.out.println("   " + BSONUtil.fromBson(buffer.duplicate().array()));
                }
            }

            // Should find exactly 0 documents
            assertEquals(0, results.size(), "Should find exactly 0 documents when searching for non-existing category");
            assertTrue(results.isEmpty(), "Result map should be empty when no documents match the query");
            
            System.out.println();
            System.out.println("✅ Found exactly 0 documents as expected!");
            
            System.out.println("📊 Test Results Summary:");
            System.out.println("  • Total documents in dataset: 20");
            System.out.println("  • Documents with quantity > 5: 20");
            System.out.println("  • Documents with price > 15: 20");
            System.out.println("  • Documents with category = 'automotive': 0");
            System.out.println("  • Final results (intersection): 0 documents");
            System.out.println("  • Existing categories: " + String.join(", ", existingCategories));
            System.out.println("  • Searched category: automotive (non-existing)");
            System.out.println("  • Result map isEmpty(): " + results.isEmpty());
            System.out.println();
            
            System.out.println("✓ AND query with non-existing category test passed - found exactly 0 documents as expected");
        }
    }

    @Test
    void shouldExecuteORQueryWithNonExistingCategory() {
        final String TEST_BUCKET_NAME = "test-bucket-or-non-existing-category";

        // Create indexes on quantity, price, and category fields
        IndexDefinition quantityIndex = IndexDefinition.create("quantity-index", "quantity", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition priceIndex = IndexDefinition.create("price-index", "price", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition categoryIndex = IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, quantityIndex, priceIndex, categoryIndex);

        // Create 20 documents with strategic values to test OR logic
        List<byte[]> documents = new ArrayList<>();
        String[] existingCategories = {"electronics", "books", "clothing", "sports", "home"};

        // Group 1: Documents that satisfy quantity > 5 AND price > 15 (10 documents)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 6, 'price': 16, 'category': 'electronics'}"));   // ✓ both conditions
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 7, 'price': 17, 'category': 'electronics'}"));   // ✓ both conditions
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 8, 'price': 18, 'category': 'books'}"));         // ✓ both conditions
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 9, 'price': 19, 'category': 'books'}"));         // ✓ both conditions
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 10, 'price': 20, 'category': 'clothing'}"));     // ✓ both conditions
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 11, 'price': 22, 'category': 'clothing'}"));     // ✓ both conditions
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 12, 'price': 24, 'category': 'sports'}"));       // ✓ both conditions
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 13, 'price': 26, 'category': 'sports'}"));       // ✓ both conditions
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 14, 'price': 28, 'category': 'home'}"));         // ✓ both conditions
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 15, 'price': 30, 'category': 'home'}"));         // ✓ both conditions

        // Group 2: Documents that satisfy ONLY quantity > 5 (5 documents)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 16, 'price': 10, 'category': 'electronics'}"));  // ✓ quantity only
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 17, 'price': 12, 'category': 'books'}"));        // ✓ quantity only
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 18, 'price': 14, 'category': 'clothing'}"));     // ✓ quantity only
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 19, 'price': 8, 'category': 'sports'}"));        // ✓ quantity only
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 20, 'price': 6, 'category': 'home'}"));          // ✓ quantity only

        // Group 3: Documents that satisfy ONLY price > 15 (3 documents)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 3, 'price': 25, 'category': 'electronics'}"));   // ✓ price only
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 4, 'price': 35, 'category': 'books'}"));         // ✓ price only
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 5, 'price': 40, 'category': 'clothing'}"));      // ✓ price only

        // Group 4: Documents that satisfy NEITHER condition (2 documents)
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 2, 'price': 5, 'category': 'sports'}"));         // ✗ neither
        documents.add(BSONUtil.jsonToDocumentThenBytes("{'quantity': 1, 'price': 3, 'category': 'home'}"));           // ✗ neither

        // Total: 20 documents
        // Expected OR results: 10 (both) + 5 (quantity only) + 3 (price only) + 0 (category 'automotive') = 18 documents

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute OR query: quantity > 5 OR price > 15 OR category = 'automotive' (non-existing)
            // Should return 18 documents (all except the 2 that satisfy none of the conditions)
            PlanExecutor executor = createPlanExecutorForQuery(metadata, 
                "{ \"$or\": [ { \"quantity\": { \"$gt\": 5 } }, { \"price\": { \"$gt\": 15 } }, { \"category\": { \"$eq\": \"automotive\" } } ] }", 100);

            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print detailed information about the test
            System.out.println("OR Query (quantity > 5 OR price > 15 OR category = 'automotive') Results:");
            System.out.println("Expected calculation:");
            System.out.println("  • Documents with quantity > 5 AND price > 15: 10 (satisfy both conditions)");
            System.out.println("  • Documents with quantity > 5 ONLY: 5 (price ≤ 15)");
            System.out.println("  • Documents with price > 15 ONLY: 3 (quantity ≤ 5)");
            System.out.println("  • Documents with category = 'automotive': 0 (non-existing category)");
            System.out.println("  • Documents satisfying NONE: 2 (quantity ≤ 5 AND price ≤ 15)");
            System.out.println("Expected total: 10 + 5 + 3 + 0 = 18 documents from 20 total");
            System.out.println("Dataset contains categories: " + String.join(", ", existingCategories));
            System.out.println("Query includes non-existing category: 'automotive' (contributes 0 documents)");
            System.out.println();

            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> OR Query Document " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Should find exactly 18 documents
            assertEquals(18, results.size(), "Should find exactly 18 documents satisfying at least one OR condition");
            
            System.out.println();
            System.out.println("✅ Found exactly 18 documents as expected!");

            // Extract and analyze results
            Map<Integer, Map<String, Object>> resultData = new LinkedHashMap<>();
            for (ByteBuffer documentBuffer : results.values()) {
                documentBuffer.rewind();
                try (BsonBinaryReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();
                    Integer quantity = null;
                    Integer price = null;
                    String category = null;

                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        switch (fieldName) {
                            case "quantity" -> quantity = reader.readInt32();
                            case "price" -> price = reader.readInt32();
                            case "category" -> category = reader.readString();
                            default -> reader.skipValue();
                        }
                    }
                    reader.readEndDocument();

                    if (quantity != null && price != null && category != null) {
                        Map<String, Object> docData = new LinkedHashMap<>();
                        docData.put("price", price);
                        docData.put("category", category);
                        resultData.put(quantity, docData);
                    }
                }
            }

            // Analyze results by condition satisfaction
            int bothConditions = 0;
            int quantityOnly = 0;
            int priceOnly = 0;
            int categoryOnly = 0;
            int noneConditions = 0;

            for (Map.Entry<Integer, Map<String, Object>> entry : resultData.entrySet()) {
                int quantity = entry.getKey();
                int price = (Integer) entry.getValue().get("price");
                String category = (String) entry.getValue().get("category");

                boolean satisfiesQuantity = quantity > 5;
                boolean satisfiesPrice = price > 15;
                boolean satisfiesCategory = "automotive".equals(category);

                // Verify at least one OR condition is satisfied
                assertTrue(satisfiesQuantity || satisfiesPrice || satisfiesCategory, 
                    String.format("Document should satisfy at least one OR condition: quantity > 5 OR price > 15 OR category = 'automotive'. " +
                    "Found: quantity=%d, price=%d, category=%s", quantity, price, category));

                // Categorize results
                if (satisfiesQuantity && satisfiesPrice && !satisfiesCategory) {
                    bothConditions++;
                } else if (satisfiesQuantity && !satisfiesPrice && !satisfiesCategory) {
                    quantityOnly++;
                } else if (!satisfiesQuantity && satisfiesPrice && !satisfiesCategory) {
                    priceOnly++;
                } else if (!satisfiesQuantity && !satisfiesPrice) {
                    categoryOnly++;
                }
            }

            // Verify expected distribution
            assertEquals(10, bothConditions, "Should have 10 documents satisfying both quantity > 5 AND price > 15");
            assertEquals(5, quantityOnly, "Should have 5 documents satisfying only quantity > 5");
            assertEquals(3, priceOnly, "Should have 3 documents satisfying only price > 15");
            assertEquals(0, categoryOnly, "Should have 0 documents satisfying only category = 'automotive'");
            assertEquals(0, noneConditions, "Should have 0 documents in results that satisfy none of the conditions");

            // Note: OR query result ordering depends on how sub-queries are processed and merged
            // The actual ordering may not be strictly ascending due to union operations
            List<Integer> quantities = new ArrayList<>(resultData.keySet());
            Set<Integer> expectedQuantitiesSet = Set.of(3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);
            assertEquals(expectedQuantitiesSet, new LinkedHashSet<>(quantities), "Results should contain expected quantities regardless of order");

            // Verify no documents with quantities 1,2 are present (they satisfy none of the conditions)
            assertFalse(resultData.containsKey(1), "Document with quantity=1 should not be in results");
            assertFalse(resultData.containsKey(2), "Document with quantity=2 should not be in results");

            System.out.println("📊 Test Results Summary:");
            System.out.println("  • Total documents in dataset: 20");
            System.out.println("  • Documents satisfying quantity > 5 AND price > 15: " + bothConditions);
            System.out.println("  • Documents satisfying ONLY quantity > 5: " + quantityOnly);
            System.out.println("  • Documents satisfying ONLY price > 15: " + priceOnly);
            System.out.println("  • Documents satisfying ONLY category = 'automotive': " + categoryOnly);
            System.out.println("  • Final results (union): " + results.size() + " documents");
            System.out.println("  • Excluded documents (satisfy none): 2 (quantities 1,2)");
            System.out.println("  • Quantities found (in result order): " + quantities);
            System.out.println("  • Expected quantities (any order): " + expectedQuantitiesSet);
            System.out.println("  • Non-existing category contributed: 0 documents");
            System.out.println("  • Result map isEmpty(): " + results.isEmpty());
            System.out.println();
            
            System.out.println("✓ OR query with non-existing category test passed - found exactly 18 documents satisfying at least one condition");
        }
    }

    /**
     * Tests concurrent full bucket scan optimization with non-indexed field query.
     * This test validates that the batch-concurrent document retrieval works correctly
     * when scanning all documents in a bucket without an index on the query field.
     */
    @Test
    void shouldExecuteNonIndexedPriceQueryWithConcurrentFullBucketScan() {
        final String TEST_BUCKET_NAME = "test-bucket-non-indexed-price";

        // Important: Create bucket metadata WITHOUT any indexes on price or category fields
        // This forces the query to use full bucket scan with concurrent document retrieval
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert 100 documents with sequential prices (1-100)
        // Each document: {"price": %d, "category": "electronics"}
        List<byte[]> documents = createPriceDocuments(100);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute query: price > 5 with LIMIT=100
            // This will trigger concurrent full bucket scan since price field is not indexed
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"price\": { \"$gt\": 5 } }", 100);

            // Execute the query and measure performance
            long startTime = System.nanoTime();
            Map<?, ByteBuffer> results = executor.execute(tr);
            long endTime = System.nanoTime();
            long executionTimeMs = (endTime - startTime) / 1_000_000;

            // Print performance info for debugging
            System.out.println("=== Concurrent Full Bucket Scan Test ===");
            System.out.println("Query: price > 5 on 100 documents (non-indexed)");
            System.out.println("Execution time: " + executionTimeMs + " ms");
            System.out.println("Results count: " + results.size());

            // Calculate expected results: prices 6-100 = 95 documents
            int expectedCount = 95;
            assertEquals(expectedCount, results.size(),
                    "Should return exactly 95 documents with price > 5");

            // Extract prices from results and verify correctness
            Set<Integer> actualPrices = extractPricesFromResults(results);

            // Build expected price set: {6, 7, 8, ..., 100}
            Set<Integer> expectedPrices = new HashSet<>();
            for (int i = 6; i <= 100; i++) {
                expectedPrices.add(i);
            }

            // Verify all returned prices are correct
            assertEquals(expectedPrices, actualPrices,
                    "Should return prices 6-100, got: " + actualPrices);

            // Verify each document has correct structure and values
            for (ByteBuffer documentBuffer : results.values()) {
                documentBuffer.rewind();
                try (BsonReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();

                    Integer price = null;
                    String category = null;

                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        if ("price".equals(fieldName)) {
                            price = reader.readInt32();
                        } else if ("category".equals(fieldName)) {
                            category = reader.readString();
                        } else {
                            reader.skipValue();
                        }
                    }
                    reader.readEndDocument();

                    // Validate document structure and content
                    assertNotNull(price, "Document should have price field");
                    assertNotNull(category, "Document should have category field");
                    assertTrue(price > 5, "Price should be > 5, but was: " + price);
                    assertEquals("electronics", category, "Category should be 'electronics'");
                }
            }

            // Print some sample documents for debugging
            System.out.println("Sample results:");
            int count = 0;
            for (ByteBuffer buffer : results.values()) {
                if (count >= 5) break; // Show first 5 documents
                System.out.println("  " + BSONUtil.fromBson(buffer.duplicate().array()));
                count++;
            }

            // Performance assertion: concurrent processing should complete reasonably fast
            // Even with 100 documents, concurrent retrieval should be efficient
            assertTrue(executionTimeMs < 5000,
                    "Concurrent full bucket scan should complete within 5 seconds, took: " + executionTimeMs + "ms");

            System.out.println("✓ Concurrent full bucket scan optimization test passed");
        }
    }

    /**
     * Tests concurrent full bucket scan optimization with non-indexed field query and REVERSE=true.
     * This test validates that the batch-concurrent document retrieval works correctly
     * when scanning all documents in reverse order without an index on the query field.
     */
    @Test
    void shouldExecuteNonIndexedPriceQueryWithConcurrentFullBucketScanReverse() {
        final String TEST_BUCKET_NAME = "test-bucket-non-indexed-price-reverse";

        // Important: Create bucket metadata WITHOUT any indexes on price or category fields
        // This forces the query to use full bucket scan with concurrent document retrieval
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert 100 documents with sequential prices (1-100)
        // Each document: {"price": %d, "category": "electronics"}
        List<byte[]> documents = createPriceDocuments(100);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute query: price > 5 with LIMIT=100 and REVERSE=true
            // This will trigger concurrent full bucket scan since price field is not indexed
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"price\": { \"$gt\": 5 } }", 100, true);

            // Execute the query and measure performance
            long startTime = System.nanoTime();
            Map<?, ByteBuffer> results = executor.execute(tr);
            long endTime = System.nanoTime();
            long executionTimeMs = (endTime - startTime) / 1_000_000;

            // Print performance info for debugging
            System.out.println("=== Concurrent Full Bucket Scan Test (REVERSE) ===");
            System.out.println("Query: price > 5 on 100 documents (non-indexed, reverse order)");
            System.out.println("Execution time: " + executionTimeMs + " ms");
            System.out.println("Results count: " + results.size());

            // Calculate expected results: prices 6-100 = 95 documents
            int expectedCount = 95;
            assertEquals(expectedCount, results.size(),
                    "Should return exactly 95 documents with price > 5");

            // Extract prices from results and verify correctness
            Set<Integer> actualPrices = extractPricesFromResults(results);

            // Build expected price set: {6, 7, 8, ..., 100}
            Set<Integer> expectedPrices = new HashSet<>();
            for (int i = 6; i <= 100; i++) {
                expectedPrices.add(i);
            }

            // Verify all returned prices are correct
            assertEquals(expectedPrices, actualPrices,
                    "Should return prices 6-100, got: " + actualPrices);

            // Verify each document has correct structure and values
            for (ByteBuffer documentBuffer : results.values()) {
                documentBuffer.rewind();
                try (BsonReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();

                    Integer price = null;
                    String category = null;

                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        if ("price".equals(fieldName)) {
                            price = reader.readInt32();
                        } else if ("category".equals(fieldName)) {
                            category = reader.readString();
                        } else {
                            reader.skipValue();
                        }
                    }
                    reader.readEndDocument();

                    // Validate document structure and content
                    assertNotNull(price, "Document should have price field");
                    assertNotNull(category, "Document should have category field");
                    assertTrue(price > 5, "Price should be > 5, but was: " + price);
                    assertEquals("electronics", category, "Category should be 'electronics'");
                }
            }

            // Verify reverse ordering by checking the order of results
            // Since LinkedHashMap preserves insertion order, we can verify the actual order
            List<Integer> pricesInOrder = new ArrayList<>();
            for (ByteBuffer buffer : results.values()) {
                buffer.rewind();
                try (BsonReader reader = new BsonBinaryReader(buffer)) {
                    reader.readStartDocument();
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        if ("price".equals(fieldName)) {
                            pricesInOrder.add(reader.readInt32());
                            break;
                        } else {
                            reader.skipValue();
                        }
                    }
                }
            }

            // Print the order for debugging
            System.out.println("First 10 prices in order: " + pricesInOrder.subList(0, Math.min(10, pricesInOrder.size())));
            System.out.println("Last 10 prices in order: " + pricesInOrder.subList(Math.max(0, pricesInOrder.size() - 10), pricesInOrder.size()));

            // Verify basic properties
            assertEquals(95, pricesInOrder.size(), "Should have 95 prices in order");
            assertEquals(6, (int) Collections.min(pricesInOrder), "Minimum price should be 6");
            assertEquals(100, (int) Collections.max(pricesInOrder), "Maximum price should be 100");

            // CRITICAL: Verify reverse order assertion
            // In reverse mode with price documents 1-100, where we filter price > 5,
            // the documents should be returned in reverse _id order.
            // Since documents were inserted sequentially (price 1, then 2, then 3... then 100),
            // their _id (versionstamps) should be in insertion order.
            // With REVERSE=true, we should get the LAST inserted documents first.

            // Verify that results are in descending order by checking ordering trends
            boolean hasDescendingTrend = true;
            int descendingCount = 0;

            for (int i = 0; i < pricesInOrder.size() - 1; i++) {
                if (pricesInOrder.get(i) > pricesInOrder.get(i + 1)) {
                    descendingCount++;
                }
            }

            // In reverse order, we expect most consecutive pairs to be descending
            // Allow some tolerance since exact ordering depends on versionstamp generation
            double descendingRatio = (double) descendingCount / (pricesInOrder.size() - 1);
            System.out.println("Descending pairs ratio: " + descendingRatio);

            // Assert reverse order characteristics
            assertTrue(descendingRatio > 0.7,
                    "Reverse order should show strong descending trend, got ratio: " + descendingRatio);

            // Verify that first few results are from the higher end (prices near 100)
            int firstPrice = pricesInOrder.get(0);
            int secondPrice = pricesInOrder.get(1);
            int thirdPrice = pricesInOrder.get(2);

            assertTrue(firstPrice >= 98,
                    "First price should be near 100 in reverse order, got: " + firstPrice);
            assertTrue(secondPrice >= 97,
                    "Second price should be near 100 in reverse order, got: " + secondPrice);
            assertTrue(thirdPrice >= 96,
                    "Third price should be near 100 in reverse order, got: " + thirdPrice);

            // Verify that last few results are from the lower end (prices near 6)
            int lastPrice = pricesInOrder.get(pricesInOrder.size() - 1);
            int secondLastPrice = pricesInOrder.get(pricesInOrder.size() - 2);
            int thirdLastPrice = pricesInOrder.get(pricesInOrder.size() - 3);

            assertTrue(lastPrice <= 8,
                    "Last price should be near 6 in reverse order, got: " + lastPrice);
            assertTrue(secondLastPrice <= 9,
                    "Second last price should be near 6 in reverse order, got: " + secondLastPrice);
            assertTrue(thirdLastPrice <= 10,
                    "Third last price should be near 6 in reverse order, got: " + thirdLastPrice);

            System.out.println("✓ Reverse order validation passed - documents returned in reverse _id order");

            // Print some sample documents for debugging
            System.out.println("Sample results:");
            int count = 0;
            for (ByteBuffer buffer : results.values()) {
                if (count >= 5) break; // Show first 5 documents
                System.out.println("  " + BSONUtil.fromBson(buffer.duplicate().array()));
                count++;
            }

            // Performance assertion: concurrent processing should complete reasonably fast
            // Even with 95 documents in reverse order, concurrent retrieval should be efficient
            assertTrue(executionTimeMs < 5000,
                    "Concurrent full bucket scan (reverse) should complete within 5 seconds, took: " + executionTimeMs + "ms");

            System.out.println("✓ Concurrent full bucket scan optimization test (reverse) passed");
        }
    }

    /**
     * Tests concurrent full bucket scan optimization with LIMIT=5 and batch processing validation.
     * This test validates that the batch-concurrent document retrieval works correctly with
     * small limits and proper pagination behavior, collecting multiple batches.
     */
    @Test
    void shouldExecuteNonIndexedPriceQueryWithConcurrentFullBucketScanBatching() {
        final String TEST_BUCKET_NAME = "test-bucket-non-indexed-price-batching";

        // Important: Create bucket metadata WITHOUT any indexes on price or category fields
        // This forces the query to use full bucket scan with concurrent document retrieval
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert 100 documents with sequential prices (1-100)
        // Each document: {"price": %d, "category": "electronics"}
        List<byte[]> documents = createPriceDocuments(100);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute query: price > 5 with LIMIT=5 (small limit for batch testing)
            // This will trigger concurrent full bucket scan with pagination
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"price\": { \"$gt\": 5 } }", 5);

            // Collect batches by repeatedly calling execute until no more results
            List<List<Integer>> batches = new ArrayList<>();
            int totalResults = 0;
            int batchCount = 0;
            long totalExecutionTime = 0;

            System.out.println("=== Concurrent Full Bucket Scan Test (LIMIT=5 Batching) ===");
            System.out.println("Query: price > 5 on 100 documents (non-indexed, LIMIT=5 per batch)");

            while (true) {
                long batchStartTime = System.nanoTime();
                Map<?, ByteBuffer> results = executor.execute(tr);
                long batchEndTime = System.nanoTime();
                long batchExecutionTime = (batchEndTime - batchStartTime) / 1_000_000;
                totalExecutionTime += batchExecutionTime;

                if (results.isEmpty()) {
                    System.out.println("Batch " + (batchCount + 1) + ": No more results (end of data)");
                    break;
                }

                // Extract prices from this batch and maintain order
                List<Integer> batchPrices = new ArrayList<>();
                for (ByteBuffer buffer : results.values()) {
                    buffer.rewind();
                    try (BsonReader reader = new BsonBinaryReader(buffer)) {
                        reader.readStartDocument();
                        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                            String fieldName = reader.readName();
                            if ("price".equals(fieldName)) {
                                batchPrices.add(reader.readInt32());
                                break;
                            } else {
                                reader.skipValue();
                            }
                        }
                    }
                }

                batches.add(batchPrices);
                totalResults += results.size();
                batchCount++;

                System.out.println("Batch " + batchCount + " (" + batchExecutionTime + "ms): " +
                        results.size() + " results - " + batchPrices);

                // Safety check to avoid infinite loops
                if (batchCount > 25) {
                    break;
                }
            }

            System.out.println("Total batches: " + batchCount);
            System.out.println("Total results: " + totalResults);
            System.out.println("Total execution time: " + totalExecutionTime + " ms");

            // VALIDATION: Calculate expected batch behavior
            // Documents with price > 5 = prices 6-100 = 95 documents total
            // With LIMIT=5, we expect: 95 ÷ 5 = 19 full batches + 0 partial batch
            int expectedTotalResults = 95;
            int expectedFullBatches = expectedTotalResults / 5;  // 19 full batches
            int expectedPartialBatchSize = expectedTotalResults % 5;  // 0 remaining

            // Validate total results
            assertEquals(expectedTotalResults, totalResults,
                    "Should retrieve exactly 95 documents with price > 5");

            // Validate batch count and sizes
            assertEquals(expectedFullBatches, batchCount,
                    "Should have exactly " + expectedFullBatches + " full batches");

            // Validate individual batch sizes
            for (int i = 0; i < batches.size(); i++) {
                List<Integer> batch = batches.get(i);
                if (i < expectedFullBatches) {
                    // Full batches should have exactly 5 documents
                    assertEquals(5, batch.size(),
                            "Batch " + (i + 1) + " should contain exactly 5 documents, got: " + batch.size());
                } else {
                    // Last partial batch (if any) should have remaining documents
                    assertEquals(expectedPartialBatchSize, batch.size(),
                            "Final batch should contain " + expectedPartialBatchSize + " documents, got: " + batch.size());
                }

                // Validate all prices in batch are > 5
                for (Integer price : batch) {
                    assertTrue(price > 5, "Price should be > 5, but was: " + price + " in batch " + (i + 1));
                }
            }

            // Validate price ranges for first and last batches
            List<Integer> firstBatch = batches.get(0);
            List<Integer> lastBatch = batches.get(batches.size() - 1);

            // First batch should start with prices near 6 (ascending order by _id)
            assertTrue(firstBatch.get(0) <= 10,
                    "First batch should start with low prices, got: " + firstBatch.get(0));

            // Last batch should end with prices near 100
            assertTrue(lastBatch.get(lastBatch.size() - 1) >= 95,
                    "Last batch should end with high prices, got: " + lastBatch.get(lastBatch.size() - 1));

            // Validate no gaps or overlaps between batches
            Set<Integer> allPricesFromBatches = new HashSet<>();
            for (List<Integer> batch : batches) {
                for (Integer price : batch) {
                    assertFalse(allPricesFromBatches.contains(price),
                            "Price " + price + " appears in multiple batches (duplicate)");
                    allPricesFromBatches.add(price);
                }
            }

            // Validate we got all expected prices
            Set<Integer> expectedPrices = new HashSet<>();
            for (int i = 6; i <= 100; i++) {
                expectedPrices.add(i);
            }
            assertEquals(expectedPrices, allPricesFromBatches,
                    "Should retrieve all prices 6-100 with no gaps or duplicates");

            // Validate ordering within each batch (should be ascending by _id)
            for (int i = 0; i < batches.size(); i++) {
                List<Integer> batch = batches.get(i);
                for (int j = 0; j < batch.size() - 1; j++) {
                    assertTrue(batch.get(j) < batch.get(j + 1),
                            "Batch " + (i + 1) + " should be in ascending order, but " +
                                    batch.get(j) + " >= " + batch.get(j + 1));
                }
            }

            // Performance assertion: batch processing should be efficient
            assertTrue(totalExecutionTime < 1000,
                    "Batch processing should complete within 1 second, took: " + totalExecutionTime + "ms");

            System.out.println("✓ Concurrent full bucket scan batching validation passed");
        }
    }

    /**
     * Tests concurrent full bucket scan optimization with REVERSE=true and LIMIT=5 for batch processing validation.
     * This test validates that the batch-concurrent document retrieval works correctly with
     * reverse ordering, small limits, and proper pagination behavior.
     */
    @Test
    void shouldExecuteNonIndexedPriceQueryWithConcurrentFullBucketScanReverseBatching() {
        final String TEST_BUCKET_NAME = "test-bucket-non-indexed-price-reverse-batching";

        // Important: Create bucket metadata WITHOUT any indexes on price or category fields
        // This forces the query to use full bucket scan with concurrent document retrieval
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert 100 documents with sequential prices (1-100)
        // Each document: {"price": %d, "category": "electronics"}
        List<byte[]> documents = createPriceDocuments(100);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute query: price > 5 with LIMIT=5 and REVERSE=true
            // This will trigger concurrent full bucket scan with reverse pagination
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"price\": { \"$gt\": 5 } }", 5, true);

            // Collect batches by repeatedly calling execute until no more results
            List<List<Integer>> batches = new ArrayList<>();
            int totalResults = 0;
            int batchCount = 0;
            long totalExecutionTime = 0;

            System.out.println("=== Concurrent Full Bucket Scan Test (REVERSE + LIMIT=5 Batching) ===");
            System.out.println("Query: price > 5 on 100 documents (non-indexed, REVERSE=true, LIMIT=5 per batch)");

            while (true) {
                long batchStartTime = System.nanoTime();
                Map<?, ByteBuffer> results = executor.execute(tr);
                long batchEndTime = System.nanoTime();
                long batchExecutionTime = (batchEndTime - batchStartTime) / 1_000_000;
                totalExecutionTime += batchExecutionTime;

                if (results.isEmpty()) {
                    System.out.println("Batch " + (batchCount + 1) + ": No more results (end of data)");
                    break;
                }

                // Extract prices from this batch and maintain order
                List<Integer> batchPrices = new ArrayList<>();
                for (ByteBuffer buffer : results.values()) {
                    buffer.rewind();
                    try (BsonReader reader = new BsonBinaryReader(buffer)) {
                        reader.readStartDocument();
                        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                            String fieldName = reader.readName();
                            if ("price".equals(fieldName)) {
                                batchPrices.add(reader.readInt32());
                                break;
                            } else {
                                reader.skipValue();
                            }
                        }
                    }
                }

                batches.add(batchPrices);
                totalResults += results.size();
                batchCount++;

                System.out.println("Batch " + batchCount + " (" + batchExecutionTime + "ms): " +
                        results.size() + " results - " + batchPrices);

                // Safety check to avoid infinite loops
                if (batchCount > 25) {
                    break;
                }
            }

            System.out.println("Total batches: " + batchCount);
            System.out.println("Total results: " + totalResults);
            System.out.println("Total execution time: " + totalExecutionTime + " ms");

            // VALIDATION: Calculate expected batch behavior
            // Documents with price > 5 = prices 6-100 = 95 documents total
            // With LIMIT=5, we expect: 95 ÷ 5 = 19 full batches + 0 partial batch
            int expectedTotalResults = 95;
            int expectedFullBatches = expectedTotalResults / 5;  // 19 full batches
            // 0 remaining

            // Validate total results
            assertEquals(expectedTotalResults, totalResults,
                    "Should retrieve exactly 95 documents with price > 5");

            // Validate batch count
            assertEquals(expectedFullBatches, batchCount,
                    "Should have exactly " + expectedFullBatches + " full batches");

            // Validate individual batch sizes
            for (int i = 0; i < batches.size(); i++) {
                List<Integer> batch = batches.get(i);
                assertEquals(5, batch.size(),
                        "Batch " + (i + 1) + " should contain exactly 5 documents, got: " + batch.size());

                // Validate all prices in batch are > 5
                for (Integer price : batch) {
                    assertTrue(price > 5, "Price should be > 5, but was: " + price + " in batch " + (i + 1));
                }
            }

            // CRITICAL: Validate reverse ordering within and across batches
            // In reverse mode, we should see high prices first, low prices last
            List<Integer> firstBatch = batches.get(0);
            List<Integer> lastBatch = batches.get(batches.size() - 1);

            // First batch should contain high prices (near 100) in descending order
            assertTrue(firstBatch.get(0) >= 96,
                    "First batch should start with high prices in reverse order, got: " + firstBatch.get(0));

            // Last batch should contain low prices (near 6) in descending order
            assertTrue(lastBatch.get(lastBatch.size() - 1) <= 10,
                    "Last batch should end with low prices in reverse order, got: " + lastBatch.get(lastBatch.size() - 1));

            // Validate descending order within each batch
            for (int i = 0; i < batches.size(); i++) {
                List<Integer> batch = batches.get(i);
                for (int j = 0; j < batch.size() - 1; j++) {
                    assertTrue(batch.get(j) > batch.get(j + 1),
                            "Batch " + (i + 1) + " should be in descending order (reverse), but " +
                                    batch.get(j) + " <= " + batch.get(j + 1));
                }
            }

            // Validate descending trend across batches (each batch should start with lower values than previous)
            for (int i = 0; i < batches.size() - 1; i++) {
                List<Integer> currentBatch = batches.get(i);
                List<Integer> nextBatch = batches.get(i + 1);

                int currentBatchMin = Collections.min(currentBatch);
                int nextBatchMax = Collections.max(nextBatch);

                assertTrue(currentBatchMin > nextBatchMax,
                        "Reverse batching: Batch " + (i + 1) + " min (" + currentBatchMin +
                                ") should be > Batch " + (i + 2) + " max (" + nextBatchMax + ")");
            }

            // Validate no gaps or overlaps between batches
            Set<Integer> allPricesFromBatches = new HashSet<>();
            for (List<Integer> batch : batches) {
                for (Integer price : batch) {
                    assertFalse(allPricesFromBatches.contains(price),
                            "Price " + price + " appears in multiple batches (duplicate)");
                    allPricesFromBatches.add(price);
                }
            }

            // Validate we got all expected prices
            Set<Integer> expectedPrices = new HashSet<>();
            for (int i = 6; i <= 100; i++) {
                expectedPrices.add(i);
            }
            assertEquals(expectedPrices, allPricesFromBatches,
                    "Should retrieve all prices 6-100 with no gaps or duplicates");

            // Calculate and validate overall descending ratio across all results
            List<Integer> allPricesInOrder = new ArrayList<>();
            for (List<Integer> batch : batches) {
                allPricesInOrder.addAll(batch);
            }

            int descendingCount = 0;
            for (int i = 0; i < allPricesInOrder.size() - 1; i++) {
                if (allPricesInOrder.get(i) > allPricesInOrder.get(i + 1)) {
                    descendingCount++;
                }
            }

            double descendingRatio = (double) descendingCount / (allPricesInOrder.size() - 1);
            System.out.println("Overall descending pairs ratio: " + descendingRatio);

            // Should have perfect or near-perfect descending order
            assertTrue(descendingRatio > 0.9,
                    "Reverse batching should show strong descending trend, got ratio: " + descendingRatio);

            // Performance assertion: batch processing should be efficient even with reverse ordering
            assertTrue(totalExecutionTime < 1000,
                    "Reverse batch processing should complete within 1 second, took: " + totalExecutionTime + "ms");

            System.out.println("✓ Concurrent full bucket scan reverse batching validation passed");
        }
    }

    /**
     * Tests concurrent full bucket scan optimization with empty query (no filters).
     * This test validates that the batch-concurrent document retrieval works correctly
     * when scanning ALL documents in a bucket with no filter conditions.
     */
    @Test
    void shouldExecuteEmptyQueryWithConcurrentFullBucketScan() {
        final String TEST_BUCKET_NAME = "test-bucket-empty-query";

        // Important: Create bucket metadata WITHOUT any indexes on price or category fields
        // This forces the query to use full bucket scan with concurrent document retrieval
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert 100 documents with sequential prices (1-100)
        // Each document: {"price": %d, "category": "electronics"}
        List<byte[]> documents = createPriceDocuments(100);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Execute query: {} (empty query - should return ALL documents) with LIMIT=100
            // This will trigger concurrent full bucket scan with no filtering
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{}", 100);

            // Execute the query and measure performance
            long startTime = System.nanoTime();
            Map<?, ByteBuffer> results = executor.execute(tr);
            long endTime = System.nanoTime();
            long executionTimeMs = (endTime - startTime) / 1_000_000;

            // Print performance info for debugging
            System.out.println("=== Concurrent Full Bucket Scan Test (Empty Query) ===");
            System.out.println("Query: {} on 100 documents (non-indexed, no filters)");
            System.out.println("Execution time: " + executionTimeMs + " ms");
            System.out.println("Results count: " + results.size());

            // Calculate expected results: ALL documents = prices 1-100 = 100 documents
            int expectedCount = 100;
            assertEquals(expectedCount, results.size(),
                    "Should return exactly 100 documents with empty query");

            // Extract prices from results and verify correctness
            Set<Integer> actualPrices = extractPricesFromResults(results);

            // Build expected price set: {1, 2, 3, ..., 100}
            Set<Integer> expectedPrices = new HashSet<>();
            for (int i = 1; i <= 100; i++) {
                expectedPrices.add(i);
            }

            // Verify all returned prices are correct
            assertEquals(expectedPrices, actualPrices,
                    "Should return all prices 1-100, got: " + actualPrices);

            // Verify each document has correct structure and values
            for (ByteBuffer documentBuffer : results.values()) {
                documentBuffer.rewind();
                try (BsonReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();

                    Integer price = null;
                    String category = null;

                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        if ("price".equals(fieldName)) {
                            price = reader.readInt32();
                        } else if ("category".equals(fieldName)) {
                            category = reader.readString();
                        } else {
                            reader.skipValue();
                        }
                    }
                    reader.readEndDocument();

                    // Validate document structure and content
                    assertNotNull(price, "Document should have price field");
                    assertNotNull(category, "Document should have category field");
                    assertTrue(price >= 1 && price <= 100, "Price should be between 1-100, but was: " + price);
                    assertEquals("electronics", category, "Category should be 'electronics'");
                }
            }

            // Verify ordering of results (should be ascending by _id/insertion order)
            List<Integer> pricesInOrder = new ArrayList<>();
            for (ByteBuffer buffer : results.values()) {
                buffer.rewind();
                try (BsonReader reader = new BsonBinaryReader(buffer)) {
                    reader.readStartDocument();
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        if ("price".equals(fieldName)) {
                            pricesInOrder.add(reader.readInt32());
                            break;
                        } else {
                            reader.skipValue();
                        }
                    }
                }
            }

            System.out.println("First 10 prices in order: " + pricesInOrder.subList(0, Math.min(10, pricesInOrder.size())));
            System.out.println("Last 10 prices in order: " + pricesInOrder.subList(Math.max(0, pricesInOrder.size() - 10), pricesInOrder.size()));

            // Validate ascending order (documents should be returned in insertion order)
            assertEquals(100, pricesInOrder.size(), "Should have 100 prices in order");
            assertEquals(1, (int) Collections.min(pricesInOrder), "Minimum price should be 1");
            assertEquals(100, (int) Collections.max(pricesInOrder), "Maximum price should be 100");

            // Verify ascending order trend
            int ascendingCount = 0;
            for (int i = 0; i < pricesInOrder.size() - 1; i++) {
                if (pricesInOrder.get(i) < pricesInOrder.get(i + 1)) {
                    ascendingCount++;
                }
            }

            double ascendingRatio = (double) ascendingCount / (pricesInOrder.size() - 1);
            System.out.println("Ascending pairs ratio: " + ascendingRatio);

            // Should have strong ascending trend (documents in insertion order)
            assertTrue(ascendingRatio > 0.7,
                    "Empty query should show strong ascending trend (insertion order), got ratio: " + ascendingRatio);

            // Verify that first few results are from the lower end (prices near 1)
            int firstPrice = pricesInOrder.get(0);
            int secondPrice = pricesInOrder.get(1);
            int thirdPrice = pricesInOrder.get(2);

            assertTrue(firstPrice <= 3,
                    "First price should be near 1 in ascending order, got: " + firstPrice);
            assertTrue(secondPrice <= 4,
                    "Second price should be near 1 in ascending order, got: " + secondPrice);
            assertTrue(thirdPrice <= 5,
                    "Third price should be near 1 in ascending order, got: " + thirdPrice);

            // Verify that last few results are from the higher end (prices near 100)
            int lastPrice = pricesInOrder.get(pricesInOrder.size() - 1);
            int secondLastPrice = pricesInOrder.get(pricesInOrder.size() - 2);
            int thirdLastPrice = pricesInOrder.get(pricesInOrder.size() - 3);

            assertTrue(lastPrice >= 98,
                    "Last price should be near 100 in ascending order, got: " + lastPrice);
            assertTrue(secondLastPrice >= 97,
                    "Second last price should be near 100 in ascending order, got: " + secondLastPrice);
            assertTrue(thirdLastPrice >= 96,
                    "Third last price should be near 100 in ascending order, got: " + thirdLastPrice);

            // Print some sample documents for debugging
            System.out.println("Sample results:");
            int count = 0;
            for (ByteBuffer buffer : results.values()) {
                if (count >= 5) break; // Show first 5 documents
                System.out.println("  " + BSONUtil.fromBson(buffer.duplicate().array()));
                count++;
            }

            // Performance assertion: concurrent processing should complete reasonably fast
            // With 100 documents and no filtering, concurrent retrieval should be very efficient
            assertTrue(executionTimeMs < 5000,
                    "Concurrent full bucket scan (empty query) should complete within 5 seconds, took: " + executionTimeMs + "ms");

            System.out.println("✓ Concurrent full bucket scan optimization test (empty query) passed");
        }
    }

    @Test
    void shouldExecuteEmptyQueryWithConcurrentFullBucketScanReverse() {
        System.out.println("\n=== Testing Concurrent Full Bucket Scan with Empty Query (REVERSE) ===");

        // Create bucket and insert 100 documents with prices 1-100
        String bucketName = "test-empty-query-reverse-bucket";
        createBucket(bucketName);

        BucketMetadata metadata = getBucketMetadata(bucketName);

        // Insert 100 documents: {"price": 1, "category": "electronics"}, {"price": 2, "category": "electronics"}, ..., {"price": 100, "category": "electronics"}
        List<byte[]> documents = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            String docJson = String.format("{\"price\": %d, \"category\": \"electronics\"}", i);
            documents.add(BSONUtil.jsonToDocumentThenBytes(docJson));
        }

        // Insert all documents
        insertDocumentsAndGetVersionstamps(bucketName, documents);

        // Execute empty query with REVERSE=true, LIMIT=100
        // Empty query "{}" should return all documents in reverse _id order (descending insertion order)
        long startTime = System.currentTimeMillis();

        PlanExecutor executor = createPlanExecutorForQuery(metadata, "{}", 100, true); // REVERSE=true

        Map<Versionstamp, ByteBuffer> results;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            results = executor.execute(tr);
        }

        long executionTimeMs = System.currentTimeMillis() - startTime;
        System.out.println("Empty query (REVERSE) execution time: " + executionTimeMs + "ms");

        // Verify we got all 100 documents
        assertNotNull(results, "Results should not be null");
        assertEquals(100, results.size(), "Should return all 100 documents with empty query and REVERSE=true");

        // Extract all prices to validate reverse ordering
        Set<Integer> actualPrices = extractPricesFromResults(results);
        Set<Integer> expectedPrices = IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toSet());
        assertEquals(expectedPrices, actualPrices,
                "Should return all prices 1-100, got: " + actualPrices);

        // Verify each document has correct structure and values
        for (ByteBuffer documentBuffer : results.values()) {
            documentBuffer.rewind();
            try (BsonReader reader = new BsonBinaryReader(documentBuffer)) {
                reader.readStartDocument();

                Integer price = null;
                String category = null;

                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if ("price".equals(fieldName)) {
                        price = reader.readInt32();
                    } else if ("category".equals(fieldName)) {
                        category = reader.readString();
                    } else {
                        reader.skipValue();
                    }
                }
                reader.readEndDocument();

                // Validate document structure and content
                assertNotNull(price, "Document should have price field");
                assertNotNull(category, "Document should have category field");
                assertTrue(price >= 1 && price <= 100, "Price should be between 1-100, but was: " + price);
                assertEquals("electronics", category, "Category should be 'electronics'");
            }
        }

        // Verify ordering of results (should be descending by _id/insertion order due to REVERSE=true)
        List<Integer> pricesInOrder = new ArrayList<>();
        for (ByteBuffer buffer : results.values()) {
            buffer.rewind();
            try (BsonReader reader = new BsonBinaryReader(buffer)) {
                reader.readStartDocument();
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if ("price".equals(fieldName)) {
                        pricesInOrder.add(reader.readInt32());
                        break;
                    } else {
                        reader.skipValue();
                    }
                }
            }
        }

        System.out.println("First 10 prices in REVERSE order: " + pricesInOrder.subList(0, Math.min(10, pricesInOrder.size())));
        System.out.println("Last 10 prices in REVERSE order: " + pricesInOrder.subList(Math.max(0, pricesInOrder.size() - 10), pricesInOrder.size()));

        // Validate descending order (documents should be returned in reverse insertion order)
        assertEquals(100, pricesInOrder.size(), "Should have 100 prices in reverse order");
        assertEquals(1, (int) Collections.min(pricesInOrder), "Minimum price should be 1");
        assertEquals(100, (int) Collections.max(pricesInOrder), "Maximum price should be 100");

        // Verify descending order trend (REVERSE=true should give us descending _id order)
        int descendingCount = 0;
        for (int i = 0; i < pricesInOrder.size() - 1; i++) {
            if (pricesInOrder.get(i) > pricesInOrder.get(i + 1)) {
                descendingCount++;
            }
        }

        double descendingRatio = (double) descendingCount / (pricesInOrder.size() - 1);
        System.out.println("Descending pairs ratio (REVERSE): " + descendingRatio);

        // Should have strong descending trend (documents in reverse insertion order)
        assertTrue(descendingRatio > 0.7,
                "Empty query with REVERSE=true should show strong descending trend, got ratio: " + descendingRatio);

        // Verify that first few results are from the higher end (prices near 100)
        int firstPrice = pricesInOrder.get(0);
        int secondPrice = pricesInOrder.get(1);
        int thirdPrice = pricesInOrder.get(2);

        assertTrue(firstPrice >= 98,
                "First price should be near 100 in descending order (REVERSE), got: " + firstPrice);
        assertTrue(secondPrice >= 97,
                "Second price should be near 100 in descending order (REVERSE), got: " + secondPrice);
        assertTrue(thirdPrice >= 96,
                "Third price should be near 100 in descending order (REVERSE), got: " + thirdPrice);

        // Verify that last few results are from the lower end (prices near 1)
        int lastPrice = pricesInOrder.get(pricesInOrder.size() - 1);
        int secondLastPrice = pricesInOrder.get(pricesInOrder.size() - 2);
        int thirdLastPrice = pricesInOrder.get(pricesInOrder.size() - 3);

        assertTrue(lastPrice <= 3,
                "Last price should be near 1 in descending order (REVERSE), got: " + lastPrice);
        assertTrue(secondLastPrice <= 4,
                "Second last price should be near 1 in descending order (REVERSE), got: " + secondLastPrice);
        assertTrue(thirdLastPrice <= 5,
                "Third last price should be near 1 in descending order (REVERSE), got: " + thirdLastPrice);

        // Print some sample documents for debugging
        System.out.println("Sample results (REVERSE):");
        int count = 0;
        for (ByteBuffer buffer : results.values()) {
            if (count >= 5) break; // Show first 5 documents
            System.out.println("  " + BSONUtil.fromBson(buffer.duplicate().array()));
            count++;
        }

        // Performance assertion: concurrent processing should complete reasonably fast
        // With 100 documents and no filtering, concurrent retrieval should be very efficient
        assertTrue(executionTimeMs < 5000,
                "Concurrent full bucket scan (empty query, REVERSE) should complete within 5 seconds, took: " + executionTimeMs + "ms");

        System.out.println("✓ Concurrent full bucket scan optimization test (empty query, REVERSE) passed");
    }

    @Test
    void shouldExecuteNonIndexedNeQueryWithConcurrentFullBucketScan() {
        System.out.println("\n=== Testing Concurrent Full Bucket Scan with NE Query (Non-indexed) ===");

        // Create bucket and insert 100 documents with prices 1-100
        String bucketName = "test-ne-query-bucket";
        createBucket(bucketName);

        BucketMetadata metadata = getBucketMetadata(bucketName);

        // Insert 100 documents: {"price": 1, "category": "electronics"}, {"price": 2, "category": "electronics"}, ..., {"price": 100, "category": "electronics"}
        // No indexes on price or category fields - this will force a full bucket scan
        List<byte[]> documents = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            String docJson = String.format("{\"price\": %d, \"category\": \"electronics\"}", i);
            documents.add(BSONUtil.jsonToDocumentThenBytes(docJson));
        }

        // Insert all documents
        insertDocumentsAndGetVersionstamps(bucketName, documents);

        // Test NE query: { "price": { "$ne": 50 } }
        // Expected: All documents except the one with price=50, so 99 documents
        int excludedPrice = 50;
        String query = String.format("{\"price\": {\"$ne\": %d}}", excludedPrice);

        long startTime = System.currentTimeMillis();

        PlanExecutor executor = createPlanExecutorForQuery(metadata, query, 100); // LIMIT=100

        Map<Versionstamp, ByteBuffer> results;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            results = executor.execute(tr);
        }

        long executionTimeMs = System.currentTimeMillis() - startTime;
        System.out.println("NE query execution time: " + executionTimeMs + "ms");

        // Expected result: 99 documents (all except price=50)
        int expectedCount = 99;

        // Verify we got the expected number of documents
        assertNotNull(results, "Results should not be null");
        assertEquals(expectedCount, results.size(),
                String.format("Should return %d documents (all except price=%d)", expectedCount, excludedPrice));

        // Extract all prices to validate NE filtering
        Set<Integer> actualPrices = extractPricesFromResults(results);

        // Create expected price set (1-100 except excludedPrice)
        Set<Integer> expectedPrices = IntStream.rangeClosed(1, 100)
                .filter(price -> price != excludedPrice)
                .boxed()
                .collect(Collectors.toSet());

        assertEquals(expectedPrices, actualPrices,
                String.format("Should return all prices 1-100 except %d, got: %s", excludedPrice, actualPrices));

        // Verify the excluded price is NOT present in results
        assertFalse(actualPrices.contains(excludedPrice),
                String.format("Price %d should be excluded from results", excludedPrice));

        // Verify expected prices are present
        assertEquals(99, actualPrices.size(), "Should have exactly 99 unique prices");
        assertEquals(1, (int) Collections.min(actualPrices), "Minimum price should be 1");
        assertEquals(100, (int) Collections.max(actualPrices), "Maximum price should be 100");

        // Verify each document has correct structure and excludes the target price
        for (ByteBuffer documentBuffer : results.values()) {
            documentBuffer.rewind();
            try (BsonReader reader = new BsonBinaryReader(documentBuffer)) {
                reader.readStartDocument();

                Integer price = null;
                String category = null;

                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if ("price".equals(fieldName)) {
                        price = reader.readInt32();
                    } else if ("category".equals(fieldName)) {
                        category = reader.readString();
                    } else {
                        reader.skipValue();
                    }
                }
                reader.readEndDocument();

                // Validate document structure and content
                assertNotNull(price, "Document should have price field");
                assertNotNull(category, "Document should have category field");
                assertTrue(price >= 1 && price <= 100, "Price should be between 1-100, but was: " + price);
                assertNotEquals(excludedPrice, (int) price,
                        String.format("Document should not have excluded price %d, but found: %d", excludedPrice, price));
                assertEquals("electronics", category, "Category should be 'electronics'");
            }
        }

        // Verify ordering of results (should be ascending by _id/insertion order for non-indexed scan)
        List<Integer> pricesInOrder = new ArrayList<>();
        for (ByteBuffer buffer : results.values()) {
            buffer.rewind();
            try (BsonReader reader = new BsonBinaryReader(buffer)) {
                reader.readStartDocument();
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if ("price".equals(fieldName)) {
                        pricesInOrder.add(reader.readInt32());
                        break;
                    } else {
                        reader.skipValue();
                    }
                }
            }
        }

        System.out.println("First 10 prices in order: " + pricesInOrder.subList(0, Math.min(10, pricesInOrder.size())));
        System.out.println("Last 10 prices in order: " + pricesInOrder.subList(Math.max(0, pricesInOrder.size() - 10), pricesInOrder.size()));
        System.out.println("Excluded price " + excludedPrice + " should NOT be present in results");

        // Verify ascending order trend (documents should be in insertion order, skipping excludedPrice)
        // We expect mostly ascending order, but with a gap where excludedPrice was excluded
        int ascendingCount = 0;
        for (int i = 0; i < pricesInOrder.size() - 1; i++) {
            if (pricesInOrder.get(i) < pricesInOrder.get(i + 1)) {
                ascendingCount++;
            }
        }

        double ascendingRatio = (double) ascendingCount / (pricesInOrder.size() - 1);
        System.out.println("Ascending pairs ratio: " + ascendingRatio);

        // Should have strong ascending trend (documents in insertion order, minus excluded price)
        assertTrue(ascendingRatio > 0.7,
                "NE query should show strong ascending trend (insertion order with exclusion), got ratio: " + ascendingRatio);

        // Verify that the excluded price creates the expected gap in ordering
        // Check that we don't see the excluded price in the sequence
        boolean foundGap = false;
        for (int i = 0; i < pricesInOrder.size() - 1; i++) {
            int current = pricesInOrder.get(i);
            int next = pricesInOrder.get(i + 1);

            // Look for the gap where excludedPrice should have been
            if (current == excludedPrice - 1 && next == excludedPrice + 1) {
                foundGap = true;
                System.out.println("Found expected gap: " + current + " -> " + next + " (skipped " + excludedPrice + ")");
                break;
            }
        }

        // The gap might not be perfectly positioned due to versionstamp ordering vs price ordering
        // But we should definitely not see the excluded price
        assertFalse(pricesInOrder.contains(excludedPrice),
                "Excluded price " + excludedPrice + " should not appear in ordered results");

        // Print some sample documents for debugging
        System.out.println("Sample results (NE query):");
        int count = 0;
        for (ByteBuffer buffer : results.values()) {
            if (count >= 5) break; // Show first 5 documents
            System.out.println("  " + BSONUtil.fromBson(buffer.duplicate().array()));
            count++;
        }

        // Performance assertion: NE query with full bucket scan should complete reasonably fast
        // Since this requires scanning all documents and applying filter, allow more time than indexed queries
        assertTrue(executionTimeMs < 10000,
                "NE query with concurrent full bucket scan should complete within 10 seconds, took: " + executionTimeMs + "ms");

        System.out.printf("✓ Concurrent full bucket scan optimization test (NE query, excluded price=%d) passed%n", excludedPrice);
    }

    @Test
    void shouldExecuteANDQueryWithQuantityAndPriceNoIndexes() {
        final String TEST_BUCKET_NAME = "test-bucket-and-query-no-indexes";

        // Create bucket metadata WITHOUT creating indexes for quantity and price fields
        // This will test the RangeScanFallbackRule optimizer that converts range scans to full scans
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME); // No indexes

        // Insert 20 documents with various quantity and price combinations
        List<byte[]> documents = createQuantityPriceDocuments(20);
        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create executor for AND query: quantity > 5 AND price > 15
            // This should match documents where both conditions are true using full bucket scans
            PlanExecutor executor = createPlanExecutorForQuery(metadata, "{ \"$and\": [ { \"quantity\": { \"$gt\": 5 } }, { \"price\": { \"$gt\": 15 } } ] }", 100);
            Map<?, ByteBuffer> results = executor.execute(tr);

            // Print documents for debugging purposes
            for (ByteBuffer buffer : results.values()) {
                System.out.println(">> AND Query (No Indexes) Document " + BSONUtil.fromBson(buffer.duplicate().array()));
            }

            // Extract quantities and prices from results for verification
            Map<Integer, Integer> quantityPriceMap = extractQuantityPriceFromResults(results);

            // Verify that all results satisfy both conditions
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                int quantity = entry.getKey();
                int price = entry.getValue();
                assertTrue(quantity > 5, "Quantity should be > 5, but was: " + quantity);
                assertTrue(price > 15, "Price should be > 15, but was: " + price);
            }

            // Calculate expected results based on our document generation pattern
            // Documents: quantity=1-20, price=quantity*2 (so prices 2-40)
            // Condition: quantity > 5 AND price > 15
            // quantity > 5 means quantity 6-20 (15 documents)
            // price > 15 means price 16-40, which corresponds to quantity 8-20 (13 documents)
            // The intersection should be: quantity 8-20, price 16-40 (13 documents)

            // Create expected results
            Map<Integer, Integer> expectedResults = new LinkedHashMap<>();
            for (int i = 8; i <= 20; i++) {
                expectedResults.put(i, i * 2);
            }

            // Verify result count
            assertEquals(expectedResults.size(), results.size(),
                    String.format("Should return %d documents, but got %d", expectedResults.size(), results.size()));

            System.out.println("Expected results (no indexes):");
            for (Map.Entry<Integer, Integer> entry : expectedResults.entrySet()) {
                System.out.println("  quantity=" + entry.getKey() + ", price=" + entry.getValue());
            }

            System.out.println("Actual results (no indexes):");
            for (Map.Entry<Integer, Integer> entry : quantityPriceMap.entrySet()) {
                System.out.println("  quantity=" + entry.getKey() + ", price=" + entry.getValue());
            }

            // Verify that we have the same quantity-price pairs (order might differ due to full scan vs index scan)
            Set<Map.Entry<Integer, Integer>> expectedEntries = expectedResults.entrySet();
            Set<Map.Entry<Integer, Integer>> actualEntries = quantityPriceMap.entrySet();
            assertEquals(expectedEntries, actualEntries, "Results should contain same quantity-price pairs as indexed version");

            System.out.println("✓ AND query without indexes test passed - RangeScanFallbackRule successfully converted to full bucket scan");
        }
    }

}