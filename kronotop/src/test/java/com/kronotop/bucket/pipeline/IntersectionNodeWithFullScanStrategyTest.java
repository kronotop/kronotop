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
import org.bson.BsonBinaryReader;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IntersectionNodeWithFullScanStrategyTest extends BasePipelineTest {

    @Test
    void testIntersectionContradiction() {
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

        PipelineNode plan = createExecutionPlan(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$eq': 'John', '$ne': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                System.out.println(BSONUtil.fromBson(buffer.array()).toJson());
            }
            assertEquals(0, results.size());
        }
    }

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

        PipelineNode plan = createExecutionPlan(metadata, "{ $and: [ { 'age': { '$gt': 22 } }, { 'name': { '$gte': 'John' } } ] }");
        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results.values()) {
                System.out.println(BSONUtil.fromBson(buffer.array()).toJson());
            }

            assertEquals(2, results.size(), "Should return exactly 3 documents with age > 22");

            assertEquals(Set.of("John"), extractNamesFromResults(results));
            assertEquals(Set.of(35, 47), extractIntegerFieldFromResults(results, "age"));
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

        PipelineNode plan = createExecutionPlan(metadata, "{ $and: [ { 'age': { '$gte': 10 } }, { 'category': { '$eq': 'electronics' } } ] }");
        QueryOptions config = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        Map<Object, ByteBuffer> allResults = new LinkedHashMap<>();
        int iterationCount = 0;
        int totalBatchSize = 0;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            while (true) {
                iterationCount++;
                Map<?, ByteBuffer> batchResults = readExecutor.execute(tr, ctx);

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
        Set<Integer> ages = extractIntegerFieldFromResults(allResults, "age");

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

    @Test
    void testIntersectionWithTwoHundredDocumentsBatchProcessingReverse() {
        final String TEST_BUCKET_NAME = "test-intersection-200-docs-reverse";

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

        PipelineNode plan = createExecutionPlan(metadata, "{ $and: [ { 'age': { '$gte': 10 } }, { 'category': { '$eq': 'electronics' } } ] }");
        QueryOptions config = QueryOptions.builder().limit(2).reverse(true).build();
        QueryContext ctx = new QueryContext(metadata, config, plan);

        Map<Object, ByteBuffer> allResults = new LinkedHashMap<>();
        int iterationCount = 0;
        int totalBatchSize = 0;
        List<Integer> ageSequence = new ArrayList<>(); // Track the sequence of ages

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            while (true) {
                iterationCount++;
                Map<?, ByteBuffer> batchResults = readExecutor.execute(tr, ctx);

                if (batchResults.isEmpty()) {
                    break;
                }

                // Track batch size for verification
                totalBatchSize += batchResults.size();
                allResults.putAll(batchResults);

                // Track age sequence for reverse order verification
                for (ByteBuffer buffer : batchResults.values()) {
                    buffer.rewind();
                    try (BsonBinaryReader reader = new BsonBinaryReader(buffer)) {
                        reader.readStartDocument();
                        boolean ageFound = false;
                        while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT) {
                            String fieldName = reader.readName();
                            if ("age".equals(fieldName)) {
                                ageSequence.add(reader.readInt32());
                                ageFound = true;
                            } else {
                                reader.skipValue();
                            }
                        }
                        reader.readEndDocument();
                    }
                }

                // Log batch content for verification
                System.out.println("Reverse Iteration " + iterationCount + ", batch size: " + batchResults.size());
                for (ByteBuffer buffer : batchResults.values()) {
                    buffer.rewind();
                    System.out.println("  Document: " + BSONUtil.fromBson(buffer.array()).toJson());
                }
            }
        }

        // Verify the results
        assertEquals(150, allResults.size(), "Should return exactly 150 matching documents");

        // Verify all returned documents match the condition
        Set<String> categories = extractCategoriesFromResults(allResults);
        Set<Integer> ages = extractIntegerFieldFromResults(allResults, "age");

        assertEquals(Set.of("electronics"), categories, "All documents should have category 'electronics'");
        assertTrue(ages.stream().allMatch(age -> age >= 10), "All ages should be >= 10");

        // Verify batch processing worked correctly
        assertEquals(150, totalBatchSize, "Total batch size should equal result count");

        // The iteration count includes the final empty iteration, so it should be one more than 150/2
        int expectedIterations = (150 / 2) + 1; // 75 + 1 = 76 iterations (including final empty check)
        assertEquals(expectedIterations, iterationCount, "Should take " + expectedIterations + " iterations (including final empty iteration)");

        // Verify reverse ordering - ages should be in descending order
        assertTrue(isDescendingOrder(ageSequence), "Ages should be in descending order for reverse pagination");

        // First document should have the highest age (159), last should have lowest (10)
        assertEquals(159, ageSequence.get(0), "First document should have age 159");
        assertEquals(10, ageSequence.get(ageSequence.size() - 1), "Last document should have age 10");

        System.out.println("Reverse - Total iterations: " + iterationCount);
        System.out.println("Reverse - Total results: " + allResults.size());
        System.out.println("Reverse - Age range: " + ages.stream().max(Integer::compareTo).orElse(0) + " to " + ages.stream().min(Integer::compareTo).orElse(0));
        System.out.println("Reverse - Age sequence (first 10): " + ageSequence.subList(0, Math.min(10, ageSequence.size())));
        System.out.println("Reverse - Age sequence (last 10): " + ageSequence.subList(Math.max(0, ageSequence.size() - 10), ageSequence.size()));
    }

    // Helper method to check if a list of integers is in descending order
    private boolean isDescendingOrder(List<Integer> ages) {
        for (int i = 1; i < ages.size(); i++) {
            if (ages.get(i) > ages.get(i - 1)) {
                return false;
            }
        }
        return true;
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
