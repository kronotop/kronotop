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
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for nested AND/OR query execution.
 */
class NestedQueryIntegrationTest extends BasePlanExecutorTest {

    @Test
    void testNestedAndOrQueryReturnsCorrectIntersection() {
        final String TEST_BUCKET_NAME = "test-bucket-nested-investigation";

        // Create bucket metadata without any secondary indexes
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert the specific test documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Laptop\", \"quantity\": 10, \"price\": 1200, \"category\": \"electronics\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Headphones\", \"quantity\": 3, \"price\": 50, \"category\": \"electronics\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Book A\", \"quantity\": 7, \"price\": 12, \"category\": \"book\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Book B\", \"quantity\": 2, \"price\": 20, \"category\": \"book\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Pen\", \"quantity\": 30, \"price\": 2, \"category\": \"stationery\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Notebook\", \"quantity\": 15, \"price\": 6, \"category\": \"stationery\" }")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String complexQuery = """
                    {
                      "$and": [
                        {
                          "$or": [
                            { "quantity": { "$gt": 5 } },
                            { "price": { "$lt": 10 } }
                          ]
                        },
                        {
                          "$or": [
                            { "category": { "$eq": "book" } },
                            {
                              "$and": [
                                { "category": { "$eq": "electronics" } },
                                { "price": { "$gt": 100 } }
                              ]
                            }
                          ]
                        }
                      ]
                    }
                    """;
            PlanExecutor complexExecutor = createPlanExecutorForQuery(metadata, complexQuery, 10);
            Map<?, ByteBuffer> complexResults = complexExecutor.execute(tr);
            for (ByteBuffer buffer : complexResults.values()) {
                System.out.println("Result >> " +  BSONUtil.fromBson(buffer.array()).toJson());
            }

            // Verify correct intersection: should return exactly Laptop and Book A
            assertEquals(2, complexResults.size(),
                    "Nested AND/OR query should return exactly 2 documents (Laptop and Book A)");

            // Extract document names for verification
            List<String> resultNames = complexResults.values().stream()
                    .map(buf -> BSONUtil.fromBson(buf.array()).getString("name"))
                    .sorted()
                    .toList();

            assertEquals(List.of("Book A", "Laptop"), resultNames,
                    "Result should contain exactly Laptop and Book A");
        }
    }

    @Test
    void testNestedOrWithAndBranchesReturnsCorrectUnion() {
        final String TEST_BUCKET_NAME = "test-bucket-nested-or-and";

        // Create bucket metadata without any secondary indexes
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // Insert the specific test documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Phone\", \"quantity\": 2, \"price\": 800, \"category\": \"electronics\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"TV\", \"quantity\": 12, \"price\": 1500, \"category\": \"electronics\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Book C\", \"quantity\": 1, \"price\": 8, \"category\": \"book\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Book D\", \"quantity\": 9, \"price\": 22, \"category\": \"book\" }"),
                BSONUtil.jsonToDocumentThenBytes("{ \"name\": \"Chair\", \"quantity\": 15, \"price\": 45, \"category\": \"furniture\" }")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String nestedOrQuery = """
                    {
                      "$or": [
                        {
                          "$and": [
                            { "category": { "$eq": "book" } },
                            { "quantity": { "$gt": 5 } }
                          ]
                        },
                        {
                          "$and": [
                            { "category": { "$eq": "electronics" } },
                            { "price": { "$lt": 1000 } }
                          ]
                        }
                      ]
                    }
                    """;

            PlanExecutor executor = createPlanExecutorForQuery(metadata, nestedOrQuery, 10);
            Map<?, ByteBuffer> results = executor.execute(tr);

            System.out.println("=== Nested OR(AND, AND) Results ===");
            System.out.println("Total results: " + results.size());
            for (ByteBuffer buffer : results.values()) {
                System.out.println("Result >> " + BSONUtil.fromBson(buffer.array()).toJson());
            }

            System.out.println("\n=== Expected Analysis ===");
            System.out.println("First AND (category=book AND quantity>5): Should match Book D");
            System.out.println("Second AND (category=electronics AND price<1000): Should match Phone");
            System.out.println("OR Union: Should return both Phone and Book D");

            // This test should FAIL until the nested OR(AND, AND) bug is fixed
            // The query should return 2 documents (Phone and Book D) but currently only returns 1 (Book D)
            // This indicates a bug in nested OR execution where the second AND branch is not processed correctly

            System.out.println("\n=== BUG DETECTED: Missing results from second AND branch ===");
            System.out.println("Expected: 2 documents (Phone + Book D)");
            System.out.println("Actual: " + results.size() + " documents");
            if (results.size() < 2) {
                System.out.println("Missing: Phone from second AND branch (category=electronics AND price<1000)");
            }

            // Verify correct union: should return exactly Phone and Book D
            assertEquals(2, results.size(),
                    "Nested OR(AND, AND) query should return exactly 2 documents (Phone and Book D)");

            // Extract document names for verification
            List<String> resultNames = results.values().stream()
                    .map(buf -> BSONUtil.fromBson(buf.array()).getString("name"))
                    .sorted()
                    .toList();

            assertEquals(List.of("Book D", "Phone"), resultNames,
                    "Result should contain exactly Phone and Book D");

            /*
            QUERY ANALYSIS:
            First AND branch: category=book AND quantity>5
            - Phone: category=electronics (✗), quantity=2 (✗) → No match
            - TV: category=electronics (✗), quantity=12 (✓) → No match
            - Book C: category=book (✓), quantity=1 (✗) → No match
            - Book D: category=book (✓), quantity=9 (✓) → Match ✓
            - Chair: category=furniture (✗), quantity=15 (✓) → No match
            → First AND: Book D only

            Second AND branch: category=electronics AND price<1000
            - Phone: category=electronics (✓), price=800 (✓) → Match ✓
            - TV: category=electronics (✓), price=1500 (✗) → No match
            - Book C: category=book (✗), price=8 (✓) → No match
            - Book D: category=book (✗), price=22 (✓) → No match
            - Chair: category=furniture (✗), price=45 (✓) → No match
            → Second AND: Phone only

            OR union: Book D ∪ Phone = {Book D, Phone}

            EXPECTED RESULT:
                [
                    { "name": "Phone", "quantity": 2, "price": 800, "category": "electronics" },
                    { "name": "Book D", "quantity": 9, "price": 22, "category": "book" }
                ]
            */
        }
    }
}