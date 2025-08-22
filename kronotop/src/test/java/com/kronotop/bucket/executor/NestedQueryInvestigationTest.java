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

/**
 * Investigation test for nested AND/OR query execution issues.
 */
class NestedQueryInvestigationTest extends BasePlanExecutorTest {

    @Test
    void investigateNestedQueryIssue() {
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
            // Test 1: Verify all documents are inserted
            System.out.println("=== Test 1: All Documents ===");
            PlanExecutor allExecutor = createPlanExecutorForQuery(metadata, "{}", 10);
            Map<?, ByteBuffer> allResults = allExecutor.execute(tr);
            System.out.println("All documents count: " + allResults.size());

            // Test 2: Simple equality queries
            System.out.println("\n=== Test 2: Simple Equality ===");
            PlanExecutor bookExecutor = createPlanExecutorForQuery(metadata, "{ \"category\": \"book\" }", 10);
            Map<?, ByteBuffer> bookResults = bookExecutor.execute(tr);
            System.out.println("Books count: " + bookResults.size());

            // Test 3: Simple OR query
            System.out.println("\n=== Test 3: Simple OR ===");
            PlanExecutor orExecutor = createPlanExecutorForQuery(metadata,
                    "{ \"$or\": [ { \"category\": \"book\" }, { \"category\": \"electronics\" } ] }", 10);
            Map<?, ByteBuffer> orResults = orExecutor.execute(tr);
            System.out.println("Books OR Electronics count: " + orResults.size());

            // Test 4: Simple AND query
            System.out.println("\n=== Test 4: Simple AND ===");
            PlanExecutor andExecutor = createPlanExecutorForQuery(metadata,
                    "{ \"$and\": [ { \"category\": \"electronics\" }, { \"price\": { \"$gt\": 100 } } ] }", 10);
            Map<?, ByteBuffer> andResults = andExecutor.execute(tr);
            System.out.println("Electronics AND price>100 count: " + andResults.size());

            // Test 5: Nested AND(OR) query
            System.out.println("\n=== Test 5: Nested AND(OR) ===");
            PlanExecutor nestedExecutor = createPlanExecutorForQuery(metadata,
                    "{ \"$and\": [ { \"$or\": [ { \"category\": \"book\" }, { \"category\": \"electronics\" } ] }, { \"quantity\": { \"$gt\": 5 } } ] }", 10);
            Map<?, ByteBuffer> nestedResults = nestedExecutor.execute(tr);
            System.out.println("AND(OR(books, electronics), quantity>5) count: " + nestedResults.size());

            // Test 6: First part of complex query only
            System.out.println("\n=== Test 6: First OR Part ===");
            PlanExecutor firstOrExecutor = createPlanExecutorForQuery(metadata,
                    "{ \"$or\": [ { \"quantity\": { \"$gt\": 5 } }, { \"price\": { \"$lt\": 10 } } ] }", 10);
            Map<?, ByteBuffer> firstOrResults = firstOrExecutor.execute(tr);
            System.out.println("OR(quantity>5, price<10) count: " + firstOrResults.size());

            // Test 7: Second part of complex query only
            System.out.println("\n=== Test 7: Second OR Part ===");
            PlanExecutor secondOrExecutor = createPlanExecutorForQuery(metadata,
                    "{ \"$or\": [ { \"category\": \"book\" }, { \"$and\": [ { \"category\": \"electronics\" }, { \"price\": { \"$gt\": 100 } } ] } ] }", 10);
            Map<?, ByteBuffer> secondOrResults = secondOrExecutor.execute(tr);
            System.out.println("OR(category=book, AND(category=electronics, price>100)) count: " + secondOrResults.size());

            // Test 8: Full complex query
            System.out.println("\n=== Test 8: Full Complex Query ===");
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
            System.out.println("Complex nested AND/OR count: " + complexResults.size());

            // Print detailed analysis
            System.out.println("\n=== Analysis ===");
            System.out.println("Expected for complex query: 2 (Laptop and Book A)");
            System.out.println("- First OR (quantity>5 OR price<10): should match Laptop, Book A, Pen, Notebook = 4");
            System.out.println("- Second OR (category=book OR (category=electronics AND price>100)): should match Laptop, Book A, Book B = 3");
            System.out.println("- AND of both: should match Laptop, Book A = 2");

            /*
            EXPECTED RESULT:
                [
                    { "name": "Laptop", "quantity": 10, "price": 1200, "category": "electronics" },
                    { "name": "Book A", "quantity": 7, "price": 12, "category": "book" }
                ]
            */
        }
    }
}