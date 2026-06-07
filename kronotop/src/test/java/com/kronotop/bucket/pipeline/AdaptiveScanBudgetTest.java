/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.kronotop.bucket.handlers.protocol.SortDirection;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AdaptiveScanBudgetTest extends BasePipelineTest {

    private List<byte[]> createDocumentsWithLowSelectivity(int totalCount, int matchingCount) {
        // Spread rare documents evenly across the range
        Set<Integer> rareIndices = new HashSet<>();
        if (matchingCount > 0) {
            int step = totalCount / matchingCount;
            for (int i = 0; i < matchingCount; i++) {
                rareIndices.add(i * step);
            }
        }

        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < totalCount; i++) {
            String category = rareIndices.contains(i) ? "rare" : "common";
            String json = String.format("{'age': %d, 'category': '%s'}", i, category);
            documents.add(BSONUtil.jsonToDocumentThenBytes(json));
        }
        return documents;
    }

    @Test
    void shouldFindAllMatchesWithLowSelectivityAndLimit() {
        // Behavior: With 5000 documents and only 10 matching (spread evenly), adaptive budget
        // must grow multiple times to find all matches across ADVANCE calls.
        final String TEST_BUCKET_NAME = "test-adaptive-scan-low-selectivity";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // 5000 docs, 10 rare → step=500, rare at 0, 500, 1000, ..., 4500
        // Budget sequence: 5 → 1000 → 1500 → 2250 → ... must grow past the first rare gaps
        List<byte[]> documents = createDocumentsWithLowSelectivity(5000, 10);
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'category': 'rare'}");
        QueryOptions config = QueryOptions.builder().limit(5).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<Integer> allAges = new ArrayList<>();
        int iterations = 0;

        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);
                iterations++;

                if (results.isEmpty()) {
                    break;
                }

                assertTrue(results.size() <= 5, "Each batch should return at most 5 documents");

                Set<Integer> batchAges = extractIntegerFieldFromResults(results, "age");
                allAges.addAll(batchAges);

                if (iterations > 20) {
                    fail("Too many iterations for 10 matching documents with limit 5");
                }
            }
        }

        assertEquals(10, allAges.size(), "Should find all 10 matching documents");

        Set<Integer> uniqueAges = new HashSet<>(allAges);
        assertEquals(10, uniqueAges.size(), "Should have no duplicate documents");

        // Rare at step=500: 0, 500, 1000, ..., 4500
        for (int age : allAges) {
            assertEquals(0, age % 500, "Age should be a multiple of 500 for rare category, got: " + age);
        }
    }

    @Test
    void shouldFindAllMatchesWithLowSelectivityReverse() {
        // Behavior: Same as above but in reverse sort order with 5000 documents.
        final String TEST_BUCKET_NAME = "test-adaptive-scan-low-selectivity-reverse";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        List<byte[]> documents = createDocumentsWithLowSelectivity(5000, 10);
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'category': 'rare'}");
        QueryOptions config = QueryOptions.builder().limit(5).sortDirection(SortDirection.DESC).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<Integer> allAges = new ArrayList<>();
        int iterations = 0;

        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);
                iterations++;

                if (results.isEmpty()) {
                    break;
                }

                assertTrue(results.size() <= 5, "Each batch should return at most 5 documents");

                Set<Integer> batchAges = extractIntegerFieldFromResults(results, "age");
                allAges.addAll(batchAges);

                if (iterations > 20) {
                    fail("Too many iterations");
                }
            }
        }

        assertEquals(10, allAges.size(), "Should find all 10 matching documents");
        Set<Integer> uniqueAges = new HashSet<>(allAges);
        assertEquals(10, uniqueAges.size(), "No duplicates");

        for (int age : allAges) {
            assertEquals(0, age % 500, "Age should be a multiple of 500 for rare category, got: " + age);
        }

        // Verify descending order
        for (int i = 1; i < allAges.size(); i++) {
            assertTrue(allAges.get(i - 1) >= allAges.get(i),
                    String.format("Ages should be in descending order: %d >= %d", allAges.get(i - 1), allAges.get(i)));
        }
    }

    @Test
    void shouldHandleLimitLargerThanMatchingDocuments() {
        // Behavior: When limit is larger than total matching documents, should return all
        // matches even if budget must grow multiple times to exhaust the scan.
        final String TEST_BUCKET_NAME = "test-adaptive-scan-limit-larger";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // 5000 docs, 8 rare → step=625, budget must grow well past 1000 to find them all
        List<byte[]> documents = createDocumentsWithLowSelectivity(5000, 8);
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'category': 'rare'}");
        QueryOptions config = QueryOptions.builder().limit(100).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(8, results.size(), "Should find all 8 matching documents");

            Set<Integer> ages = extractIntegerFieldFromResults(results, "age");
            // 5000/8 = 625, rare indices: 0, 625, 1250, 1875, 2500, 3125, 3750, 4375
            for (int age : ages) {
                assertEquals(0, age % 625, "Age should be a multiple of 625 for rare category, got: " + age);
            }
        }
    }

    @Test
    void shouldCursorRewindCorrectlyOnOverfetch() {
        // Behavior: When adaptive budget over-fetches, cursor rewind ensures the next ADVANCE
        // does not skip or duplicate entries. 3000 docs with 20 rare (step=150) forces the budget
        // to grow significantly, causing over-fetch and rewind on each ADVANCE.
        final String TEST_BUCKET_NAME = "test-adaptive-scan-cursor-rewind";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME);

        // 3000 docs, 20 rare → step=150, rare at 0, 150, 300, ..., 2850
        List<byte[]> documents = createDocumentsWithLowSelectivity(3000, 20);
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'category': 'rare'}");
        QueryOptions config = QueryOptions.builder().limit(3).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<Integer> allAges = new ArrayList<>();
        int iterations = 0;

        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);
                iterations++;

                if (results.isEmpty()) {
                    break;
                }

                assertTrue(results.size() <= 3, "Each batch should return at most 3 documents");

                Set<Integer> batchAges = extractIntegerFieldFromResults(results, "age");
                allAges.addAll(batchAges);

                if (iterations > 20) {
                    fail("Too many iterations for 20 matching documents with limit 3");
                }
            }
        }

        assertEquals(20, allAges.size(), "Should find all 20 matching documents");

        // Verify no duplicates (critical for cursor rewind correctness)
        Set<Integer> uniqueAges = new HashSet<>(allAges);
        assertEquals(20, uniqueAges.size(), "Cursor rewind should not cause duplicates");

        // Verify all expected ages present (3000/20 = step 150: 0, 150, 300, ..., 2850)
        for (int i = 0; i < 20; i++) {
            assertTrue(uniqueAges.contains(i * 150), "Missing age: " + (i * 150));
        }
    }
}
