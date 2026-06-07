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
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AdaptiveUnionBudgetTest extends BasePipelineTest {

    private List<byte[]> createDocumentsForUnionTest(int totalCount, int matchingCount) {
        // role cycles between 'admin' and 'user' so $in creates a 2-child UnionNode.
        // Only matchingCount documents have category='rare', spread evenly.
        String[] roles = {"admin", "user"};
        Set<Integer> rareIndices = new HashSet<>();
        if (matchingCount > 0) {
            int step = totalCount / matchingCount;
            for (int i = 0; i < matchingCount; i++) {
                rareIndices.add(i * step);
            }
        }

        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < totalCount; i++) {
            String role = roles[i % roles.length];
            String category = rareIndices.contains(i) ? "rare" : "common";
            String json = String.format("{'age': %d, 'role': '%s', 'category': '%s'}", i, role, category);
            documents.add(BSONUtil.jsonToDocumentThenBytes(json));
        }
        return documents;
    }

    @Test
    void shouldFindAllMatchesWithLowSelectivityUnionAndLimit() {
        // Behavior: With 5000 documents and only 10 matching the residual predicate (spread evenly),
        // the adaptive budget must grow to scan past gaps in a union query with $in + $and.
        final String TEST_BUCKET_NAME = "test-adaptive-union-low-selectivity";

        SingleFieldIndexDefinition roleIndex = SingleFieldIndexDefinition.create(
                "role-index", "role", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = createDocumentsForUnionTest(5000, 10);
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$and': [{'role': {'$in': ['admin', 'user']}}, {'category': 'rare'}]}");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

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
    void shouldFindAllMatchesWithLowSelectivityUnionReverse() {
        // Behavior: Same low-selectivity union test in reverse sort order.
        final String TEST_BUCKET_NAME = "test-adaptive-union-low-selectivity-reverse";

        SingleFieldIndexDefinition roleIndex = SingleFieldIndexDefinition.create(
                "role-index", "role", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = createDocumentsForUnionTest(5000, 10);
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$and': [{'role': {'$in': ['admin', 'user']}}, {'category': 'rare'}]}");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

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
    }

    @Test
    void shouldHandleLimitLargerThanMatchingDocuments() {
        // Behavior: When limit is larger than total matching documents, should return all
        // matches even if budget must grow multiple times to exhaust the scan.
        final String TEST_BUCKET_NAME = "test-adaptive-union-limit-larger";

        SingleFieldIndexDefinition roleIndex = SingleFieldIndexDefinition.create(
                "role-index", "role", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = createDocumentsForUnionTest(5000, 10);
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$and': [{'role': {'$in': ['admin', 'user']}}, {'category': 'rare'}]}");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

        QueryOptions config = QueryOptions.builder().limit(100).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(10, results.size(), "Should find all 10 matching documents");

            Set<Integer> ages = extractIntegerFieldFromResults(results, "age");
            for (int age : ages) {
                assertEquals(0, age % 500, "Age should be a multiple of 500 for rare category, got: " + age);
            }
        }
    }
}
