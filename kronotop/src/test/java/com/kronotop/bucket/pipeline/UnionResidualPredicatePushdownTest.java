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
import com.kronotop.TestUtil;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.transaction.TransactionUtil;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the PipelineRewriter fix that pushes residual predicates into each
 * child of a UnionNode rather than placing them after the union.
 * <p>
 * The bug: when the primary node in convertNestedAndToIndexedPlan was a UnionNode,
 * residual predicates were chained after the union. The union's internal capacity cap
 * (ctx.options().limit()) filled the sink before the post-union transform could filter,
 * causing incomplete results when the residual had low selectivity.
 * <p>
 * The fix: push residual predicates into each union child so filtering happens before
 * the union collects, enabling the adaptive scan budget to grow correctly.
 */
class UnionResidualPredicatePushdownTest extends BasePipelineTest {

    @Test
    void shouldPushResidualPredicateIntoUnionChildren() {
        // Behavior: When $in on an indexed field is combined with $and and a non-indexed predicate,
        // the plan should be a UnionNode whose children each have a TransformWithResidualPredicateNode
        // chained — not a TransformWithResidualPredicateNode after the union itself.
        final String TEST_BUCKET_NAME = "test-pushdown-plan-structure";

        SingleFieldIndexDefinition roleIndex = SingleFieldIndexDefinition.create(
                "role-index", "role", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$and': [{'role': {'$in': ['admin', 'editor']}}, {'status': 'active'}]}");

        // Root must be UnionNode
        assertInstanceOf(UnionNode.class, planWithParams.plan());
        UnionNode union = (UnionNode) planWithParams.plan();

        // The union itself must NOT have a next node (residual is pushed into children)
        assertNull(union.next(), "UnionNode should not have a post-union transform");

        // Each child must have a TransformWithResidualPredicateNode chained
        for (PipelineNode child : union.children()) {
            assertInstanceOf(IndexScanNode.class, child, "Each union child should be an IndexScanNode");
            assertNotNull(child.next(), "Each union child should have a next node");
            assertInstanceOf(TransformWithResidualPredicateNode.class, child.next(),
                    "Each union child's next should be TransformWithResidualPredicateNode");
        }
    }

    @Test
    void shouldReturnCorrectResultsWithLimitAndLowSelectivityResidual() {
        // Behavior: With 50 docs matching the index but only 4 matching the residual predicate,
        // and limit=10, all 4 matching docs should be returned in a single batch.
        // Before the fix, the union cap at limit=10 would scan 10 index entries, filter most out,
        // and return fewer than 4 results.
        final String TEST_BUCKET_NAME = "test-pushdown-low-selectivity";

        SingleFieldIndexDefinition roleIndex = SingleFieldIndexDefinition.create(
                "role-index", "role", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = new ArrayList<>();
        // 25 admins and 25 editors, but only 2 of each have status='active'
        for (int i = 0; i < 25; i++) {
            String status = (i == 5 || i == 20) ? "active" : "inactive";
            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{'role': 'admin', 'status': '%s', 'seq': %d}", status, i)));
        }
        for (int i = 0; i < 25; i++) {
            String status = (i == 10 || i == 15) ? "active" : "inactive";
            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{'role': 'editor', 'status': '%s', 'seq': %d}", status, i + 25)));
        }
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$and': [{'role': {'$in': ['admin', 'editor']}}, {'status': 'active'}]}");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

        QueryOptions config = QueryOptions.builder().limit(10).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(4, actualResult.size(), "Should find all 4 active documents");

        // All returned docs must have status=active
        for (String json : actualResult) {
            assertTrue(json.contains("\"status\": \"active\""),
                    "Every result should have status=active, got: " + json);
        }
    }

    @Test
    void shouldPaginateCorrectlyWithResidualFilteredUnion() {
        // Behavior: With 200 docs where only 12 match the residual, and limit=5, pagination
        // via ADVANCE calls should return all 12 docs without duplicates or missing entries.
        final String TEST_BUCKET_NAME = "test-pushdown-pagination";

        SingleFieldIndexDefinition roleIndex = SingleFieldIndexDefinition.create(
                "role-index", "role", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        Set<Integer> targetSeqs = Set.of(10, 30, 50, 70, 90, 110, 130, 150, 170, 190, 5, 95);
        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            String role = (i % 2 == 0) ? "admin" : "editor";
            String category = targetSeqs.contains(i) ? "special" : "ordinary";
            documents.add(BSONUtil.jsonToDocumentThenBytes(
                    String.format("{'role': '%s', 'category': '%s', 'seq': %d}", role, category, i)));
        }
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$and': [{'role': {'$in': ['admin', 'editor']}}, {'category': 'special'}]}");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

        QueryOptions config = QueryOptions.builder().limit(5).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<Integer> allSeqs = new ArrayList<>();
        int iterations = 0;

        while (true) {
            try (Transaction tr = createTransaction()) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);
                iterations++;

                if (results.isEmpty()) {
                    break;
                }

                assertTrue(results.size() <= 5, "Each batch should return at most 5 documents");
                Set<Integer> batchSeqs = extractIntegerFieldFromResults(results, "seq");
                allSeqs.addAll(batchSeqs);

                if (iterations > 20) {
                    fail("Too many iterations for 12 matching documents with limit 5");
                }
            }
        }

        assertEquals(12, allSeqs.size(), "Should find all 12 special documents");

        Set<Integer> uniqueSeqs = new HashSet<>(allSeqs);
        assertEquals(12, uniqueSeqs.size(), "Should have no duplicate documents");

        for (int seq : targetSeqs) {
            assertTrue(uniqueSeqs.contains(seq), "Missing seq: " + seq);
        }
    }

    @Test
    void shouldDeleteViaUnionWithResidualPredicate() {
        // Behavior: Delete executor should correctly delete only documents matching both
        // the indexed $in predicate and the non-indexed residual predicate, using the
        // pushdown plan shape.
        final String TEST_BUCKET_NAME = "test-pushdown-delete";

        SingleFieldIndexDefinition roleIndex = SingleFieldIndexDefinition.create(
                "role-index", "role", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'active', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'active', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'inactive', 'name': 'Diana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'status': 'active', 'name': 'Eve'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$and': [{'role': {'$in': ['admin', 'editor']}}, {'status': 'active'}]}");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

        QueryOptions config = QueryOptions.builder().build();
        QueryContext deleteCtx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ObjectId> deletedIds = deleteExecutor.execute(tr, deleteCtx);
            assertEquals(2, deletedIds.size(), "Should delete exactly 2 active admin/editor documents");
            tr.commit().join();
            assertDoesNotThrow(() -> TransactionUtil.runPostCommitHooks(getSession()));
        }

        // Verify remaining documents
        List<String> remaining = runQueryOnBucket(metadata, "{}");
        assertEquals(3, remaining.size(), "Should have 3 remaining documents");

        // The deleted ones were Alice (admin+active) and Charlie (editor+active)
        for (String json : remaining) {
            assertFalse(json.contains("\"name\": \"Alice\""), "Alice should be deleted");
            assertFalse(json.contains("\"name\": \"Charlie\""), "Charlie should be deleted");
        }
    }

    @Test
    void shouldUpdateViaUnionWithResidualPredicate() {
        // Behavior: Update executor should correctly update only documents matching both
        // the indexed $in predicate and the non-indexed residual predicate.
        final String TEST_BUCKET_NAME = "test-pushdown-update";

        SingleFieldIndexDefinition roleIndex = SingleFieldIndexDefinition.create(
                "role-index", "role", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'active', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'active', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'inactive', 'name': 'Diana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'status': 'active', 'name': 'Eve'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$and': [{'role': {'$in': ['admin', 'editor']}}, {'status': 'active'}]}");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

        UpdateOptions update = UpdateOptions.builder().set("status", new BsonString("archived")).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext updateCtx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> updatedIds;
        try (Transaction tr = createTransaction()) {
            updatedIds = updateExecutor.execute(tr, updateCtx);
            assertEquals(2, updatedIds.size(), "Should update exactly 2 active admin/editor documents");
            tr.commit().join();
            assertDoesNotThrow(() -> TransactionUtil.runPostCommitHooks(getSession()));
        }

        // Verify updated documents
        for (ObjectId objectId : updatedIds) {
            String query = String.format("{'_id': {'$eq': '%s'}}", objectId.toHexString());
            List<String> result = runQueryOnBucket(metadata, query);
            assertEquals(1, result.size());
            assertTrue(result.getFirst().contains("\"status\": \"archived\""),
                    "Updated document should have status=archived, got: " + result.getFirst());
        }

        // Verify non-matching documents are unchanged
        List<String> inactiveResults = runQueryOnBucket(metadata,
                "{'status': 'inactive'}");
        assertEquals(2, inactiveResults.size(), "Inactive documents should be unchanged");

        List<String> eveResults = runQueryOnBucket(metadata, "{'name': 'Eve'}");
        assertEquals(1, eveResults.size());
        assertTrue(eveResults.getFirst().contains("\"status\": \"active\""),
                "Eve (user role) should be unchanged");
    }

    @Test
    void shouldHandleMultipleResidualPredicatesOnUnion() {
        // Behavior: When $in (indexed) is combined with multiple non-indexed predicates via $and,
        // all residual predicates should be pushed into each child and evaluated correctly.
        final String TEST_BUCKET_NAME = "test-pushdown-multiple-residuals";

        SingleFieldIndexDefinition roleIndex = SingleFieldIndexDefinition.create(
                "role-index", "role", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'active', 'level': 3, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'active', 'level': 1, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'level': 3, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'active', 'level': 3, 'name': 'Diana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'active', 'level': 1, 'name': 'Eve'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'status': 'active', 'level': 3, 'name': 'Frank'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        // Both status and level are non-indexed — two residual predicates
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$and': [{'role': {'$in': ['admin', 'editor']}}, {'status': 'active'}, {'level': 3}]}");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        // Only Alice (admin, active, level=3) and Diana (editor, active, level=3)
        assertEquals(2, actualResult.size(), "Should find exactly 2 documents matching all predicates");

        Collections.sort(actualResult);
        Set<String> names = extractNamesFromResults(
                new ArrayList<>() {{
                    try (Transaction tr = createTransaction()) {
                        QueryContext readCtx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());
                        addAll(readExecutor.execute(tr, readCtx));
                    }
                }}
        );
        assertTrue(names.contains("Alice"), "Should contain Alice");
        assertTrue(names.contains("Diana"), "Should contain Diana");
    }

    @Test
    void shouldReturnAllMatchesWhenAllPassResidual() {
        // Behavior: When every document matching the index also passes the residual predicate,
        // the pushdown should not alter correctness (no filtering happens, all entries pass through).
        final String TEST_BUCKET_NAME = "test-pushdown-all-pass";

        SingleFieldIndexDefinition roleIndex = SingleFieldIndexDefinition.create(
                "role-index", "role", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'active', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'active', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'user', 'status': 'inactive', 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$and': [{'role': {'$in': ['admin', 'editor']}}, {'status': 'active'}]}");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(2, actualResult.size(), "Both indexed matches pass the residual");
        Collections.sort(actualResult);
        assertTrue(actualResult.get(0).contains("\"name\": \"Alice\""));
        assertTrue(actualResult.get(1).contains("\"name\": \"Bob\""));
    }

    @Test
    void shouldReturnEmptyWhenNoDocumentsPassResidual() {
        // Behavior: When all index matches fail the residual predicate, the result should be
        // empty and all children should be exhausted.
        final String TEST_BUCKET_NAME = "test-pushdown-none-pass";

        SingleFieldIndexDefinition roleIndex = SingleFieldIndexDefinition.create(
                "role-index", "role", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, roleIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'inactive', 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'editor', 'status': 'inactive', 'name': 'Diana'}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET_NAME, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$and': [{'role': {'$in': ['admin', 'editor']}}, {'status': 'active'}]}");
        assertInstanceOf(UnionNode.class, planWithParams.plan());

        QueryOptions config = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, config, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertTrue(results.isEmpty(), "No documents should match when all fail the residual");
        }
    }
}
