/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ArrayFiltersIntegrationTest extends BasePipelineTest {

    private static final String ARRAY_BUCKET = "array-filter-test-bucket";
    private static final String INDEXED_ARRAY_BUCKET = "indexed-array-filter-test-bucket";

    /**
     * Test data:
     * - doc1: name=Alice, grades=[85, 82, 80]
     * - doc2: name=Bob, grades=[88, 90, 92]
     * - doc3: name=Charlie, grades=[85, 100, 90]
     */
    private BucketMetadata insertTestDocuments() {
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(ARRAY_BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'grades': [85, 82, 80]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'grades': [88, 90, 92]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'grades': [85, 100, 90]}")
        );

        insertDocumentsAndGetObjectIds(ARRAY_BUCKET, documents);
        return metadata;
    }

    @Test
    void shouldMatchArrayWithEqWhenAnyElementEquals() {
        // Behavior: EQ on array field matches documents where ANY element equals the value.
        // grades=[85, 100, 90] has element 100, so Charlie matches.

        BucketMetadata metadata = insertTestDocuments();

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'grades': {'$eq': 100}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size());
            assertEquals(Set.of("Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchArrayWithNeWhenNoElementEquals() {
        // Behavior: NE on array field matches documents where NO element equals the value.
        // Only Alice and Bob have no element equal to 100.

        BucketMetadata metadata = insertTestDocuments();

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'grades': {'$ne': 100}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size());
            assertEquals(Set.of("Alice", "Bob"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchArrayWithGtWhenAnyElementGreater() {
        // Behavior: GT on the array field matches documents where ANY element is greater than the value.
        // Bob has [88, 90, 92] with elements > 85. Charlie has [85, 100, 90] with elements > 85.

        BucketMetadata metadata = insertTestDocuments();

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'grades': {'$gt': 85}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size());
            assertEquals(Set.of("Bob", "Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchArrayWithGteWhenAnyElementGreaterOrEqual() {
        // Behavior: GTE on the array field matches documents where ANY element is >= the value.
        // All docs have at least one element >= 85.

        BucketMetadata metadata = insertTestDocuments();

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'grades': {'$gte': 85}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(3, results.size());
            assertEquals(Set.of("Alice", "Bob", "Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchArrayWithLtWhenAnyElementLess() {
        // Behavior: LT on array field matches documents where ANY element is less than the value.
        // Alice has [85, 82, 80] with elements 82 and 80 < 85.

        BucketMetadata metadata = insertTestDocuments();

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'grades': {'$lt': 85}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size());
            assertEquals(Set.of("Alice"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchArrayWithLteWhenAnyElementLessOrEqual() {
        // Behavior: LTE on the array field matches documents where ANY element is <= the value.
        // Alice has [85, 82, 80], and Charlie has [85, 100, 90] - both have element 85.

        BucketMetadata metadata = insertTestDocuments();

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'grades': {'$lte': 85}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size());
            assertEquals(Set.of("Alice", "Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchArrayWithMultipleComparisonOperators() {
        // Behavior: Multiple comparison operators on array field are combined with AND.
        // grades >= 80 AND grades <= 85 matches docs with any element in [80, 85] range.

        BucketMetadata metadata = insertTestDocuments();

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'grades': {'$gte': 80, '$lte': 85}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            // Alice has [85, 82, 80] - all elements satisfy both conditions
            // Charlie has [85, 100, 90] - element 85 satisfies both conditions
            assertEquals(2, results.size());
            assertEquals(Set.of("Alice", "Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldNotMatchEmptyArrayWithComparisonOperators() {
        // Behavior: Empty arrays never match EQ/GT/GTE/LT/LTE (no elements to satisfy).
        // NE matches because no element equals the value.

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(ARRAY_BUCKET + "-empty");

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Empty', 'grades': []}")
        );
        insertDocumentsAndGetObjectIds(ARRAY_BUCKET + "-empty", documents);

        // EQ should not match
        PlanWithParams eqPlanWithParams = createPlanWithParams(metadata, "{'grades': {'$eq': 100}}");
        QueryContext eqCtx = new QueryContext(getSession(), metadata, QueryOptions.builder().build(), eqPlanWithParams.plan(), eqPlanWithParams.parameters());
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, eqCtx);
            assertEquals(0, results.size(), "EQ should not match empty array");
        }

        // NE should match (no element equals 100)
        PlanWithParams nePlanWithParams = createPlanWithParams(metadata, "{'grades': {'$ne': 100}}");
        QueryContext neCtx = new QueryContext(getSession(), metadata, QueryOptions.builder().build(), nePlanWithParams.plan(), nePlanWithParams.parameters());
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, neCtx);
            assertEquals(1, results.size(), "NE should match empty array");
            assertEquals(Set.of("Empty"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchArrayWithStringComparison() {
        // Behavior: Comparison operators work with string arrays using lexicographic ordering.

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(ARRAY_BUCKET + "-strings");

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc1', 'tags': ['apple', 'banana', 'cherry']}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc2', 'tags': ['date', 'elderberry']}")
        );
        insertDocumentsAndGetObjectIds(ARRAY_BUCKET + "-strings", documents);

        // EQ matches Doc1 (has 'banana')
        PlanWithParams eqPlanWithParams = createPlanWithParams(metadata, "{'tags': {'$eq': 'banana'}}");
        QueryContext eqCtx = new QueryContext(getSession(), metadata, QueryOptions.builder().build(), eqPlanWithParams.plan(), eqPlanWithParams.parameters());
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, eqCtx);
            assertEquals(1, results.size());
            assertEquals(Set.of("Doc1"), extractNamesFromResults(results));
        }

        // GT 'cherry' matches Doc2 (has 'date' and 'elderberry' > 'cherry')
        PlanWithParams gtPlanWithParams = createPlanWithParams(metadata, "{'tags': {'$gt': 'cherry'}}");
        QueryContext gtCtx = new QueryContext(getSession(), metadata, QueryOptions.builder().build(), gtPlanWithParams.plan(), gtPlanWithParams.parameters());
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, gtCtx);
            assertEquals(1, results.size());
            assertEquals(Set.of("Doc2"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchArrayWithDoubleComparison() {
        // Behavior: Comparison operators work with double arrays.

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(ARRAY_BUCKET + "-doubles");

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc1', 'scores': [3.5, 4.0, 3.8]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Doc2', 'scores': [2.5, 2.8, 3.0]}")
        );
        insertDocumentsAndGetObjectIds(ARRAY_BUCKET + "-doubles", documents);

        // GT 3.5 matches Doc1 (has 4.0 and 3.8 > 3.5)
        PlanWithParams gtPlanWithParams = createPlanWithParams(metadata, "{'scores': {'$gt': 3.5}}");
        QueryContext gtCtx = new QueryContext(getSession(), metadata, QueryOptions.builder().build(), gtPlanWithParams.plan(), gtPlanWithParams.parameters());
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, gtCtx);
            assertEquals(1, results.size());
            assertEquals(Set.of("Doc1"), extractNamesFromResults(results));
        }

        // LTE 3.0 matches Doc2 (all elements <= 3.0)
        PlanWithParams ltePlanWithParams = createPlanWithParams(metadata, "{'scores': {'$lte': 3.0}}");
        QueryContext lteCtx = new QueryContext(getSession(), metadata, QueryOptions.builder().build(), ltePlanWithParams.plan(), ltePlanWithParams.parameters());
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, lteCtx);
            assertEquals(1, results.size());
            assertEquals(Set.of("Doc2"), extractNamesFromResults(results));
        }
    }

    // ==================== INDEXED ARRAY TESTS ====================
    // These tests verify comparison operators on array fields with multikey indexes.
    // With multikey indexes, each array element has its own index entry, enabling
    // RangeScanNode execution path instead of FullScanNode with PredicateEvaluator.

    /**
     * Test data for indexed array tests:
     * - doc1: name=Alice, grades=[85, 82, 80]
     * - doc2: name=Bob, grades=[88, 90, 92]
     * - doc3: name=Charlie, grades=[85, 100, 90]
     */
    private BucketMetadata insertTestDocumentsWithIndex() {
        SingleFieldIndexDefinition gradesIndex = SingleFieldIndexDefinition.create("grades-idx", "grades", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(INDEXED_ARRAY_BUCKET, gradesIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'grades': [85, 82, 80]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'grades': [88, 90, 92]}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'grades': [85, 100, 90]}")
        );

        insertDocumentsAndGetObjectIds(INDEXED_ARRAY_BUCKET, documents);
        return metadata;
    }

    @Test
    void shouldMatchIndexedArrayWithEq() {
        // Behavior: EQ on the indexed array field uses RangeScanNode to find documents where
        // ANY element equals the value. grades=[85, 100, 90] has element 100, so Charlie matches.

        BucketMetadata metadata = insertTestDocumentsWithIndex();

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'grades': {'$eq': 100}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size());
            assertEquals(Set.of("Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchIndexedArrayWithNe() {
        // Behavior: NE on indexed array field matches documents where NO element equals the value.
        // This is identical to non-indexed NE behavior - indexes only accelerate queries without
        // changing semantics. Only Alice and Bob have no element equal to 100.

        BucketMetadata metadata = insertTestDocumentsWithIndex();

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'grades': {'$ne': 100}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size());
            assertEquals(Set.of("Alice", "Bob"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchIndexedArrayWithGt() {
        // Behavior: GT on the indexed array field uses RangeScanNode to find documents where
        // ANY element is greater than the value. Bob has [88, 90, 92], and Charlie has
        // [85, 100, 90] - both have elements > 85.

        BucketMetadata metadata = insertTestDocumentsWithIndex();

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'grades': {'$gt': 85}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size());
            assertEquals(Set.of("Bob", "Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchIndexedArrayWithGte() {
        // Behavior: GTE on the indexed array field uses RangeScanNode to find documents where
        // ANY element is >= the value. All docs have at least one element >= 85.

        BucketMetadata metadata = insertTestDocumentsWithIndex();

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'grades': {'$gte': 85}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(3, results.size());
            assertEquals(Set.of("Alice", "Bob", "Charlie"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchIndexedArrayWithLt() {
        // Behavior: LT on the indexed array field uses RangeScanNode to find documents where
        // ANY element is less than the value. Alice has [85, 82, 80] with elements 82 and 80 < 85.

        BucketMetadata metadata = insertTestDocumentsWithIndex();

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'grades': {'$lt': 85}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(1, results.size());
            assertEquals(Set.of("Alice"), extractNamesFromResults(results));
        }
    }

    @Test
    void shouldMatchIndexedArrayWithLte() {
        // Behavior: LTE on the indexed array field uses RangeScanNode to find documents where
        // ANY element is <= the value. Alice has [85, 82, 80], and Charlie has [85, 100, 90] -
        // both have element 85.

        BucketMetadata metadata = insertTestDocumentsWithIndex();

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'grades': {'$lte': 85}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);

            assertEquals(2, results.size());
            assertEquals(Set.of("Alice", "Charlie"), extractNamesFromResults(results));
        }
    }
}
