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
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.handlers.protocol.SortDirection;
import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import org.bson.BsonType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Integration tests for RESULTSORT functionality — in-memory per-batch sorting of query results.
 * Unlike SORTBY (which uses index-backed ordering), RESULTSORT sorts the fetched result set
 * in memory by an arbitrary field, regardless of whether that field is indexed.
 */
class ResultSortIntegrationTest extends BasePipelineTest {

    // ==================== RESULTSORT with Non-Indexed Field Tests ====================

    @Test
    void shouldResultSortByNonIndexedFieldAscending() {
        // Behavior: RESULTSORT on a non-indexed field sorts fetched results in-memory by that field in ASC order.
        final String BUCKET = "test-resultsort-nonindexed-asc";

        // Only create the index on 'status', not on 'score'
        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-idx", "status", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, statusIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'score': 75}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'score': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'score': 100}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        // Query uses status index, but RESULTSORT is on the non-indexed 'score' field
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': 'active'}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("score")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"status\": \"active\", \"score\": 50}",
                "{\"status\": \"active\", \"score\": 75}",
                "{\"status\": \"active\", \"score\": 100}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by score ASC (in-memory)");
    }

    @Test
    void shouldResultSortByNonIndexedFieldDescending() {
        // Behavior: RESULTSORT on a non-indexed field sorts fetched results in-memory by that field in DESC order.
        final String BUCKET = "test-resultsort-nonindexed-desc";

        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-idx", "status", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, statusIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'score': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'score': 100}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'score': 75}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': 'active'}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("score")
                .resultSortDirection(SortDirection.DESC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"status\": \"active\", \"score\": 100}",
                "{\"status\": \"active\", \"score\": 75}",
                "{\"status\": \"active\", \"score\": 50}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by score DESC (in-memory)");
    }

    // ==================== RESULTSORT with Different Data Types ====================

    @Test
    void shouldResultSortByStringFieldAscending() {
        // Behavior: RESULTSORT on a string field sorts results lexicographically in ASC order.
        final String BUCKET = "test-resultsort-string-asc";

        SingleFieldIndexDefinition categoryIndex = SingleFieldIndexDefinition.create("cat-idx", "category", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, categoryIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'category': 'books', 'name': 'Zebra'}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'books', 'name': 'Apple'}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'books', 'name': 'Mango'}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'category': 'books'}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("name")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"category\": \"books\", \"name\": \"Apple\"}",
                "{\"category\": \"books\", \"name\": \"Mango\"}",
                "{\"category\": \"books\", \"name\": \"Zebra\"}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by name ASC");
    }

    @Test
    void shouldResultSortByDoubleFieldDescending() {
        // Behavior: RESULTSORT on a double field sorts results numerically in DESC order.
        final String BUCKET = "test-resultsort-double-desc";

        SingleFieldIndexDefinition typeIndex = SingleFieldIndexDefinition.create("type-idx", "type", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, typeIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'type': 'product', 'price': 19.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'type': 'product', 'price': 99.50}"),
                BSONUtil.jsonToDocumentThenBytes("{'type': 'product', 'price': 5.25}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'type': 'product'}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("price")
                .resultSortDirection(SortDirection.DESC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"type\": \"product\", \"price\": 99.5}",
                "{\"type\": \"product\", \"price\": 19.99}",
                "{\"type\": \"product\", \"price\": 5.25}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by price DESC");
    }

    // ==================== RESULTSORT with Null Values ====================

    @Test
    void shouldResultSortWithNullValuesFirst() {
        // Behavior: RESULTSORT ASC places null and missing values first, then sorted non-null values.
        final String BUCKET = "test-resultsort-nulls-first";

        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-idx", "status", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, statusIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'priority': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'priority': null}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'priority': 1}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': 'active'}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("priority")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        // Nulls first (explicit null and missing field), then sorted values
        List<String> expectedResult = List.of(
                "{\"status\": \"active\", \"priority\": null}",
                "{\"status\": \"active\"}",
                "{\"status\": \"active\", \"priority\": 1}",
                "{\"status\": \"active\", \"priority\": 2}"
        );
        assertEquals(expectedResult, actualResult, "Null values should sort first");
    }

    @Test
    void shouldResultSortWithNullValuesLastWhenDescending() {
        // Behavior: RESULTSORT DESC places non-null values first (descending), then null and missing values last.
        final String BUCKET = "test-resultsort-nulls-last-desc";

        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-idx", "status", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, statusIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'priority': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'priority': null}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'priority': 1}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': 'active'}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("priority")
                .resultSortDirection(SortDirection.DESC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        // DESC: the values sorted descending (2, 1), then nulls last (relative order of null-equivalent docs follows insertion order)
        List<String> expectedResult = List.of(
                "{\"status\": \"active\", \"priority\": 2}",
                "{\"status\": \"active\", \"priority\": 1}",
                "{\"status\": \"active\", \"priority\": null}",
                "{\"status\": \"active\"}"
        );
        assertEquals(expectedResult, actualResult, "Null values should sort last when DESC");
    }

    // ==================== RESULTSORT with Mixed Types (Type Bracketing) ====================

    @Test
    void shouldResultSortByTypeBracketingOrderWithMixedTypes() {
        // Behavior: RESULTSORT with mixed BSON types sorts by type bracketing order (Null < Numeric < String < Boolean).
        final String BUCKET = "test-resultsort-mixed-types";

        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-idx", "status", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, statusIndex);

        // Documents with different types for the 'value' field
        // Type order: Null(0) < Numeric(1) < String(2) < Boolean(7)
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': 'hello'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': 42}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': null}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': 3.14}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': {\"$numberLong\": \"100\"}}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': 'active'}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("value")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        // Expected order: NULL, then numerics by value (3.14, 42, 100), STRING, BOOLEAN
        List<String> expectedResult = List.of(
                "{\"status\": \"active\", \"value\": null}",
                "{\"status\": \"active\", \"value\": 3.14}",
                "{\"status\": \"active\", \"value\": 42}",
                "{\"status\": \"active\", \"value\": 100}",
                "{\"status\": \"active\", \"value\": \"hello\"}",
                "{\"status\": \"active\", \"value\": true}"
        );
        assertEquals(expectedResult, actualResult, "Types should be sorted according to type bracketing order");
    }

    @Test
    void shouldResultSortMixedTypesDescending() {
        // Behavior: RESULTSORT DESC with mixed BSON types sorts in reverse type bracketing order (numerics sorted by value descending).
        final String BUCKET = "test-resultsort-mixed-types-desc";

        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-idx", "status", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, statusIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': 'hello'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': 42}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': true}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': null}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': 3.14}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': 'active'}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("value")
                .resultSortDirection(SortDirection.DESC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        // DESC order: BOOLEAN, STRING, then numerics by value descending (42, 3.14), NULL
        List<String> expectedResult = List.of(
                "{\"status\": \"active\", \"value\": true}",
                "{\"status\": \"active\", \"value\": \"hello\"}",
                "{\"status\": \"active\", \"value\": 42}",
                "{\"status\": \"active\", \"value\": 3.14}",
                "{\"status\": \"active\", \"value\": null}"
        );
        assertEquals(expectedResult, actualResult, "Types should be sorted in reverse type bracketing order");
    }

    @Test
    void shouldResultSortSameTypeByValueWithMixedTypes() {
        // Behavior: RESULTSORT with mixed types sorts by type bracket first, then by value within the same type.
        final String BUCKET = "test-resultsort-mixed-same-type-values";

        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-idx", "status", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, statusIndex);

        // Multiple Int32 values mixed with other types
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': 'zebra'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': 'apple'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': null}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'value': 20}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': 'active'}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("value")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        // Expected: null, 10, 20, 30 (Int32s sorted by value), then "apple", "zebra" (Strings sorted)
        List<String> expectedResult = List.of(
                "{\"status\": \"active\", \"value\": null}",
                "{\"status\": \"active\", \"value\": 10}",
                "{\"status\": \"active\", \"value\": 20}",
                "{\"status\": \"active\", \"value\": 30}",
                "{\"status\": \"active\", \"value\": \"apple\"}",
                "{\"status\": \"active\", \"value\": \"zebra\"}"
        );
        assertEquals(expectedResult, actualResult, "Values should be sorted by type first, then by value within same type");
    }

    // ==================== RESULTSORT with $or and Indexes ====================

    @Test
    void shouldResultSortOrQueryResults() {
        // Behavior: $or query with multiple indexed fields returns results sorted by RESULTSORT field in-memory.
        final String BUCKET = "test-resultsort-or-indexes";

        SingleFieldIndexDefinition priceIndex = SingleFieldIndexDefinition.create("price-idx", "price", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition quantityIndex = SingleFieldIndexDefinition.create("quantity-idx", "quantity", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, priceIndex, quantityIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'A', 'price': 100, 'quantity': 5}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B', 'price': 50, 'quantity': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C', 'price': 200, 'quantity': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'D', 'price': 30, 'quantity': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'E', 'price': 150, 'quantity': 8}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        // $or: price > 100 OR quantity < 10 → matches A(100,5), C(200,3), E(150,8)
        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$or': [{'price': {'$gt': 100}}, {'quantity': {'$lt': 10}}]}");

        // Verify the plan is a UnionNode with IndexScanNode children
        assertInstanceOf(UnionNode.class, planWithParams.plan(), "Root should be UnionNode for $or query");
        UnionNode unionNode = (UnionNode) planWithParams.plan();
        assertEquals(2, unionNode.children().size(), "UnionNode should have 2 children for 2 $or branches");
        for (PipelineNode child : unionNode.children()) {
            assertInstanceOf(IndexScanNode.class, child, "Each $or branch should use IndexScanNode");
        }

        QueryOptions options = QueryOptions.builder()
                .resultSortField("price")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"A\", \"price\": 100, \"quantity\": 5}",
                "{\"name\": \"E\", \"price\": 150, \"quantity\": 8}",
                "{\"name\": \"C\", \"price\": 200, \"quantity\": 3}"
        );
        assertEquals(expectedResult, actualResult, "$or results should be sorted by price ASC");
    }

    // ==================== RESULTSORT with Full Scan ====================

    @Test
    void shouldResultSortByFieldWithFullScan() {
        // Behavior: RESULTSORT works with full scan (no indexes) by sorting fetched results in-memory.
        final String BUCKET = "test-resultsort-fullscan";

        // No indexes - will use full scan
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'rank': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'rank': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'rank': 2}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        // Empty query triggers full scan
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("rank")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"Alice\", \"rank\": 1}",
                "{\"name\": \"Bob\", \"rank\": 2}",
                "{\"name\": \"Charlie\", \"rank\": 3}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by rank ASC");
    }

    // ==================== RESULTSORT with Missing Field Semantics ====================

    @Test
    void shouldReturnNaturalOrderWhenResultSortFieldMissingFromAllDocuments() {
        // Behavior: When the RESULTSORT field does not exist in any document, ASC and DESC produce
        // identical results in natural (insertion) order because all values are null-equivalent.
        final String BUCKET = "test-resultsort-missing-all";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{}");

        // ASC
        QueryOptions ascOptions = QueryOptions.builder()
                .resultSortField("nonexistent")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ascCtx = new QueryContext(getSession(), metadata, ascOptions, planWithParams.plan(), planWithParams.parameters());

        List<String> ascResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ascCtx);
            for (ByteBuffer buffer : results) {
                ascResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        // DESC
        PlanWithParams descPlanWithParams = createPlanWithParams(metadata, "{}");
        QueryOptions descOptions = QueryOptions.builder()
                .resultSortField("nonexistent")
                .resultSortDirection(SortDirection.DESC)
                .build();
        QueryContext descCtx = new QueryContext(getSession(), metadata, descOptions, descPlanWithParams.plan(), descPlanWithParams.parameters());

        List<String> descResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, descCtx);
            for (ByteBuffer buffer : results) {
                descResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(ascResult, descResult, "ASC and DESC should produce the same order when RESULTSORT field is missing from all docs");
    }

    @Test
    void shouldResultSortPartiallyMissingFieldAscending() {
        // Behavior: Documents missing the RESULTSORT field are treated as null and appear first in ASC order.
        final String BUCKET = "test-resultsort-partial-asc";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'A', 'a': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C', 'a': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'D'}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("a")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"B\"}",
                "{\"name\": \"D\"}",
                "{\"name\": \"C\", \"a\": 1}",
                "{\"name\": \"A\", \"a\": 2}"
        );
        assertEquals(expectedResult, actualResult, "Missing fields should sort first (null-equivalent) in ASC");
    }

    @Test
    void shouldResultSortPartiallyMissingFieldDescending() {
        // Behavior: Documents missing the RESULTSORT field are treated as null and appear last in DESC order.
        final String BUCKET = "test-resultsort-partial-desc";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'A', 'a': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C', 'a': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'D'}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("a")
                .resultSortDirection(SortDirection.DESC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"A\", \"a\": 2}",
                "{\"name\": \"C\", \"a\": 1}",
                "{\"name\": \"B\"}",
                "{\"name\": \"D\"}"
        );
        assertEquals(expectedResult, actualResult, "Missing fields should sort last (null-equivalent) in DESC");
    }

    @Test
    void shouldPreserveInsertionOrderForEqualResultSortValues() {
        // Behavior: Documents with equal RESULTSORT values preserve their natural (insertion) order.
        final String BUCKET = "test-resultsort-equal-values";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'x': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'x': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'x': 3}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("a")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"a\": 1, \"x\": 1}",
                "{\"a\": 1, \"x\": 2}",
                "{\"a\": 1, \"x\": 3}"
        );
        assertEquals(expectedResult, actualResult, "Equal RESULTSORT values should preserve insertion order");
    }

    @Test
    void shouldTreatExplicitNullAndMissingFieldEquallyInResultSort() {
        // Behavior: Explicit null and missing field are both null-equivalent, appearing before
        // non-null values in RESULTSORT ASC order while preserving their relative insertion order.
        final String BUCKET = "test-resultsort-null-vs-missing";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'A', 'a': null}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C', 'a': 1}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("a")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"A\", \"a\": null}",
                "{\"name\": \"B\"}",
                "{\"name\": \"C\", \"a\": 1}"
        );
        assertEquals(expectedResult, actualResult, "Explicit null and missing field should be treated equally");
    }

    // ==================== RESULTSORT with Deeply Nested Field ====================

    @Test
    void shouldResultSortByDeeplyNestedField() {
        // Behavior: RESULTSORT on a deeply nested dot-notation field sorts results by the nested value.
        final String BUCKET = "test-resultsort-nested";

        // Index on the top-level field, but RESULTSORT on the nested field
        SingleFieldIndexDefinition typeIndex = SingleFieldIndexDefinition.create("type-idx", "type", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, typeIndex);

        // Documents with nested structure: user.profile.score
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'type': 'player', 'user': {'profile': {'score': 75, 'level': 5}}}"),
                BSONUtil.jsonToDocumentThenBytes("{'type': 'player', 'user': {'profile': {'score': 100, 'level': 10}}}"),
                BSONUtil.jsonToDocumentThenBytes("{'type': 'player', 'user': {'profile': {'score': 50, 'level': 3}}}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        // Query uses type index, RESULTSORT on deeply nested user.profile.score
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'type': 'player'}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("user.profile.score")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"type\": \"player\", \"user\": {\"profile\": {\"score\": 50, \"level\": 3}}}",
                "{\"type\": \"player\", \"user\": {\"profile\": {\"score\": 75, \"level\": 5}}}",
                "{\"type\": \"player\", \"user\": {\"profile\": {\"score\": 100, \"level\": 10}}}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by user.profile.score ASC");
    }

    @Test
    void shouldResultSortByFieldInNestedArraysOfDocuments() {
        // Behavior: RESULTSORT on a field within nested arrays of documents sorts by the nested array element value.
        final String BUCKET = "test-resultsort-nested-arrays";

        SingleFieldIndexDefinition typeIndex = SingleFieldIndexDefinition.create("type-idx", "type", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, typeIndex);

        // Document structure:
        // {
        //   "type": "store",
        //   "departments": [
        //     {
        //       "name": "Electronics",
        //       "products": [
        //         {"sku": "TV1", "price": 500},
        //         {"sku": "TV2", "price": 800}
        //       ]
        //     }
        //   ]
        // }
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes(
                        "{'type': 'store', 'departments': [{'name': 'Electronics', 'products': [{'sku': 'TV1', 'price': 500}]}]}"),
                BSONUtil.jsonToDocumentThenBytes(
                        "{'type': 'store', 'departments': [{'name': 'Books', 'products': [{'sku': 'B1', 'price': 20}]}]}"),
                BSONUtil.jsonToDocumentThenBytes(
                        "{'type': 'store', 'departments': [{'name': 'Toys', 'products': [{'sku': 'T1', 'price': 150}]}]}"),
                BSONUtil.jsonToDocumentThenBytes(
                        "{'type': 'gifts', 'departments': [{'name': 'Toys', 'products': [{'sku': 'T1', 'price': 50}]}]}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        // RESULTSORT on field within nested arrays: departments.products.price
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'type': 'store'}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("departments.products.price")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"type\": \"store\", \"departments\": [{\"name\": \"Books\", \"products\": [{\"sku\": \"B1\", \"price\": 20}]}]}",
                "{\"type\": \"store\", \"departments\": [{\"name\": \"Toys\", \"products\": [{\"sku\": \"T1\", \"price\": 150}]}]}",
                "{\"type\": \"store\", \"departments\": [{\"name\": \"Electronics\", \"products\": [{\"sku\": \"TV1\", \"price\": 500}]}]}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by departments.products.price ASC");
    }

    // ==================== RESULTSORT with Compound Indexes ====================

    @Test
    void shouldSortByNonIndexedFieldWithCompoundIndex() {
        // Behavior: When RESULTSORT targets a field not in the compound index, the compound index is
        // used for filtering and results are sorted in-memory by the non-indexed field.
        final String BUCKET = "test-resultsort-compound-nonindexed";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'score': 75}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'score': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'score': 100}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'name': 'Alice', 'age': 25}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("score")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"name\": \"Alice\", \"age\": 25, \"score\": 50}",
                "{\"name\": \"Alice\", \"age\": 25, \"score\": 75}",
                "{\"name\": \"Alice\", \"age\": 25, \"score\": 100}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by non-indexed score field ASC");
    }

    // ==================== RESULTSORT with $or and LIMIT ====================

    @Test
    @Disabled("RESULTSORT top-K across OR branches not yet implemented")
    void shouldReturnCorrectTopKWithOrAndResultSortAndLimit() {
        // Behavior: $or query with RESULTSORT on a non-indexed field and LIMIT should return the
        // true top-K documents across all matching results, not a biased subset from each branch.
        final String BUCKET = "test-or-resultsort-limit-topk";

        SingleFieldIndexDefinition roleIdx = SingleFieldIndexDefinition.create(
                "idx_role", "role", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition statusIdx = SingleFieldIndexDefinition.create(
                "idx_status", "status", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET,
                new SingleFieldIndexDefinition[]{roleIdx, statusIdx}, new CompoundIndexDefinition[]{});

        // Admin group: low salaries (10-40), inserted first so they appear first in the index scan order.
        // Active group: high salaries (100-400), inserted second.
        // True top-4 by salary DESC should be [400, 300, 200, 100] — all from the active group.
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'salary': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'salary': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'salary': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'admin', 'status': 'inactive', 'salary': 40}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'engineer', 'status': 'active', 'salary': 100}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'engineer', 'status': 'active', 'salary': 200}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'engineer', 'status': 'active', 'salary': 300}"),
                BSONUtil.jsonToDocumentThenBytes("{'role': 'engineer', 'status': 'active', 'salary': 400}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'$or': [{'role': 'admin'}, {'status': 'active'}]}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("salary")
                .resultSortDirection(SortDirection.DESC)
                .limit(4)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"role\": \"engineer\", \"status\": \"active\", \"salary\": 400}",
                "{\"role\": \"engineer\", \"status\": \"active\", \"salary\": 300}",
                "{\"role\": \"engineer\", \"status\": \"active\", \"salary\": 200}",
                "{\"role\": \"engineer\", \"status\": \"active\", \"salary\": 100}"
        );
        assertEquals(expectedResult, actualResult, "Should return true top-4 by salary DESC across all OR branches");
    }

    // ==================== RESULTSORT with Collation Tests ====================

    @Test
    void shouldResultSortRespectBucketCollationCaseInsensitiveASC() {
        // Behavior: RESULTSORT with bucket-level en/PRIMARY collation must sort strings
        // case-insensitively. Binary comparison places uppercase before lowercase (B<C<a),
        // producing wrong order. Collation-aware sort treats 'A'='a', giving apple<Banana<Cherry.
        final String BUCKET = "test-resultsort-bucket-collation-asc";

        BucketMetadata metadata;
        createBucketWithCollation(BUCKET, "{\"locale\": \"en\", \"strength\": 1}");
        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-idx", "status", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, BUCKET, statusIndex);
        metadata = getBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'name': 'Banana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'name': 'apple'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'name': 'Cherry'}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': {'$eq': 'active'}}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("name")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"status\": \"active\", \"name\": \"apple\"}",
                "{\"status\": \"active\", \"name\": \"Banana\"}",
                "{\"status\": \"active\", \"name\": \"Cherry\"}"
        );
        assertEquals(expectedResult, actualResult, "RESULTSORT must use bucket collation for case-insensitive ASC ordering");
    }

    @Test
    void shouldResultSortRespectBucketCollationCaseInsensitiveDESC() {
        // Behavior: RESULTSORT DESC with bucket-level en/PRIMARY collation must reverse the
        // collation-aware order: Cherry > Banana > apple. Binary comparison would give apple > Cherry > Banana.
        final String BUCKET = "test-resultsort-bucket-collation-desc";

        BucketMetadata metadata;
        createBucketWithCollation(BUCKET, "{\"locale\": \"en\", \"strength\": 1}");
        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-idx", "status", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(TEST_NAMESPACE, BUCKET, statusIndex);
        metadata = getBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'name': 'Banana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'name': 'apple'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'name': 'Cherry'}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': {'$eq': 'active'}}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("name")
                .resultSortDirection(SortDirection.DESC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"status\": \"active\", \"name\": \"Cherry\"}",
                "{\"status\": \"active\", \"name\": \"Banana\"}",
                "{\"status\": \"active\", \"name\": \"apple\"}"
        );
        assertEquals(expectedResult, actualResult, "RESULTSORT must use bucket collation for case-insensitive DESC ordering");
    }

    @Test
    void shouldResultSortRespectQueryCollationCaseInsensitiveASC() {
        // Behavior: RESULTSORT with query-level en/PRIMARY collation (no bucket collation)
        // must sort case-insensitively. QueryOptions.collation must be applied by resultSortIfNeeded.
        final String BUCKET = "test-resultsort-query-collation-asc";

        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-idx", "status", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, statusIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'name': 'Banana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'name': 'apple'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'name': 'Cherry'}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        Collation enPrimary = Collation.create("en", 1, null, null, null, null, null, null, null);
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': {'$eq': 'active'}}", null, enPrimary);
        QueryOptions options = QueryOptions.builder()
                .resultSortField("name")
                .resultSortDirection(SortDirection.ASC)
                .collation(enPrimary)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"status\": \"active\", \"name\": \"apple\"}",
                "{\"status\": \"active\", \"name\": \"Banana\"}",
                "{\"status\": \"active\", \"name\": \"Cherry\"}"
        );
        assertEquals(expectedResult, actualResult, "RESULTSORT must use query-level collation for case-insensitive ASC ordering");
    }

    @Test
    void shouldResultSortRespectIndexCollationCaseInsensitiveASC() {
        // Behavior: RESULTSORT with no query- or bucket-level collation must fall back to the
        // single-field index collation of the RESULTSORT field. A 'name' index with en/PRIMARY
        // collation must make RESULTSORT case-insensitive. Binary comparison would give Banana <
        // Cherry < apple (wrong). Index-collation-aware sort gives apple < Banana < Cherry.
        final String BUCKET = "test-resultsort-index-collation-asc";

        Collation enPrimary = Collation.create("en", 1, null, null, null, null, null, null, null);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create(
                "name-idx", "name", BsonType.STRING, false, IndexStatus.WAITING, enPrimary);
        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create(
                "status-idx", "status", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, nameIndex, statusIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'name': 'Banana'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'name': 'apple'}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'name': 'Cherry'}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'status': {'$eq': 'active'}}");
        QueryOptions options = QueryOptions.builder()
                .resultSortField("name")
                .resultSortDirection(SortDirection.ASC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        List<String> expectedResult = List.of(
                "{\"status\": \"active\", \"name\": \"apple\"}",
                "{\"status\": \"active\", \"name\": \"Banana\"}",
                "{\"status\": \"active\", \"name\": \"Cherry\"}"
        );
        assertEquals(expectedResult, actualResult,
                "RESULTSORT must fall back to the RESULTSORT field's index collation when no query/bucket collation is set");
    }
}
