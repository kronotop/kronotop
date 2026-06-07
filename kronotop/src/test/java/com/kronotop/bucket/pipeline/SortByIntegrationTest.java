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
import com.kronotop.bucket.handlers.protocol.SortDirection;
import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.planner.physical.PhysicalPlanValidationException;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for index-based SORTBY functionality.
 * SORTBY requires a supporting index — single field or compound.
 * For in-memory result sorting tests, see {@link ResultSortIntegrationTest}.
 */
class SortByIntegrationTest extends BasePipelineTest {

    // ==================== SORTBY with Single Field Index ====================

    @Test
    void shouldSortByIndexedFieldAscending() {
        final String BUCKET = "test-sortby-indexed-asc";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 20}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$gte': 0}}");
        QueryOptions options = QueryOptions.builder()
                .sortByField("age")
                .sortDirection(SortDirection.ASC)
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
                "{\"name\": \"Alice\", \"age\": 10}",
                "{\"name\": \"Bob\", \"age\": 20}",
                "{\"name\": \"Charlie\", \"age\": 30}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by age ASC");
    }

    @Test
    void shouldSortByIndexedFieldDescending() {
        final String BUCKET = "test-sortby-indexed-desc";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 30}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$gte': 0}}");
        QueryOptions options = QueryOptions.builder()
                .sortByField("age")
                .sortDirection(SortDirection.DESC)
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
                "{\"name\": \"Charlie\", \"age\": 30}",
                "{\"name\": \"Bob\", \"age\": 20}",
                "{\"name\": \"Alice\", \"age\": 10}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by age DESC");
    }

    // ==================== SORTBY Index Preference ====================

    @Test
    void shouldPreferSortByIndexOverMoreSelectiveIndex() {
        final String BUCKET = "test-sortby-index-preference";

        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status-idx", "status", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition createdAtIndex = SingleFieldIndexDefinition.create("created-idx", "createdAt", BsonType.INT64, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, statusIndex, createdAtIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'createdAt': {\"$numberLong\": \"3\"}}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'createdAt': {\"$numberLong\": \"1\"}}"),
                BSONUtil.jsonToDocumentThenBytes("{'status': 'active', 'createdAt': {\"$numberLong\": \"2\"}}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'status': 'active', 'createdAt': {'$gte': {\"$numberLong\": \"0\"}}}",
                "createdAt");

        assertInstanceOf(IndexScanNode.class, planWithParams.plan(), "Root should be IndexScanNode");
        IndexScanNode scanNode = (IndexScanNode) planWithParams.plan();
        assertEquals("createdAt", scanNode.getIndexDefinition().selector(),
                "Should prefer createdAt index because it matches SORTBY field");

        QueryOptions options = QueryOptions.builder()
                .sortByField("createdAt")
                .sortDirection(SortDirection.ASC)
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
                "{\"status\": \"active\", \"createdAt\": 1}",
                "{\"status\": \"active\", \"createdAt\": 2}",
                "{\"status\": \"active\", \"createdAt\": 3}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by createdAt ASC");
    }

    // ==================== SORTBY with Compound Indexes ====================

    @Test
    void shouldSortByCompoundIndexFieldAscending() {
        // Behavior: SORTBY on a compound index field with ASC direction returns documents sorted
        // by that field in ascending order when the compound index is used.
        final String BUCKET = "test-sortby-compound-asc";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 20}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'name': 'Alice', 'age': {'$gte': 0}}", "age");
        QueryOptions options = QueryOptions.builder()
                .sortByField("age")
                .sortDirection(SortDirection.ASC)
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
                "{\"name\": \"Alice\", \"age\": 10}",
                "{\"name\": \"Alice\", \"age\": 20}",
                "{\"name\": \"Alice\", \"age\": 30}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by age ASC via compound index");
    }

    @Test
    void shouldSortByCompoundIndexFieldDescending() {
        // Behavior: SORTBY on a compound index field with DESC direction returns documents sorted
        // by that field in descending order via reverse compound index scan.
        final String BUCKET = "test-sortby-compound-desc";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 20}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'name': 'Alice', 'age': {'$gte': 0}}", "age");
        QueryOptions options = QueryOptions.builder()
                .sortByField("age")
                .sortDirection(SortDirection.DESC)
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
                "{\"name\": \"Alice\", \"age\": 30}",
                "{\"name\": \"Alice\", \"age\": 20}",
                "{\"name\": \"Alice\", \"age\": 10}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by age DESC via reverse compound index scan");
    }

    @Test
    void shouldSortByCompoundIndexFieldDescendingWithPagination() {
        // Behavior: SORTBY DESC on a compound index field with LIMIT correctly paginates across
        // multiple batches in descending order.
        final String BUCKET = "test-sortby-compound-desc-pagination";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 21}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 22}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 23}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 24}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 22}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'name': 'Alice', 'age': {'$gte': 20, '$lte': 24}}", "age");
        QueryOptions options = QueryOptions.builder()
                .sortByField("age")
                .sortDirection(SortDirection.DESC)
                .limit(2)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            while (true) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);
                if (results.isEmpty()) {
                    break;
                }
                for (ByteBuffer buffer : results) {
                    actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
                }
            }
        }

        assertEquals(5, actualResult.size(), "Should return all 5 Alice docs");
        List<String> expectedResult = List.of(
                "{\"name\": \"Alice\", \"age\": 24}",
                "{\"name\": \"Alice\", \"age\": 23}",
                "{\"name\": \"Alice\", \"age\": 22}",
                "{\"name\": \"Alice\", \"age\": 21}",
                "{\"name\": \"Alice\", \"age\": 20}"
        );
        assertEquals(expectedResult, actualResult, "Results should be paginated in descending age order");
    }

    @Test
    void shouldSortByCompoundIndexFieldWithThreeFieldIndex() {
        // Behavior: SORTBY on the range field of a three-field compound index returns documents
        // sorted by that field when the first two fields are EQ-filtered.
        final String BUCKET = "test-sortby-compound-three-field";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_abc", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false),
                new CompoundIndexField("c", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 2, 'c': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 2, 'c': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 2, 'c': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 2, 'c': 40}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 3, 'c': 20}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'a': 1, 'b': 2, 'c': {'$gte': 10, '$lte': 40}}", "c");
        QueryOptions options = QueryOptions.builder()
                .sortByField("c")
                .sortDirection(SortDirection.DESC)
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
                "{\"a\": 1, \"b\": 2, \"c\": 40}",
                "{\"a\": 1, \"b\": 2, \"c\": 30}",
                "{\"a\": 1, \"b\": 2, \"c\": 20}",
                "{\"a\": 1, \"b\": 2, \"c\": 10}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by c DESC via three-field compound index");
    }

    // ==================== Sort-aware single-filter compound index selection ====================

    @Test
    void shouldSortBySingleFilterWithCompoundIndexAscending() {
        // Behavior: A single EQ filter with SORTBY on the next compound index field returns
        // documents sorted by that field in ascending order via compound index scan.
        final String BUCKET = "test-sortby-single-filter-compound-asc";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 20}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'category': 'bugs'}", "priority");
        QueryOptions options = QueryOptions.builder()
                .sortByField("priority")
                .sortDirection(SortDirection.ASC)
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
                "{\"category\": \"bugs\", \"priority\": 10}",
                "{\"category\": \"bugs\", \"priority\": 20}",
                "{\"category\": \"bugs\", \"priority\": 30}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by priority ASC via compound index");
    }

    @Test
    void shouldSortBySingleFilterWithCompoundIndexDescending() {
        // Behavior: A single EQ filter with SORTBY DESC on the next compound index field returns
        // documents sorted by that field in descending order via reverse compound index scan.
        final String BUCKET = "test-sortby-single-filter-compound-desc";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 20}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'category': 'bugs'}", "priority");
        QueryOptions options = QueryOptions.builder()
                .sortByField("priority")
                .sortDirection(SortDirection.DESC)
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
                "{\"category\": \"bugs\", \"priority\": 30}",
                "{\"category\": \"bugs\", \"priority\": 20}",
                "{\"category\": \"bugs\", \"priority\": 10}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by priority DESC via reverse compound index scan");
    }

    @Test
    void shouldSortBySingleFilterWithCompoundIndexDescendingWithLimit() {
        // Behavior: A single EQ filter with SORTBY DESC and LIMIT 1 returns only the document
        // with the highest value for the sort field.
        final String BUCKET = "test-sortby-single-filter-compound-desc-limit";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 40}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 30}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'category': 'bugs'}", "priority");
        QueryOptions options = QueryOptions.builder()
                .sortByField("priority")
                .sortDirection(SortDirection.DESC)
                .limit(1)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
            }
        }

        assertEquals(1, actualResult.size(), "Should return exactly 1 document");
        List<String> expectedResult = List.of(
                "{\"category\": \"bugs\", \"priority\": 50}"
        );
        assertEquals(expectedResult, actualResult, "Should return the document with highest priority");
    }

    @Test
    void shouldSortBySingleFilterWithCompoundIndexDescendingWithPagination() {
        // Behavior: A single EQ filter with SORTBY DESC and LIMIT correctly paginates across
        // multiple batches in descending order.
        final String BUCKET = "test-sortby-single-filter-compound-desc-pagination";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 40}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 30}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'category': 'bugs'}", "priority");
        QueryOptions options = QueryOptions.builder()
                .sortByField("priority")
                .sortDirection(SortDirection.DESC)
                .limit(2)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            while (true) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);
                if (results.isEmpty()) {
                    break;
                }
                for (ByteBuffer buffer : results) {
                    actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
                }
            }
        }

        assertEquals(5, actualResult.size(), "Should return all 5 docs");
        List<String> expectedResult = List.of(
                "{\"category\": \"bugs\", \"priority\": 50}",
                "{\"category\": \"bugs\", \"priority\": 40}",
                "{\"category\": \"bugs\", \"priority\": 30}",
                "{\"category\": \"bugs\", \"priority\": 20}",
                "{\"category\": \"bugs\", \"priority\": 10}"
        );
        assertEquals(expectedResult, actualResult, "Results should be paginated in descending priority order");
    }

    @Test
    void shouldSortByThirdFieldWithMultipleEqPrefix() {
        // Behavior: Two EQ prefix fields with SORTBY on the third compound index field return
        // documents sorted by that field in descending order.
        final String BUCKET = "test-sortby-multi-eq-prefix-desc";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_cat_stat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("status", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'status': 'open', 'priority': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'status': 'open', 'priority': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'status': 'open', 'priority': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'status': 'closed', 'priority': 99}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata,
                "{'category': 'bugs', 'status': 'open'}", "priority");
        QueryOptions options = QueryOptions.builder()
                .sortByField("priority")
                .sortDirection(SortDirection.DESC)
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
                "{\"category\": \"bugs\", \"status\": \"open\", \"priority\": 30}",
                "{\"category\": \"bugs\", \"status\": \"open\", \"priority\": 20}",
                "{\"category\": \"bugs\", \"status\": \"open\", \"priority\": 10}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by priority DESC with multi-EQ prefix");
    }

    @Test
    void shouldSelectCorrectCompoundIndexForSortBy() {
        // Behavior: When two compound indexes share the same prefix field but differ in the
        // second field, the planner selects the compound index whose next field matches sortByField.
        final String BUCKET = "test-sortby-select-correct-compound";

        CompoundIndexDefinition compoundIdx1 = CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        CompoundIndexDefinition compoundIdx2 = CompoundIndexDefinition.create("idx_cat_ts", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("created_at", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx1, compoundIdx2});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 1, 'created_at': 300}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 3, 'created_at': 100}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'bugs', 'priority': 2, 'created_at': 200}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'category': 'bugs'}", "created_at");
        QueryOptions options = QueryOptions.builder()
                .sortByField("created_at")
                .sortDirection(SortDirection.DESC)
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
                "{\"category\": \"bugs\", \"priority\": 1, \"created_at\": 300}",
                "{\"category\": \"bugs\", \"priority\": 2, \"created_at\": 200}",
                "{\"category\": \"bugs\", \"priority\": 3, \"created_at\": 100}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by created_at DESC via correct compound index");
    }

    // ==================== SORTBY with $ne ====================

    @Test
    void shouldSortByWithNeAscending() {
        // Behavior: $ne with SORTBY on the same indexed field produces globally sorted results.
        // The index scan iterates in order and skips the excluded value via residual NE predicate.
        final String BUCKET = "test-sortby-ne-asc";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'age': 40}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$ne': 20}}", "age");
        QueryOptions options = QueryOptions.builder()
                .sortByField("age")
                .sortDirection(SortDirection.ASC)
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
                "{\"name\": \"Alice\", \"age\": 10}",
                "{\"name\": \"Charlie\", \"age\": 30}",
                "{\"name\": \"Diana\", \"age\": 40}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by age ASC, excluding $ne value");
    }

    @Test
    void shouldRejectSortByWithNeOnDifferentField() {
        // Behavior: $ne on field X with SORTBY on field Y is rejected because the index scan
        // on X does not provide ordering on Y.
        final String BUCKET = "test-sortby-ne-different-field-rejected";

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name-idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, nameIndex, ageIndex);

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class, () ->
                createPlanWithParams(metadata, "{'name': {'$ne': 'Alice'}}", "age")
        );
        assertTrue(ex.getMessage().contains("SORTBY 'age'"));
        assertTrue(ex.getMessage().contains("RESULTSORT"));
    }

    // ==================== SORTBY with $nin ====================

    @Test
    void shouldSortByWithNinAscending() {
        // Behavior: $nin with SORTBY produces globally sorted results via the same index.
        // PhysicalAnd([IndexScan(NE, val1), IndexScan(NE, val2)]) collapses to a single
        // index scan with residual NE predicate, preserving age index ordering.
        final String BUCKET = "test-sortby-nin-asc";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'age': 40}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Eve', 'age': 50}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$nin': [20, 40]}}", "age");
        QueryOptions options = QueryOptions.builder()
                .sortByField("age")
                .sortDirection(SortDirection.ASC)
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
                "{\"name\": \"Alice\", \"age\": 10}",
                "{\"name\": \"Charlie\", \"age\": 30}",
                "{\"name\": \"Eve\", \"age\": 50}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by age ASC, excluding $nin values");
    }

    @Test
    void shouldSortByWithNinDescending() {
        // Behavior: $nin with SORTBY DESC produces globally sorted results in descending order.
        final String BUCKET = "test-sortby-nin-desc";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'age': 40}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Eve', 'age': 50}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$nin': [20, 40]}}", "age");
        QueryOptions options = QueryOptions.builder()
                .sortByField("age")
                .sortDirection(SortDirection.DESC)
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
                "{\"name\": \"Eve\", \"age\": 50}",
                "{\"name\": \"Charlie\", \"age\": 30}",
                "{\"name\": \"Alice\", \"age\": 10}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by age DESC, excluding $nin values");
    }

    // ==================== SORTBY with $in ====================

    @Test
    void shouldSortByWithInAscending() {
        // Behavior: $in with SORTBY on the same indexed field produces globally sorted results
        // by executing EQ scans sequentially in value order via OrderedConcatNode.
        final String BUCKET = "test-sortby-in-asc";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'age': 40}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Eve', 'age': 50}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$in': [30, 10, 50]}}", "age");
        QueryOptions options = QueryOptions.builder()
                .sortByField("age")
                .sortDirection(SortDirection.ASC)
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
                "{\"name\": \"Alice\", \"age\": 10}",
                "{\"name\": \"Charlie\", \"age\": 30}",
                "{\"name\": \"Eve\", \"age\": 50}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by age ASC regardless of $in array order");
    }

    @Test
    void shouldSortByWithInDescending() {
        // Behavior: $in with SORTBY DESC on the same indexed field produces globally sorted
        // results in descending order.
        final String BUCKET = "test-sortby-in-desc";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'age': 40}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Eve', 'age': 50}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$in': [30, 10, 50]}}", "age");
        QueryOptions options = QueryOptions.builder()
                .sortByField("age")
                .sortDirection(SortDirection.DESC)
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
                "{\"name\": \"Eve\", \"age\": 50}",
                "{\"name\": \"Charlie\", \"age\": 30}",
                "{\"name\": \"Alice\", \"age\": 10}"
        );
        assertEquals(expectedResult, actualResult, "Results should be sorted by age DESC");
    }

    @Test
    void shouldSortByWithInAndPagination() {
        // Behavior: $in + SORTBY correctly paginates across EQ value boundaries.
        final String BUCKET = "test-sortby-in-pagination";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, ageIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'A1', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'A2', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'A3', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B1', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B2', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B3', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C1', 'age': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C2', 'age': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C3', 'age': 50}")
        );
        insertDocumentsAndGetObjectIds(BUCKET, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$in': [10, 30, 50]}}", "age");
        QueryOptions options = QueryOptions.builder()
                .sortByField("age")
                .sortDirection(SortDirection.ASC)
                .limit(2)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<String> actualResult = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            while (true) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);
                if (results.isEmpty()) {
                    break;
                }
                for (ByteBuffer buffer : results) {
                    actualResult.add(TestUtil.bsonToJsonWithoutId(buffer));
                }
            }
        }

        assertEquals(9, actualResult.size(), "Should return all 9 documents");
        // Verify global sort order: all age=10 first, then age=30, then age=50
        for (int i = 0; i < 3; i++) {
            assertTrue(actualResult.get(i).contains("\"age\": 10"), "First 3 should be age=10");
        }
        for (int i = 3; i < 6; i++) {
            assertTrue(actualResult.get(i).contains("\"age\": 30"), "Next 3 should be age=30");
        }
        for (int i = 6; i < 9; i++) {
            assertTrue(actualResult.get(i).contains("\"age\": 50"), "Last 3 should be age=50");
        }
    }

    @Test
    void shouldRejectSortByWithNinOnDifferentField() {
        // Behavior: $nin on field X with SORTBY on field Y is rejected because the index scan
        // on X does not provide ordering on Y.
        final String BUCKET = "test-sortby-nin-different-field-rejected";

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name-idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, nameIndex, ageIndex);

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class, () ->
                createPlanWithParams(metadata, "{'name': {'$nin': ['Alice', 'Bob']}}", "age")
        );
        assertTrue(ex.getMessage().contains("SORTBY 'age'"));
        assertTrue(ex.getMessage().contains("RESULTSORT"));
    }

    // ==================== SORTBY Validation ====================

    @Test
    void shouldRejectSortByWithNor() {
        // Behavior: $nor + SORTBY is rejected at planning time because the execution plan
        // cannot guarantee global ordering without scanning the entire index.
        final String BUCKET = "test-sortby-nor-rejected";

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, ageIndex);

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class, () ->
                createPlanWithParams(metadata, "{'$nor': [{'age': 10}, {'age': 30}]}", "age")
        );
        assertTrue(ex.getMessage().contains("SORTBY 'age'"));
        assertTrue(ex.getMessage().contains("RESULTSORT"));
    }

    @Test
    void shouldRejectSortByWithInOnDifferentField() {
        // Behavior: $in on field X with SORTBY on field Y is rejected because PhysicalOr
        // children scan X, not Y.
        final String BUCKET = "test-sortby-in-different-field-rejected";

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name-idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET, nameIndex, ageIndex);

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class, () ->
                createPlanWithParams(metadata, "{'name': {'$in': ['Alice', 'Bob']}}", "age")
        );
        assertTrue(ex.getMessage().contains("SORTBY 'age'"));
        assertTrue(ex.getMessage().contains("RESULTSORT"));
    }

    @Test
    void shouldRejectSortByOnNonIndexedFieldWithoutCompoundIndex() {
        // Behavior: SORTBY on a field without a supporting index is rejected at planning time
        // with an actionable error message, instead of silently returning wrong results.
        final String BUCKET = "test-sortby-no-compound-rejected";

        createBucket(BUCKET);
        BucketMetadata metadata = getBucketMetadata(BUCKET);

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class, () ->
                createPlanWithParams(metadata, "{'category': 'bugs'}", "sequence")
        );
        assertTrue(ex.getMessage().contains("SORTBY 'sequence'"));
        assertTrue(ex.getMessage().contains("Hint:"));
    }
}
