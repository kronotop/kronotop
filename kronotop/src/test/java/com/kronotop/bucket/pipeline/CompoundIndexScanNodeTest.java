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
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CompoundIndexScanNodeTest extends BasePipelineTest {

    @Test
    void shouldReturnDocumentsMatchingAllEqFilters() {
        // Behavior: A compound index on (name:STRING, age:INT32) with an all-EQ query should
        // perform a single range scan and return only documents matching both fields exactly.
        final String bucket = "compound-all-eq";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 30}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'name': 'Alice', 'age': 25}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(1, results.size());
            assertEquals(Set.of("Alice"), extractNamesFromResults(results));
            assertEquals(Set.of(25), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldReturnDocumentsMatchingEqPrefixPlusGtRange() {
        // Behavior: Compound index (name:STRING, age:INT32) with query {name:'Alice', age:{$gt:20}}
        // should return only Alice's documents with age > 20.
        final String bucket = "compound-eq-gt";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 35}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'name': 'Alice', 'age': {'$gt': 20}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            assertEquals(Set.of("Alice"), extractNamesFromResults(results));
            assertEquals(Set.of(25, 35), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldReturnDocumentsMatchingEqPrefixPlusLtRange() {
        // Behavior: Compound index (name:STRING, age:INT32) with query {name:'Alice', age:{$lt:30}}
        // should return only Alice's documents with age < 30.
        final String bucket = "compound-eq-lt";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 35}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'name': 'Alice', 'age': {'$lt': 30}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            assertEquals(Set.of("Alice"), extractNamesFromResults(results));
            assertEquals(Set.of(15, 25), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldReturnDocumentsMatchingEqPrefixPlusRange() {
        // Behavior: Compound index (name:STRING, age:INT32) with query
        // {name:'Alice', age:{$gte:20, $lte:30}} should return Alice's docs within the range.
        final String bucket = "compound-eq-range";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 35}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'name': 'Alice', 'age': {'$gte': 20, '$lte': 30}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(3, results.size());
            assertEquals(Set.of("Alice"), extractNamesFromResults(results));
            assertEquals(Set.of(20, 25, 30), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldReturnEmptyResultWhenNoMatch() {
        // Behavior: A compound index query with values not present in the data should return no results.
        final String bucket = "compound-no-match";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 30}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'name': 'Charlie', 'age': 40}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(0, results.size());
        }
    }

    @Test
    void shouldHandleThreeFieldCompoundIndex() {
        // Behavior: A three-field compound index (a:INT32, b:INT32, c:INT32) with all-EQ query
        // should return only documents matching all three fields.
        final String bucket = "compound-three-field";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_abc", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false),
                new CompoundIndexField("c", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 2, 'c': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 2, 'c': 4}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 3, 'c': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 2, 'b': 2, 'c': 3}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': 1, 'b': 2, 'c': 3}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(1, results.size());
            assertEquals(Set.of(1), extractIntegerFieldFromResults(results, "a"));
            assertEquals(Set.of(2), extractIntegerFieldFromResults(results, "b"));
            assertEquals(Set.of(3), extractIntegerFieldFromResults(results, "c"));
        }
    }

    @Test
    void shouldHandleCursorContinuation() {
        // Behavior: Compound index scan with LIMIT should paginate correctly across multiple batches.
        final String bucket = "compound-cursor";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        // Insert 7 docs that all match the query
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 4}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 5}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 6}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 7}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'name': 'Alice', 'age': 25}");
        QueryOptions options = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        int totalResults = 0;
        try (Transaction tr = createTransaction()) {
            while (true) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);
                if (results.isEmpty()) {
                    break;
                }
                totalResults += results.size();
            }
        }
        assertEquals(7, totalResults);
    }

    @Test
    void shouldWorkWithResidualPredicates() {
        // Behavior: Query {a:1, b:2, c:3} with compound index on (a, b) should use compound scan
        // for (a, b) and apply residual predicate for c.
        final String bucket = "compound-residual";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 2, 'c': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 2, 'c': 4}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 2, 'c': 5}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 3, 'c': 3}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': 1, 'b': 2, 'c': 3}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(1, results.size());
            assertEquals(Set.of(1), extractIntegerFieldFromResults(results, "a"));
            assertEquals(Set.of(2), extractIntegerFieldFromResults(results, "b"));
            assertEquals(Set.of(3), extractIntegerFieldFromResults(results, "c"));
        }
    }

    @Test
    void shouldReturnDocumentsMatchingEqOnFirstIntPlusGtOnSecondInt() {
        // Behavior: Compound index (a:INT32, b:INT32) with query {a:5, b:{$gt:10}} should return
        // only documents where a=5 and b > 10.
        final String bucket = "compound-int-eq-gt";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 5}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 3, 'b': 15}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': 5, 'b': {'$gt': 10}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            assertEquals(Set.of(5), extractIntegerFieldFromResults(results, "a"));
            assertEquals(Set.of(15, 20), extractIntegerFieldFromResults(results, "b"));
        }
    }

    @Test
    void shouldReturnDocumentsMatchingEqOnFirstIntPlusLtOnSecondInt() {
        // Behavior: Compound index (a:INT32, b:INT32) with query {a:5, b:{$lt:30}} should return
        // only documents where a=5 and b < 30.
        final String bucket = "compound-int-eq-lt";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 40}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 3, 'b': 20}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': 5, 'b': {'$lt': 30}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            assertEquals(Set.of(5), extractIntegerFieldFromResults(results, "a"));
            assertEquals(Set.of(10, 20), extractIntegerFieldFromResults(results, "b"));
        }
    }

    @Test
    void shouldReturnDocumentsMatchingEqOnFirstIntPlusBoundedRangeOnSecondInt() {
        // Behavior: Compound index (a:INT32, b:INT32) with query {a:5, b:{$gte:10, $lte:30}} should
        // return only documents where a=5 and 10 <= b <= 30.
        final String bucket = "compound-int-eq-bounded";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 5}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 35}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 3, 'b': 20}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': 5, 'b': {'$gte': 10, '$lte': 30}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(3, results.size());
            assertEquals(Set.of(5), extractIntegerFieldFromResults(results, "a"));
            assertEquals(Set.of(10, 20, 30), extractIntegerFieldFromResults(results, "b"));
        }
    }

    @Test
    void shouldRespectGtVsGteBoundaryExactness() {
        // Behavior: Compound index (a:INT32, b:INT32) with $gt should exclude the boundary value
        // while $gte should include it, locking in strict vs inclusive boundary semantics.
        final String bucket = "compound-int-gt-gte-boundary";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 25}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        // Query A: $gt excludes boundary
        PlanWithParams planA = createPlanWithParams(metadata, "{'a': 5, 'b': {'$gt': 20}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctxA = new QueryContext(getSession(), metadata, options, planA.plan(), planA.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> resultsA = readExecutor.execute(tr, ctxA);
            assertEquals(1, resultsA.size());
            assertEquals(Set.of(25), extractIntegerFieldFromResults(resultsA, "b"));
        }

        // Query B: $gte includes boundary
        PlanWithParams planB = createPlanWithParams(metadata, "{'a': 5, 'b': {'$gte': 20}}");
        QueryContext ctxB = new QueryContext(getSession(), metadata, options, planB.plan(), planB.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> resultsB = readExecutor.execute(tr, ctxB);
            assertEquals(2, resultsB.size());
            assertEquals(Set.of(20, 25), extractIntegerFieldFromResults(resultsB, "b"));
        }
    }

    @Test
    void shouldUseCompoundScanWithResidualWhenRangeOnBothFields() {
        // Behavior: Query {a:{$gt:3}, b:{$gt:10}} with compound index (a:INT32, b:INT32) should
        // use compound scan for a>3 on the leading prefix, with b>10 applied as residual filter.
        final String bucket = "compound-int-range-both";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 5}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 5}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 15}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 10, 'b': 15}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': {'$gt': 3}, 'b': {'$gt': 10}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            assertEquals(Set.of(5, 10), extractIntegerFieldFromResults(results, "a"));
            assertEquals(Set.of(15), extractIntegerFieldFromResults(results, "b"));
        }
    }

    @Test
    void shouldReturnEmptyResultWhenRangeExceedsAllValues() {
        // Behavior: Compound index (a:INT32, b:INT32) with query {a:5, b:{$gt:100}} should return
        // no results when no documents have a=5 with b > 100, and documents with other 'a' values
        // must not leak through.
        final String bucket = "compound-int-range-exceeds";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 3, 'b': 200}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': 5, 'b': {'$gt': 100}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(0, results.size());
        }
    }

    @Test
    void shouldHandleNegativeNumbersInRange() {
        // Behavior: Compound index (a:INT32, b:INT32) with query {a:-5, b:{$gt:-20, $lt:-5}} should
        // correctly handle negative numbers, validating FDB Tuple packing preserves ordering for
        // negative longs.
        final String bucket = "compound-int-negative";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': -5, 'b': -25}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': -5, 'b': -20}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': -5, 'b': -15}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': -5, 'b': -10}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': -5, 'b': -5}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': -5, 'b': 0}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': -3, 'b': -15}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': -5, 'b': {'$gt': -20, '$lt': -5}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            assertEquals(Set.of(-5), extractIntegerFieldFromResults(results, "a"));
            assertEquals(Set.of(-15, -10), extractIntegerFieldFromResults(results, "b"));
        }
    }

    @Test
    void shouldHandleCursorContinuationWithRange() {
        // Behavior: Compound index scan with EQ prefix + range filter and LIMIT should paginate correctly.
        final String bucket = "compound-cursor-range";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 21}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 22}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 23}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 24}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 22}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'name': 'Alice', 'age': {'$gte': 20, '$lte': 24}}");
        QueryOptions options = QueryOptions.builder().limit(2).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        int totalResults = 0;
        try (Transaction tr = createTransaction()) {
            while (true) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);
                if (results.isEmpty()) {
                    break;
                }
                totalResults += results.size();
            }
        }
        assertEquals(5, totalResults);
    }

    @Test
    void shouldReturnAllEqResultsInReverseOrder() {
        // Behavior: Compound index (name:STRING, age:INT32) with all-EQ query and sortDirection=DESC
        // should return matching documents in reverse insertion order.
        final String bucket = "compound-all-eq-reverse";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 3}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        // Forward order
        PlanWithParams forwardPlan = createPlanWithParams(metadata, "{'name': 'Alice', 'age': 25}");
        QueryOptions forwardOptions = QueryOptions.builder().build();
        QueryContext forwardCtx = new QueryContext(getSession(), metadata, forwardOptions, forwardPlan.plan(), forwardPlan.parameters());

        List<String> forwardResults;
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, forwardCtx);
            forwardResults = results.stream().map(TestUtil::bsonToJsonWithoutId).toList();
        }

        // Reverse order
        PlanWithParams reversePlan = createPlanWithParams(metadata, "{'name': 'Alice', 'age': 25}");
        QueryOptions reverseOptions = QueryOptions.builder().sortDirection(SortDirection.DESC).build();
        QueryContext reverseCtx = new QueryContext(getSession(), metadata, reverseOptions, reversePlan.plan(), reversePlan.parameters());

        List<String> reverseResults;
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, reverseCtx);
            reverseResults = results.stream().map(TestUtil::bsonToJsonWithoutId).toList();
        }

        assertEquals(3, forwardResults.size());
        assertEquals(3, reverseResults.size());
        assertEquals(forwardResults, reverseResults.reversed());
    }

    @Test
    void shouldReturnEqPrefixPlusRangeInReverseOrder() {
        // Behavior: Compound index (name:STRING, age:INT32) with EQ prefix + range and sortDirection=DESC
        // should return ages in descending order.
        final String bucket = "compound-eq-range-reverse";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'name': 'Alice', 'age': {'$gte': 20, '$lte': 30}}");
        QueryOptions options = QueryOptions.builder().sortDirection(SortDirection.DESC).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(3, results.size());
            List<Integer> ages = extractOrderedIntegerField(results, "age");
            assertEquals(List.of(30, 25, 20), ages);
        }
    }

    @Test
    void shouldHandleCursorContinuationWithReverse() {
        // Behavior: Compound index scan with all-EQ query, LIMIT, and sortDirection=DESC should
        // paginate correctly in reverse order across multiple batches.
        final String bucket = "compound-cursor-reverse";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 4}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 25, 'seq': 5}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        // Forward: collect all results
        PlanWithParams forwardPlan = createPlanWithParams(metadata, "{'name': 'Alice', 'age': 25}");
        QueryOptions forwardOptions = QueryOptions.builder().limit(2).build();
        QueryContext forwardCtx = new QueryContext(getSession(), metadata, forwardOptions, forwardPlan.plan(), forwardPlan.parameters());

        List<String> allForward = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            while (true) {
                List<ByteBuffer> results = readExecutor.execute(tr, forwardCtx);
                if (results.isEmpty()) break;
                results.forEach(r -> allForward.add(TestUtil.bsonToJsonWithoutId(r)));
            }
        }

        // Reverse: collect all results
        PlanWithParams reversePlan = createPlanWithParams(metadata, "{'name': 'Alice', 'age': 25}");
        QueryOptions reverseOptions = QueryOptions.builder().limit(2).sortDirection(SortDirection.DESC).build();
        QueryContext reverseCtx = new QueryContext(getSession(), metadata, reverseOptions, reversePlan.plan(), reversePlan.parameters());

        List<String> allReverse = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            while (true) {
                List<ByteBuffer> results = readExecutor.execute(tr, reverseCtx);
                if (results.isEmpty()) break;
                allReverse.add(TestUtil.bsonToJsonWithoutId(results.getFirst()));
                allReverse.addAll(results.stream().skip(1).map(TestUtil::bsonToJsonWithoutId).toList());
            }
        }

        assertEquals(5, allForward.size());
        assertEquals(5, allReverse.size());
        assertEquals(allForward, allReverse.reversed());
    }

    @Test
    void shouldHandleCursorContinuationWithRangeAndReverse() {
        // Behavior: Compound index scan with EQ prefix + range filter, LIMIT, and sortDirection=DESC
        // should paginate correctly in descending order without duplicates or gaps.
        final String bucket = "compound-cursor-range-reverse";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 21}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 22}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 23}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 24}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 22}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'name': 'Alice', 'age': {'$gte': 20, '$lte': 24}}");
        QueryOptions options = QueryOptions.builder().limit(2).sortDirection(SortDirection.DESC).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<Integer> allAges = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            while (true) {
                List<ByteBuffer> results = readExecutor.execute(tr, ctx);
                if (results.isEmpty()) break;
                allAges.addAll(extractOrderedIntegerField(results, "age"));
            }
        }
        assertEquals(List.of(24, 23, 22, 21, 20), allAges);
    }

    @Test
    void shouldReverseThreeFieldCompoundIndex() {
        // Behavior: Three-field compound index (a:INT32, b:INT32, c:INT32) with all-EQ on first two
        // fields and range on third, sortDirection=DESC, should return results in descending order
        // on the range field.
        final String bucket = "compound-three-field-reverse";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_abc", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false),
                new CompoundIndexField("c", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 2, 'c': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 2, 'c': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 2, 'c': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 2, 'c': 40}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 1, 'b': 3, 'c': 20}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': 1, 'b': 2, 'c': {'$gte': 10, '$lte': 40}}");
        QueryOptions options = QueryOptions.builder().sortDirection(SortDirection.DESC).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(4, results.size());
            List<Integer> cValues = extractOrderedIntegerField(results, "c");
            assertEquals(List.of(40, 30, 20, 10), cValues);
        }
    }

    @Test
    void shouldReturnDocumentsMatchingEqStringPlusGtDoubleRange() {
        // Behavior: Compound index (category:STRING, price:DOUBLE) with query
        // {category:'electronics', price:{$gt:100.0}} should return only electronics
        // documents with price > 100.0, validating DOUBLE type in compound index range scans.
        final String bucket = "compound-string-double-range";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_cat_price", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("price", BsonType.DOUBLE, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'category': 'electronics', 'price': 49.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'electronics', 'price': 299.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'electronics', 'price': 999.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'category': 'books', 'price': 199.99}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'category': 'electronics', 'price': {'$gt': 100.0}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            assertEquals(Set.of("electronics"), extractNamesFromResults(results, "category"));
            assertEquals(Set.of(299.99, 999.99), extractDoubleFieldFromResults(results, "price"));
        }
    }

    // ==================== Leading prefix scans ====================

    @Test
    void shouldReturnDocumentsMatchingSingleEqOnFirstField() {
        // Behavior: Compound index (a:INT32, b:INT32) with query {a:5} should use compound
        // scan on the leading prefix and return only documents where a = 5.
        final String bucket = "compound-single-eq-first";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 10, 'b': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 15, 'b': 40}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': 5}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            assertEquals(Set.of(5), extractIntegerFieldFromResults(results, "a"));
            assertEquals(Set.of(10, 20), extractIntegerFieldFromResults(results, "b"));
        }
    }

    @Test
    void shouldReturnDocumentsMatchingSingleGteOnFirstField() {
        // Behavior: Compound index (age:INT32, score:INT32) with query {age:{$gte:20}} should
        // use compound scan on the leading prefix and return only documents with age >= 20.
        final String bucket = "compound-single-gte-first";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_age_score", List.of(
                new CompoundIndexField("age", BsonType.INT32, false),
                new CompoundIndexField("score", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'age': 15, 'score': 80}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 20, 'score': 90}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 25, 'score': 70}"),
                BSONUtil.jsonToDocumentThenBytes("{'age': 30, 'score': 60}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'age': {'$gte': 20}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(3, results.size());
            assertEquals(Set.of(20, 25, 30), extractIntegerFieldFromResults(results, "age"));
        }
    }

    @Test
    void shouldReturnDocumentsMatchingSingleGtOnFirstField() {
        // Behavior: Compound index (a:INT32, b:INT32) with query {a:{$gt:5}} should use
        // compound scan on the leading prefix and return only documents with a > 5.
        final String bucket = "compound-single-gt-first";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 3, 'b': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 8, 'b': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 12, 'b': 40}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': {'$gt': 5}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            assertEquals(Set.of(8, 12), extractIntegerFieldFromResults(results, "a"));
        }
    }

    @Test
    void shouldReturnDocumentsMatchingSingleLtOnFirstField() {
        // Behavior: Compound index (a:INT32, b:INT32) with query {a:{$lt:30}} should use
        // compound scan on the leading prefix and return only documents with a < 30.
        final String bucket = "compound-single-lt-first";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 10, 'b': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 20, 'b': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 30, 'b': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 40, 'b': 4}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': {'$lt': 30}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            assertEquals(Set.of(10, 20), extractIntegerFieldFromResults(results, "a"));
        }
    }

    @Test
    void shouldReturnDocumentsMatchingSingleBoundedRangeOnFirstField() {
        // Behavior: Compound index (a:INT32, b:INT32) with query {a:{$gte:10, $lte:30}} should
        // use compound scan on the leading prefix and return only documents with 10 <= a <= 30.
        final String bucket = "compound-single-bounded-first";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 5, 'b': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 10, 'b': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 20, 'b': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 30, 'b': 4}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 40, 'b': 5}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': {'$gte': 10, '$lte': 30}}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(3, results.size());
            assertEquals(Set.of(10, 20, 30), extractIntegerFieldFromResults(results, "a"));
        }
    }

    @Test
    void shouldReturnDocumentsMatchingRangeOnFirstFieldWithResidualEq() {
        // Behavior: Compound index (a:INT32, b:INT32) with query {a:{$gt:5}, b:2} should
        // use compound scan for a>5 and apply b=2 as residual filter.
        final String bucket = "compound-range-first-residual-eq";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'a': 3, 'b': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 8, 'b': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 8, 'b': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 12, 'b': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'a': 12, 'b': 5}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{'a': {'$gt': 5}, 'b': 2}");
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            assertEquals(2, results.size());
            assertEquals(Set.of(8, 12), extractIntegerFieldFromResults(results, "a"));
            assertEquals(Set.of(2), extractIntegerFieldFromResults(results, "b"));
        }
    }

    private Set<String> extractNamesFromResults(List<ByteBuffer> results, String field) {
        Set<String> values = new HashSet<>();
        for (ByteBuffer documentBuffer : results) {
            documentBuffer.rewind();
            try (org.bson.BsonBinaryReader reader = new org.bson.BsonBinaryReader(documentBuffer)) {
                reader.readStartDocument();
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if (field.equals(fieldName)) {
                        values.add(reader.readString());
                    } else {
                        reader.skipValue();
                    }
                }
                reader.readEndDocument();
            }
        }
        return values;
    }

    private Set<Double> extractDoubleFieldFromResults(List<ByteBuffer> results, String field) {
        Set<Double> values = new LinkedHashSet<>();
        for (ByteBuffer documentBuffer : results) {
            documentBuffer.rewind();
            try (org.bson.BsonBinaryReader reader = new org.bson.BsonBinaryReader(documentBuffer)) {
                reader.readStartDocument();
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if (field.equals(fieldName)) {
                        values.add(reader.readDouble());
                    } else {
                        reader.skipValue();
                    }
                }
                reader.readEndDocument();
            }
        }
        return values;
    }

    private List<Integer> extractOrderedIntegerField(List<ByteBuffer> results, String field) {
        List<Integer> values = new ArrayList<>();
        for (ByteBuffer documentBuffer : results) {
            documentBuffer.rewind();
            try (org.bson.BsonBinaryReader reader = new org.bson.BsonBinaryReader(documentBuffer)) {
                reader.readStartDocument();
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    String fieldName = reader.readName();
                    if (field.equals(fieldName)) {
                        values.add(reader.readInt32());
                    } else {
                        reader.skipValue();
                    }
                }
                reader.readEndDocument();
            }
        }
        return values;
    }

    // ============================================================================
    // Numeric Widening Tests
    // ============================================================================

    @Test
    void shouldFindDocumentsViaCompoundIndexScanWhenInt32PredicateMatchesInt64Fields() {
        // Behavior: INT32 query literals should match INT64 compound index fields via lossless widening
        final String bucket = "compound-widening-int32-to-int64";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_x_y", List.of(
                new CompoundIndexField("x", BsonType.INT64, false),
                new CompoundIndexField("y", BsonType.INT64, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'x': {'$numberLong': '10'}, 'y': {'$numberLong': '20'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': {'$numberLong': '10'}, 'y': {'$numberLong': '30'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': {'$numberLong': '20'}, 'y': {'$numberLong': '20'}, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        // Query with INT32 literals against INT64 compound index
        List<String> results = runQueryOnBucket(metadata, "{'x': 10, 'y': 20}");

        assertEquals(1, results.size());
        assertEquals(List.of("{\"x\": 10, \"y\": 20, \"name\": \"Alice\"}"), results);
    }

    @Test
    void shouldFindDocumentsViaCompoundIndexScanWhenInt32RangeMatchesInt64Field() {
        // Behavior: INT32 range predicate on second compound field should work with INT64 index
        final String bucket = "compound-widening-int32-to-int64-range";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_score", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("score", BsonType.INT64, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'score': {'$numberLong': '100'}}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'score': {'$numberLong': '200'}}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'score': {'$numberLong': '300'}}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'score': {'$numberLong': '150'}}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        // Query: name='Alice' (STRING EQ) AND score > 150 (INT32 range on INT64 index)
        List<String> results = runQueryOnBucket(metadata, "{'name': 'Alice', 'score': {'$gt': 150}}");

        assertEquals(2, results.size());
    }

    @Test
    void shouldFindDocumentsViaCompoundIndexScanWhenInt32PredicateMatchesDoubleFields() {
        // Behavior: INT32 query literals should match DOUBLE compound index fields via lossless widening
        final String bucket = "compound-widening-int32-to-double";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_x_y", List.of(
                new CompoundIndexField("x", BsonType.DOUBLE, false),
                new CompoundIndexField("y", BsonType.DOUBLE, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'x': 10.0, 'y': 20.0, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': 10.0, 'y': 30.0, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': 20.0, 'y': 20.0, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        // Query with INT32 literals against DOUBLE compound index
        List<String> results = runQueryOnBucket(metadata, "{'x': 10, 'y': 20}");

        assertEquals(1, results.size());
        assertEquals(List.of("{\"x\": 10.0, \"y\": 20.0, \"name\": \"Alice\"}"), results);
    }

    @Test
    void shouldFindDocumentsViaCompoundIndexScanWhenMixedWideningAcrossFields() {
        // Behavior: INT32 predicates should widen to INT64 on first field and DOUBLE on second
        // field in the same compound scan
        final String bucket = "compound-widening-mixed-int64-double";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_x_y", List.of(
                new CompoundIndexField("x", BsonType.INT64, false),
                new CompoundIndexField("y", BsonType.DOUBLE, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'x': {'$numberLong': '10'}, 'y': 20.0, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': {'$numberLong': '10'}, 'y': 30.0, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': {'$numberLong': '20'}, 'y': 20.0, 'name': 'Charlie'}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        // Both INT32 literals, widened to different target types (INT64 and DOUBLE)
        List<String> results = runQueryOnBucket(metadata, "{'x': 10, 'y': 20}");

        assertEquals(1, results.size());
        assertEquals(List.of("{\"x\": 10, \"y\": 20.0, \"name\": \"Alice\"}"), results);
    }

    @Test
    void shouldFindDocumentsViaCompoundIndexScanWhenInt32RangeMatchesDoubleField() {
        // Behavior: INT32 range predicate on second compound field should widen to DOUBLE for index scan
        final String bucket = "compound-widening-int32-range-to-double";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_name_price", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("price", BsonType.DOUBLE, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'price': 49.99}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'price': 150.0}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'price': 300.0}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'price': 200.0}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        // INT32 range on DOUBLE compound index field
        List<String> results = runQueryOnBucket(metadata, "{'name': 'Alice', 'price': {'$gt': 100}}");

        assertEquals(2, results.size());
    }

    @Test
    void shouldFindDocumentsViaCompoundIndexScanWhenInt32RangeOnFirstInt64Field() {
        // Behavior: INT32 range predicate on the leading compound field should widen to INT64
        // for prefix scan
        final String bucket = "compound-widening-int32-range-leading-int64";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_x_y", List.of(
                new CompoundIndexField("x", BsonType.INT64, false),
                new CompoundIndexField("y", BsonType.INT64, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'x': {'$numberLong': '5'}, 'y': {'$numberLong': '100'}, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': {'$numberLong': '10'}, 'y': {'$numberLong': '200'}, 'name': 'Bob'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': {'$numberLong': '15'}, 'y': {'$numberLong': '300'}, 'name': 'Charlie'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': {'$numberLong': '20'}, 'y': {'$numberLong': '400'}, 'name': 'Diana'}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        // INT32 range on leading INT64 compound field
        List<String> results = runQueryOnBucket(metadata, "{'x': {'$gte': 10}}");

        assertEquals(3, results.size());
    }

    @Test
    void shouldFallBackFromCompoundIndexWhenInt64PredicateMatchesDoubleField() {
        // Behavior: INT64 predicate on DOUBLE compound index is lossy; compound index is rejected
        // but query returns correct results via full scan
        final String bucket = "compound-widening-lossy-fallback";

        CompoundIndexDefinition compoundIdx = CompoundIndexDefinition.create("idx_x_y", List.of(
                new CompoundIndexField("x", BsonType.DOUBLE, false),
                new CompoundIndexField("y", BsonType.DOUBLE, false)
        ), IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(bucket,
                new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundIdx});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'x': 42.0, 'y': 1.5, 'name': 'Alice'}"),
                BSONUtil.jsonToDocumentThenBytes("{'x': 99.0, 'y': 2.5, 'name': 'Bob'}")
        );
        insertDocumentsAndGetObjectIds(bucket, documents);

        // INT64 on DOUBLE field = lossy, compound index rejected, falls back to full scan
        List<String> results = runQueryOnBucket(metadata, "{'x': {'$numberLong': '42'}, 'y': 1.5}");

        assertEquals(1, results.size());
        assertEquals(List.of("{\"x\": 42.0, \"y\": 1.5, \"name\": \"Alice\"}"), results);
    }
}
