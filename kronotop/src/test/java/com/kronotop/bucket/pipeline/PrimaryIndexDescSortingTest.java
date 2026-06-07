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
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Tests for primary index (_id) sorting with DESC direction.
 *
 * <p><b>Bug Fixed:</b> Queries like {@code {"_id": {"$gt": "..."}}} with {@code sortby _id desc}
 * would freeze/hang indefinitely while ASC worked fine.</p>
 *
 * <p><b>Root Cause:</b> In {@code SelectorCalculator}, cursor continuation keys were constructed
 * incorrectly for the primary index. The code built cursor tuples as:
 * <pre>
 *   Tuple.from(ENTRIES, indexValue, versionstamp)
 * </pre>
 * This works for secondary indexes where the key structure is {@code (ENTRIES, value, versionstamp)},
 * but for the primary index (_id), the key structure is just {@code (ENTRIES, versionstamp)}.
 * Since indexValue IS the versionstamp for _id, this created {@code (ENTRIES, vs, vs)} instead of
 * {@code (ENTRIES, vs)}.</p>
 *
 * <p><b>Effect:</b> The malformed cursor key caused {@code firstGreaterOrEqual} to resolve to the
 * NEXT key instead of the current key. In DESC mode, this made the range include the same documents
 * repeatedly, causing an infinite loop during pagination.</p>
 *
 * <p><b>Fix:</b> {@code SelectorCalculator.buildCursorTuple()} now detects primary index by checking
 * if indexValue is a Versionstamp, and constructs the correct tuple structure accordingly.</p>
 *
 */
class PrimaryIndexDescSortingTest extends BasePipelineTest {

    @Test
    void shouldSortByIdDescWithGtOperator() {
        // Behavior: Query with _id > X and SORTBY _id DESC returns documents in descending _id order.
        // This exercises SelectorCalculator.buildCursorTuple() for primary index cursor continuation.
        final String BUCKET = "test-id-gt-desc";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET);

        // Insert documents - they get sequential versionstamps
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc1', 'seq': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc2', 'seq': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc3', 'seq': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc4', 'seq': 4}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc5', 'seq': 5}")
        );
        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(BUCKET, documents);

        // Query: _id > first document's versionstamp, sorted DESC
        // This should return doc5, doc4, doc3, doc2 (excluding doc1)
        String firstId = objectIds.get(0).toHexString();
        String query = String.format("{'_id': {'$gt': '%s'}}", firstId);

        PlanWithParams planWithParams = createPlanWithParams(metadata, query, "_id");
        assertInstanceOf(IndexScanNode.class, planWithParams.plan(), "Should use IndexScanNode for _id query");

        QueryOptions options = QueryOptions.builder()
                .sortByField("_id")
                .sortDirection(SortDirection.DESC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<Integer> actualSeqs = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                String json = TestUtil.bsonToJsonWithoutId(buffer);
                // Extract seq value
                int seq = Integer.parseInt(json.replaceAll(".*\"seq\": (\\d+).*", "$1"));
                actualSeqs.add(seq);
            }
        }

        // Should be in descending order: 5, 4, 3, 2 (doc1 excluded by $gt)
        List<Integer> expectedSeqs = List.of(5, 4, 3, 2);
        assertEquals(expectedSeqs, actualSeqs, "Results should be sorted by _id DESC");
    }

    @Test
    void shouldSortByIdDescWithGtOperatorAndSmallLimit() {
        // Behavior: Query with _id > X, SORTBY _id DESC, and small LIMIT triggers cursor continuation.
        // This is the critical test for SelectorCalculator.buildCursorTuple() - it must correctly
        // construct cursor tuples for the primary index (ENTRIES, versionstamp) vs secondary index
        // (ENTRIES, value, versionstamp) to avoid infinite loops during pagination.
        final String BUCKET = "test-id-gt-desc-pagination";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET);

        // Insert 10 documents
        List<byte[]> documents = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'name': 'doc%d', 'seq': %d}", i, i)));
        }
        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(BUCKET, documents);

        // Query: _id > first document, with small limit to force multiple batches
        String firstId = objectIds.get(0).toHexString();
        String query = String.format("{'_id': {'$gt': '%s'}}", firstId);

        PlanWithParams planWithParams = createPlanWithParams(metadata, query, "_id");
        assertInstanceOf(IndexScanNode.class, planWithParams.plan(), "Should use IndexScanNode for _id query");

        // Use limit=3 to force pagination (9 matching docs / 3 = 3 batches)
        QueryOptions options = QueryOptions.builder()
                .sortByField("_id")
                .sortDirection(SortDirection.DESC)
                .limit(3)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<Integer> actualSeqs = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                String json = TestUtil.bsonToJsonWithoutId(buffer);
                int seq = Integer.parseInt(json.replaceAll(".*\"seq\": (\\d+).*", "$1"));
                actualSeqs.add(seq);
            }
        }

        // With limit=3 and DESC, should return top 3: 10, 9, 8
        List<Integer> expectedSeqs = List.of(10, 9, 8);
        assertEquals(expectedSeqs, actualSeqs, "Results should be top 3 by _id DESC");
    }

    @Test
    void shouldSortByIdDescWithGteOperator() {
        // Behavior: Query with _id >= X and SORTBY _id DESC returns documents in descending order
        // including the boundary document.
        final String BUCKET = "test-id-gte-desc";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc1', 'seq': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc2', 'seq': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc3', 'seq': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc4', 'seq': 4}")
        );
        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(BUCKET, documents);

        // Query: _id >= second document's versionstamp
        String secondId = objectIds.get(1).toHexString();
        String query = String.format("{'_id': {'$gte': '%s'}}", secondId);

        PlanWithParams planWithParams = createPlanWithParams(metadata, query, "_id");

        QueryOptions options = QueryOptions.builder()
                .sortByField("_id")
                .sortDirection(SortDirection.DESC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<Integer> actualSeqs = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                String json = TestUtil.bsonToJsonWithoutId(buffer);
                int seq = Integer.parseInt(json.replaceAll(".*\"seq\": (\\d+).*", "$1"));
                actualSeqs.add(seq);
            }
        }

        // Should include doc2 and return: 4, 3, 2
        List<Integer> expectedSeqs = List.of(4, 3, 2);
        assertEquals(expectedSeqs, actualSeqs, "Results should be sorted by _id DESC including boundary");
    }

    @Test
    void shouldSortByIdDescWithLtOperator() {
        // Behavior: Query with _id < X and SORTBY _id DESC returns documents less than X in descending order.
        final String BUCKET = "test-id-lt-desc";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc1', 'seq': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc2', 'seq': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc3', 'seq': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc4', 'seq': 4}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'doc5', 'seq': 5}")
        );
        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(BUCKET, documents);

        // Query: _id < last document's versionstamp
        String lastId = objectIds.get(4).toHexString();
        String query = String.format("{'_id': {'$lt': '%s'}}", lastId);

        PlanWithParams planWithParams = createPlanWithParams(metadata, query, "_id");

        QueryOptions options = QueryOptions.builder()
                .sortByField("_id")
                .sortDirection(SortDirection.DESC)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<Integer> actualSeqs = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                String json = TestUtil.bsonToJsonWithoutId(buffer);
                int seq = Integer.parseInt(json.replaceAll(".*\"seq\": (\\d+).*", "$1"));
                actualSeqs.add(seq);
            }
        }

        // Should return: 4, 3, 2, 1 (excluding doc5)
        List<Integer> expectedSeqs = List.of(4, 3, 2, 1);
        assertEquals(expectedSeqs, actualSeqs, "Results should be sorted by _id DESC");
    }

    @Test
    void shouldSortByIdAscWithGtOperatorAndSmallLimit() {
        // Behavior: Query with _id > X, SORTBY _id ASC with small limit works correctly.
        // This is a control test to verify ASC still works after the DESC fix.
        final String BUCKET = "test-id-gt-asc-pagination";

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{'name': 'doc%d', 'seq': %d}", i, i)));
        }
        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(BUCKET, documents);

        String firstId = objectIds.get(0).toHexString();
        String query = String.format("{'_id': {'$gt': '%s'}}", firstId);

        PlanWithParams planWithParams = createPlanWithParams(metadata, query, "_id");

        // Use limit=3 with ASC
        QueryOptions options = QueryOptions.builder()
                .sortByField("_id")
                .sortDirection(SortDirection.ASC)
                .limit(3)
                .build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<Integer> actualSeqs = new ArrayList<>();
        try (Transaction tr = createTransaction()) {
            List<ByteBuffer> results = readExecutor.execute(tr, ctx);
            for (ByteBuffer buffer : results) {
                String json = TestUtil.bsonToJsonWithoutId(buffer);
                int seq = Integer.parseInt(json.replaceAll(".*\"seq\": (\\d+).*", "$1"));
                actualSeqs.add(seq);
            }
        }

        // With limit=3 and ASC, should return first 3 after doc1: 2, 3, 4
        List<Integer> expectedSeqs = List.of(2, 3, 4);
        assertEquals(expectedSeqs, actualSeqs, "Results should be first 3 by _id ASC");
    }
}
