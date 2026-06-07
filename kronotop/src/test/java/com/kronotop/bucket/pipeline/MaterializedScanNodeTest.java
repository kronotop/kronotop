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
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.*;
import com.kronotop.volume.EntryMetadata;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MaterializedScanNodeTest extends BasePipelineTest {

    private List<DocumentLocation> buildDocumentLocations(BucketMetadata metadata, List<ObjectId> objectIds) {
        List<DocumentLocation> locations = new ArrayList<>();
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        assertNotNull(primaryIndex, "Primary index should exist");

        try (Transaction tr = createTransaction()) {
            for (ObjectId objectId : objectIds) {
                byte[] indexKey = primaryIndex.subspace().pack(
                        Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectId.toByteArray())
                );
                byte[] indexEntryBytes = tr.get(indexKey).join();
                assertNotNull(indexEntryBytes, "Index entry should exist for " + objectId);

                IndexEntry indexEntry = IndexEntry.decode(indexEntryBytes);
                EntryMetadata entryMetadata = EntryMetadata.decode(indexEntry.entryMetadata());
                locations.add(new DocumentLocation(objectId, indexEntry.shardId(), entryMetadata));
            }
        }
        return locations;
    }

    private MaterializedPlan buildMaterializedPlan(
            BucketMetadata metadata, String filter, CandidateSupplier supplier) {
        byte[] filterBytes = filter.getBytes(StandardCharsets.UTF_8);
        return MaterializedPlanBuilder.build(
                context, metadata, bucketService.getPlanner(),
                filterBytes, supplier, planCacheEnabled, planCacheMaxTtl);
    }

    private List<ByteBuffer> executeMaterializedPlan(
            MaterializedPlan materializedPlan,
            BucketMetadata metadata,
            QueryOptions options) {
        QueryContext ctx = new QueryContext(
                getSession(), metadata, options,
                materializedPlan.plan(), materializedPlan.parameters());
        try (Transaction tr = createTransaction()) {
            return readExecutor.execute(tr, ctx);
        }
    }

    @Test
    void shouldReturnAllCandidatesWithEmptyFilter() {
        // Behavior: Empty filter {} with all docs as candidates returns all documents.
        final String BUCKET = "mat-scan-empty-filter";
        createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 35}")
        );
        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(BUCKET, documents);
        BucketMetadata metadata = getBucketMetadata(BUCKET);

        List<DocumentLocation> allLocations = buildDocumentLocations(metadata, objectIds);
        CandidateSupplier supplier = () -> {
            List<DocumentLocation> result = new ArrayList<>(allLocations);
            allLocations.clear();
            return result;
        };

        MaterializedPlan plan = buildMaterializedPlan(metadata, "{}", supplier);
        assertNotNull(plan);

        List<ByteBuffer> results = executeMaterializedPlan(plan, metadata, QueryOptions.builder().build());
        assertEquals(3, results.size());
        assertEquals(Set.of("Alice", "Bob", "Charlie"), extractNamesFromResults(results));
    }

    @Test
    void shouldFilterCandidatesWithEqPredicate() {
        // Behavior: $eq filter only returns candidates matching the predicate.
        final String BUCKET = "mat-scan-eq";
        createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 30}")
        );
        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(BUCKET, documents);
        BucketMetadata metadata = getBucketMetadata(BUCKET);

        List<DocumentLocation> allLocations = buildDocumentLocations(metadata, objectIds);
        CandidateSupplier supplier = () -> {
            List<DocumentLocation> result = new ArrayList<>(allLocations);
            allLocations.clear();
            return result;
        };

        MaterializedPlan plan = buildMaterializedPlan(
                metadata, "{'age': {'$eq': 30}}", supplier);
        assertNotNull(plan);

        List<ByteBuffer> results = executeMaterializedPlan(plan, metadata, QueryOptions.builder().build());
        assertEquals(2, results.size());
        assertEquals(Set.of("Alice", "Charlie"), extractNamesFromResults(results));
    }

    @Test
    void shouldFilterCandidatesWithGtPredicate() {
        // Behavior: $gt filter returns only candidates satisfying the comparison.
        final String BUCKET = "mat-scan-gt";
        createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 35}")
        );
        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(BUCKET, documents);
        BucketMetadata metadata = getBucketMetadata(BUCKET);

        List<DocumentLocation> allLocations = buildDocumentLocations(metadata, objectIds);
        CandidateSupplier supplier = () -> {
            List<DocumentLocation> result = new ArrayList<>(allLocations);
            allLocations.clear();
            return result;
        };

        MaterializedPlan plan = buildMaterializedPlan(
                metadata, "{'age': {'$gt': 22}}", supplier);
        assertNotNull(plan);

        List<ByteBuffer> results = executeMaterializedPlan(plan, metadata, QueryOptions.builder().build());
        assertEquals(2, results.size());
        assertEquals(Set.of("Bob", "Charlie"), extractNamesFromResults(results));
    }

    @Test
    void shouldFilterCandidatesWithCompoundPredicate() {
        // Behavior: AND compound filter ($gt + $eq on different fields) respects all conditions.
        final String BUCKET = "mat-scan-compound";
        createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 30, 'city': 'NYC'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 25, 'city': 'NYC'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 35, 'city': 'LA'}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'age': 28, 'city': 'NYC'}")
        );
        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(BUCKET, documents);
        BucketMetadata metadata = getBucketMetadata(BUCKET);

        List<DocumentLocation> allLocations = buildDocumentLocations(metadata, objectIds);
        CandidateSupplier supplier = () -> {
            List<DocumentLocation> result = new ArrayList<>(allLocations);
            allLocations.clear();
            return result;
        };

        MaterializedPlan plan = buildMaterializedPlan(
                metadata, "{'age': {'$gt': 26}, 'city': {'$eq': 'NYC'}}", supplier);
        assertNotNull(plan);

        List<ByteBuffer> results = executeMaterializedPlan(plan, metadata, QueryOptions.builder().build());
        assertEquals(2, results.size());
        assertEquals(Set.of("Alice", "Diana"), extractNamesFromResults(results));
    }

    @Test
    void shouldReturnEmptyForExhaustedSupplier() {
        // Behavior: Supplier returns empty immediately, so 0 results are produced.
        final String BUCKET = "mat-scan-exhausted";
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET);

        CandidateSupplier supplier = List::of;

        MaterializedPlan plan = buildMaterializedPlan(metadata, "{}", supplier);
        assertNotNull(plan);

        List<ByteBuffer> results = executeMaterializedPlan(plan, metadata, QueryOptions.builder().build());
        assertEquals(0, results.size());
    }

    @Test
    void shouldHandleMultipleBatches() {
        // Behavior: Supplier returns 2 docs per batch across 3 batches, all 6 are returned.
        final String BUCKET = "mat-scan-batches";
        createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'A', 'age': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B', 'age': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C', 'age': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'D', 'age': 4}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'E', 'age': 5}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'F', 'age': 6}")
        );
        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(BUCKET, documents);
        BucketMetadata metadata = getBucketMetadata(BUCKET);

        List<DocumentLocation> allLocations = buildDocumentLocations(metadata, objectIds);
        int batchSize = 2;
        AtomicInteger batchIndex = new AtomicInteger(0);
        CandidateSupplier supplier = () -> {
            int start = batchIndex.getAndAdd(batchSize);
            if (start >= allLocations.size()) return List.of();
            return allLocations.subList(start, Math.min(start + batchSize, allLocations.size()));
        };

        MaterializedPlan plan = buildMaterializedPlan(metadata, "{}", supplier);
        assertNotNull(plan);

        List<ByteBuffer> results = executeMaterializedPlan(plan, metadata, QueryOptions.builder().build());
        assertEquals(6, results.size());
        assertEquals(Set.of("A", "B", "C", "D", "E", "F"), extractNamesFromResults(results));
    }

    @Test
    void shouldRespectLimit() {
        // Behavior: QueryOptions.limit(2) with 5 candidates returns exactly 2 results.
        final String BUCKET = "mat-scan-limit";
        createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'A', 'age': 1}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B', 'age': 2}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C', 'age': 3}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'D', 'age': 4}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'E', 'age': 5}")
        );
        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(BUCKET, documents);
        BucketMetadata metadata = getBucketMetadata(BUCKET);

        List<DocumentLocation> allLocations = buildDocumentLocations(metadata, objectIds);
        CandidateSupplier supplier = () -> {
            List<DocumentLocation> result = new ArrayList<>(allLocations);
            allLocations.clear();
            return result;
        };

        MaterializedPlan plan = buildMaterializedPlan(metadata, "{}", supplier);
        assertNotNull(plan);

        List<ByteBuffer> results = executeMaterializedPlan(
                plan, metadata, QueryOptions.builder().limit(2).build());
        assertEquals(2, results.size());
    }

    @Test
    void shouldReturnNullForContradictoryFilter() {
        // Behavior: Contradictory filter causes MaterializedPlanBuilder.build() to return null.
        final String BUCKET = "mat-scan-contradictory";
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET);

        CandidateSupplier supplier = List::of;
        MaterializedPlan plan = buildMaterializedPlan(
                metadata, "{'$and': [{'age': {'$eq': 5}}, {'age': {'$eq': 10}}]}", supplier);
        assertNull(plan);
    }

    @Test
    void shouldFilterSubsetOfCandidates() {
        // Behavior: Only 3 of 5 docs provided as candidates + $gt filter returns only matching subset.
        final String BUCKET = "mat-scan-subset";
        createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Alice', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Bob', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Charlie', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Diana', 'age': 40}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'Eve', 'age': 50}")
        );
        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(BUCKET, documents);
        BucketMetadata metadata = getBucketMetadata(BUCKET);

        // Only provide candidates for Bob, Charlie, Diana (indices 1, 2, 3)
        List<DocumentLocation> allLocations = buildDocumentLocations(metadata, objectIds);
        List<DocumentLocation> subset = List.of(allLocations.get(1), allLocations.get(2), allLocations.get(3));
        AtomicInteger called = new AtomicInteger(0);
        CandidateSupplier supplier = () -> {
            if (called.getAndIncrement() == 0) return new ArrayList<>(subset);
            return List.of();
        };

        // Filter: age > 25 → from subset (Bob=20, Charlie=30, Diana=40) only Charlie and Diana match
        MaterializedPlan plan = buildMaterializedPlan(
                metadata, "{'age': {'$gt': 25}}", supplier);
        assertNotNull(plan);

        List<ByteBuffer> results = executeMaterializedPlan(plan, metadata, QueryOptions.builder().build());
        assertEquals(2, results.size());
        assertEquals(Set.of("Charlie", "Diana"), extractNamesFromResults(results));
    }

    @Test
    void shouldHandleBatchBoundaryWithFilter() {
        // Behavior: Filter eliminates some docs across batch boundaries, correct results returned.
        final String BUCKET = "mat-scan-batch-boundary";
        createIndexesAndLoadBucketMetadata(BUCKET);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{'name': 'A', 'age': 10}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'B', 'age': 20}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'C', 'age': 30}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'D', 'age': 40}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'E', 'age': 50}"),
                BSONUtil.jsonToDocumentThenBytes("{'name': 'F', 'age': 60}")
        );
        List<ObjectId> objectIds = insertDocumentsAndGetObjectIds(BUCKET, documents);
        BucketMetadata metadata = getBucketMetadata(BUCKET);

        List<DocumentLocation> allLocations = buildDocumentLocations(metadata, objectIds);
        int batchSize = 2;
        AtomicInteger batchIndex = new AtomicInteger(0);
        CandidateSupplier supplier = () -> {
            int start = batchIndex.getAndAdd(batchSize);
            if (start >= allLocations.size()) return List.of();
            return allLocations.subList(start, Math.min(start + batchSize, allLocations.size()));
        };

        // Filter: age > 25 → should return C(30), D(40), E(50), F(60) across batch boundaries
        MaterializedPlan plan = buildMaterializedPlan(
                metadata, "{'age': {'$gt': 25}}", supplier);
        assertNotNull(plan);

        List<ByteBuffer> results = executeMaterializedPlan(plan, metadata, QueryOptions.builder().build());
        assertEquals(4, results.size());
        assertEquals(Set.of("C", "D", "E", "F"), extractNamesFromResults(results));
    }
}
