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

package com.kronotop.bucket.vector;

import com.kronotop.bucket.pipeline.DocumentLocation;
import com.kronotop.volume.EntryMetadata;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class VectorCandidateSupplierTest {
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @AfterEach
    void tearDown() {
        executor.close();
    }

    private EntryMetadata newEntryMetadata(long segment) {
        return new EntryMetadata(1L, new byte[8], 0L, segment, 1L);
    }

    @Test
    void shouldReturnCandidatesAndPopulateScoreMap() throws IOException {
        // Behavior: First, fetch returns document locations and populates the score map with corresponding scores.
        try (OnHeapVectorGraphIndex index = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
            ObjectId oid1 = new ObjectId();
            ObjectId oid2 = new ObjectId();
            ObjectId oid3 = new ObjectId();
            index.addGraphNode(oid1, 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            index.addGraphNode(oid2, 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
            index.addGraphNode(oid3, 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

            SearchHandle handle = index.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});
            VectorSearchSession session = new VectorSearchSession(List.of(handle));
            Map<ObjectId, Float> scoreMap = new HashMap<>();

            try (VectorCandidateSupplier supplier = new VectorCandidateSupplier(session, 3, 0.0f, scoreMap, Integer.MAX_VALUE, 1.0f)) {
                List<DocumentLocation> candidates = supplier.fetch();
                assertFalse(candidates.isEmpty());
                assertEquals(candidates.size(), scoreMap.size());
                for (DocumentLocation loc : candidates) {
                    assertTrue(scoreMap.containsKey(loc.objectId()));
                }
            }
        }
    }

    @Test
    void shouldDeduplicateAcrossFetches() throws IOException {
        // Behavior: Two consecutive fetches never return the same ObjectId twice.
        try (OnHeapVectorGraphIndex index = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

            SearchHandle handle = index.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});
            VectorSearchSession session = new VectorSearchSession(List.of(handle));
            Map<ObjectId, Float> scoreMap = new HashMap<>();

            try (VectorCandidateSupplier supplier = new VectorCandidateSupplier(session, 2, 0.0f, scoreMap, Integer.MAX_VALUE, 1.0f)) {
                List<DocumentLocation> first = supplier.fetch();
                List<DocumentLocation> second = supplier.fetch();

                Set<ObjectId> allIds = new HashSet<>();
                for (DocumentLocation loc : first) {
                    assertTrue(allIds.add(loc.objectId()), "Duplicate in first batch");
                }
                for (DocumentLocation loc : second) {
                    assertTrue(allIds.add(loc.objectId()), "Duplicate across batches");
                }
            }
        }
    }

    @Test
    void shouldBecomeExhaustedWhenFewerResultsThanBatchSize() throws IOException {
        // Behavior: When search returns fewer results than batchSize, subsequent fetches return empty.
        try (OnHeapVectorGraphIndex index = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

            SearchHandle handle = index.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});
            VectorSearchSession session = new VectorSearchSession(List.of(handle));
            Map<ObjectId, Float> scoreMap = new HashMap<>();

            try (VectorCandidateSupplier supplier = new VectorCandidateSupplier(session, 5, 0.0f, scoreMap, Integer.MAX_VALUE, 1.0f)) {
                List<DocumentLocation> first = supplier.fetch();
                // Only 1 result but batchSize is 5, so the supplier becomes exhausted
                assertTrue(first.size() < 5);

                List<DocumentLocation> second = supplier.fetch();
                assertTrue(second.isEmpty());
            }
        }
    }

    @Test
    void shouldBecomeExhaustedWhenMaxScanCandidatesReached() throws IOException {
        // Behavior: When seen candidates reach maxScanCandidates, the supplier becomes exhausted
        // even if the graph has more results available.
        try (OnHeapVectorGraphIndex index = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
            for (int i = 0; i < 10; i++) {
                index.addGraphNode(new ObjectId(), 0, newEntryMetadata(i + 1),
                        new float[]{0.1f * (i + 1), 0.2f * (i + 1), 0.3f * (i + 1)}, executor).join();
            }

            SearchHandle handle = index.openSearchHandle(new float[]{0.5f, 0.5f, 0.5f});
            VectorSearchSession session = new VectorSearchSession(List.of(handle));
            Map<ObjectId, Float> scoreMap = new HashMap<>();

            // maxScanCandidates=3, batchSize=3 → first fetch returns 3 candidates and hits the cap
            try (VectorCandidateSupplier supplier = new VectorCandidateSupplier(session, 3, 0.0f, scoreMap, 3, 1.0f)) {
                List<DocumentLocation> first = supplier.fetch();
                assertEquals(3, first.size());

                // Exhausted because seen.size() (3) >= maxScanCandidates (3)
                List<DocumentLocation> second = supplier.fetch();
                assertTrue(second.isEmpty());
            }
        }
    }

    @Test
    void shouldCloseUnderlyingSession() throws IOException {
        // Behavior: close() delegates to the underlying session without throwing.
        try (OnHeapVectorGraphIndex index = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

            SearchHandle handle = index.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});
            VectorSearchSession session = new VectorSearchSession(List.of(handle));
            Map<ObjectId, Float> scoreMap = new HashMap<>();

            VectorCandidateSupplier supplier = new VectorCandidateSupplier(session, 3, 0.0f, scoreMap, Integer.MAX_VALUE, 1.0f);
            assertDoesNotThrow(supplier::close);
        }
    }
}
