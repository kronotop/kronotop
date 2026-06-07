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

import com.kronotop.volume.EntryMetadata;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class VectorSearchSessionTest {
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
    void shouldReturnEmptyListWhenNoEntries() throws IOException {
        // Behavior: A session created with an empty entries list returns an empty result on search.
        try (VectorSearchSession session = new VectorSearchSession(List.of())) {
            List<MergedNodeScore> results = session.search(3, 0.0f, 1.0f);
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void shouldReturnTopKFromSingleIndex() throws IOException {
        // Behavior: A session with a single index returns the top-K results in descending score order.
        try (OnHeapVectorGraphIndex index = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(4), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(5), new float[]{0.5f, 0.5f, 0.0f}, executor).join();

            SearchHandle handle = index.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});
            try (VectorSearchSession session = new VectorSearchSession(List.of(handle))) {
                List<MergedNodeScore> results = session.search(3, 0.0f, 1.0f);
                assertEquals(3, results.size());
                for (int i = 0; i < results.size() - 1; i++) {
                    assertTrue(results.get(i).score() >= results.get(i + 1).score(),
                            "Expected descending order at index " + i);
                }
            }
        }
    }

    @Test
    void shouldMergeResultsFromMultipleIndexes() throws IOException {
        // Behavior: A session with two indexes merges results into a single top-K list in descending order.
        try (OnHeapVectorGraphIndex indexA = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
            indexA.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            indexA.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

            try (OnHeapVectorGraphIndex indexB = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
                indexB.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
                indexB.addGraphNode(new ObjectId(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

                SearchHandle handleA = indexA.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});
                SearchHandle handleB = indexB.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});

                try (VectorSearchSession session = new VectorSearchSession(List.of(handleA, handleB))) {
                    List<MergedNodeScore> results = session.search(3, 0.0f, 1.0f);
                    assertEquals(3, results.size());
                    for (int i = 0; i < results.size() - 1; i++) {
                        assertTrue(results.get(i).score() >= results.get(i + 1).score(),
                                "Expected descending order at index " + i);
                    }
                }
            }
        }
    }

    @Test
    void shouldResumeOnSubsequentCalls() throws IOException {
        // Behavior: The first call to search() performs a fresh search; the second call resumes without error.
        try (OnHeapVectorGraphIndex index = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(4), new float[]{0.5f, 0.5f, 0.0f}, executor).join();

            SearchHandle handle = index.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});
            try (VectorSearchSession session = new VectorSearchSession(List.of(handle))) {
                List<MergedNodeScore> first = session.search(2, 0.0f, 1.0f);
                assertFalse(first.isEmpty());

                // Second call should use resume() path — no exception thrown
                List<MergedNodeScore> second = session.search(2, 0.0f, 1.0f);
                assertNotNull(second);
            }
        }
    }

    @Test
    void shouldSkipNullMetadataEntries() throws IOException {
        // Behavior: Nodes whose metadata has been removed (e.g., deleted) are skipped in results.
        try (OnHeapVectorGraphIndex index = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {

            ObjectId oid1 = new ObjectId();
            ObjectId oid2 = new ObjectId();
            index.addGraphNode(oid1, 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            index.addGraphNode(oid2, 0, newEntryMetadata(2), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

            // Remove metadata for oid1 (ordinal 0), simulating a delete
            index.markNodeDeleted(oid1, 0);

            SearchHandle handle = index.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});
            try (VectorSearchSession session = new VectorSearchSession(List.of(handle))) {
                List<MergedNodeScore> results = session.search(3, 0.0f, 1.0f);
                // oid1's node should be absent since its metadata was removed
                for (MergedNodeScore r : results) {
                    assertNotEquals(oid1, r.location().objectId());
                }
            }
        }
    }

    @Test
    void shouldCloseAllSearchHandles() throws IOException {
        // Behavior: close() on the session closes all underlying search handles without throwing.
        try (OnHeapVectorGraphIndex indexA = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
            indexA.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

            try (OnHeapVectorGraphIndex indexB = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
                indexB.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

                SearchHandle handleA = indexA.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});
                SearchHandle handleB = indexB.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});

                try (VectorSearchSession session = new VectorSearchSession(List.of(handleA, handleB))) {
                    assertDoesNotThrow(session::close);
                }
            }
        }
    }
}
