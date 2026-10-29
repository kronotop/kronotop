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

import com.kronotop.TestUtil;
import com.kronotop.volume.EntryMetadata;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

    private OnDiskVectorGraphIndex flushAndOpen(Path tempDir, OnHeapVectorGraphIndex heapIndex) throws IOException {
        heapIndex.flush(tempDir);
        heapIndex.close();

        Path indexFile = Files.list(tempDir)
                .filter(p -> p.toString().endsWith(".index"))
                .findFirst()
                .orElseThrow(() -> new IOException("No .index file found after flush"));
        return new OnDiskVectorGraphIndex(null, indexFile, VectorSimilarityFunction.COSINE);
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
            index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
            index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(4), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(5), new float[]{0.5f, 0.5f, 0.0f}, executor).join();

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
            indexA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            indexA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

            try (OnHeapVectorGraphIndex indexB = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
                indexB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
                indexB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(4), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

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
            index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();
            index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(4), new float[]{0.5f, 0.5f, 0.0f}, executor).join();

            SearchHandle handle = index.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});
            try (VectorSearchSession session = new VectorSearchSession(List.of(handle))) {
                List<MergedNodeScore> first = session.search(2, 0.0f, 1.0f);
                assertFalse(first.isEmpty());

                // Second call should use the resume() path without an exception
                List<MergedNodeScore> second = session.search(2, 0.0f, 1.0f);
                assertNotNull(second);
            }
        }
    }

    @Test
    void shouldKeepDeletedOrdinalExcludedOnResume(@TempDir Path tempDir) throws IOException {
        // Behavior: An on-disk handle carries the live ordinals as acceptOrds. The first search and the resumed
        // search both skip the deleted ordinal.
        ObjectId oid1 = new ObjectId();
        ObjectId oid2 = new ObjectId();
        OnHeapVectorGraphIndex heapIndex = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE);
        heapIndex.addGraphNode(oid1, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        heapIndex.addGraphNode(oid2, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.8f, 0.6f, 0.0f}, executor).join();
        heapIndex.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

        try (OnDiskVectorGraphIndex index = flushAndOpen(tempDir, heapIndex)) {
            index.markNodeDeletedInMemory(index.getMetadata().findNodeRef(oid1).ordinal());

            SearchHandle handle = index.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});
            try (VectorSearchSession session = new VectorSearchSession(List.of(handle))) {
                List<MergedNodeScore> first = session.search(1, 0.0f, 1.0f);
                assertEquals(1, first.size());
                assertEquals(oid2, first.getFirst().location().objectId());

                List<MergedNodeScore> second = session.search(1, 0.0f, 1.0f);
                for (MergedNodeScore r : second) {
                    assertNotEquals(oid1, r.location().objectId());
                }
            }
        }
    }

    @Test
    void shouldExcludeDeletedNodeFromResults() throws IOException {
        // Behavior: A node removed with markNodeDeleted does not appear in the session results.
        try (OnHeapVectorGraphIndex index = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {

            ObjectId oid1 = new ObjectId();
            ObjectId oid2 = new ObjectId();
            index.addGraphNode(oid1, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            index.addGraphNode(oid2, TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.9f, 0.1f, 0.0f}, executor).join();
            index.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

            index.markNodeDeleted(oid1, TestUtil.generateVersionstamp(1));

            SearchHandle handle = index.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});
            try (VectorSearchSession session = new VectorSearchSession(List.of(handle))) {
                List<MergedNodeScore> results = session.search(3, 0.0f, 1.0f);
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
            indexA.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

            try (OnHeapVectorGraphIndex indexB = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE)) {
                indexB.addGraphNode(new ObjectId(), TestUtil.zeroVersionstamp(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

                SearchHandle handleA = indexA.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});
                SearchHandle handleB = indexB.openSearchHandle(new float[]{1.0f, 0.0f, 0.0f});

                try (VectorSearchSession session = new VectorSearchSession(List.of(handleA, handleB))) {
                    assertDoesNotThrow(session::close);
                }
            }
        }
    }
}
