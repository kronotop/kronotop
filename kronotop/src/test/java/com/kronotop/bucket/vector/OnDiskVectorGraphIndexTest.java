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
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class OnDiskVectorGraphIndexTest {

    private static final int PQ_DIMENSIONS = 12;
    private static final int PQ_THRESHOLD = 50;
    private static final int PQ_DIVISOR = 3;
    private final VectorSimilarityFunction VSF = VectorSimilarityFunction.COSINE;

    private EntryMetadata newEntryMetadata(long segmentId) {
        return new EntryMetadata(segmentId, new byte[8], 0L, 100L, 1L);
    }

    private OnDiskVectorGraphIndex flushAndOpen(Path tempDir, OnHeapVectorGraphIndex heapIndex) throws IOException {
        heapIndex.flush(tempDir);
        heapIndex.close();

        Path indexFile = Files.list(tempDir)
                .filter(p -> p.toString().endsWith(".index"))
                .findFirst()
                .orElseThrow(() -> new IOException("No .index file found after flush"));
        return new OnDiskVectorGraphIndex(indexFile, VSF);
    }

    private float[] randomVector(Random rng) {
        float[] v = new float[PQ_DIMENSIONS];
        for (int i = 0; i < PQ_DIMENSIONS; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    void shouldSearchFlushedIndex(@TempDir Path tempDir) throws IOException {
        // Behavior: An on-disk index loaded from a flushed graph returns the nearest neighbor for a query vector.
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            OnHeapVectorGraphIndex heapIndex = new OnHeapVectorGraphIndex(3, VSF);
            heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
            heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

            try (OnDiskVectorGraphIndex diskIndex = flushAndOpen(tempDir, heapIndex)) {
                SearchResult result = diskIndex.search(new float[]{0.9f, 0.1f, 0.0f}, 1);
                assertEquals(1, result.getNodes().length);
                assertTrue(result.getNodes()[0].score > 0);
            }
        }
    }

    @Test
    void shouldReturnCorrectSize(@TempDir Path tempDir) throws IOException {
        // Behavior: The on-disk index size matches the number of vectors that were flushed.
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            OnHeapVectorGraphIndex heapIndex = new OnHeapVectorGraphIndex(3, VSF);
            heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
            heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

            try (OnDiskVectorGraphIndex diskIndex = flushAndOpen(tempDir, heapIndex)) {
                assertEquals(3, diskIndex.size());
            }
        }
    }

    @Test
    void shouldLookupMetadataForSearchResults(@TempDir Path tempDir) throws IOException {
        // Behavior: Search result nodes have valid metadata accessible via getMetadata().findEntryMetadata().
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            OnHeapVectorGraphIndex heapIndex = new OnHeapVectorGraphIndex(3, VSF);
            heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
            heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

            try (OnDiskVectorGraphIndex diskIndex = flushAndOpen(tempDir, heapIndex)) {
                SearchResult result = diskIndex.search(new float[]{1.0f, 0.0f, 0.0f}, 2);
                for (SearchResult.NodeScore ns : result.getNodes()) {
                    DocumentLocation loc = diskIndex.getMetadata().findDocumentLocation(ns.node);
                    assertNotNull(loc);
                    assertNotNull(loc.objectId());
                    assertEquals(0, loc.shardId());
                    assertNotNull(loc.entryMetadata());
                }
            }
        }
    }

    @Test
    void shouldSearchAfterFlushWithDeletedNodes(@TempDir Path tempDir) throws IOException {
        // Behavior: Deleted nodes are excluded during the flush operation, so the on-disk index has reduced size and search still works.
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            OnHeapVectorGraphIndex heapIndex = new OnHeapVectorGraphIndex(3, VSF);
            heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            ObjectId oid1 = new ObjectId();
            heapIndex.addGraphNode(oid1, 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
            heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

            heapIndex.markNodeDeleted(oid1, 1);

            try (OnDiskVectorGraphIndex diskIndex = flushAndOpen(tempDir, heapIndex)) {
                assertEquals(2, diskIndex.size());
                SearchResult result = diskIndex.search(new float[]{1.0f, 0.0f, 0.0f}, 2);
                assertTrue(result.getNodes().length > 0);
            }
        }
    }

    @Test
    void shouldSearchFlushedIndexWithPQ(@TempDir Path tempDir) throws IOException {
        // Behavior: An on-disk index loaded from a PQ-trained on-heap graph uses two-pass PQ scoring for search.
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            OnHeapVectorGraphIndex heapIndex = new OnHeapVectorGraphIndex(PQ_DIMENSIONS, VSF, PQ_THRESHOLD, PQ_DIVISOR);
            Random rng = new Random(42);
            float[] target = randomVector(rng);
            heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), target, executor).join();
            for (int i = 1; i < PQ_THRESHOLD; i++) {
                heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(i + 1), randomVector(rng), executor).join();
            }
            assertTrue(heapIndex.isPqTrained());

            try (OnDiskVectorGraphIndex diskIndex = flushAndOpen(tempDir, heapIndex)) {
                assertTrue(Files.list(tempDir).anyMatch(p -> p.toString().endsWith(".pqv")));

                SearchResult result = diskIndex.search(target, 1);
                assertEquals(1, result.getNodes().length);
                assertTrue(result.getNodes()[0].score > 0);

                DocumentLocation loc = diskIndex.getMetadata().findDocumentLocation(result.getNodes()[0].node);
                assertNotNull(loc);
            }
        }
    }

    @Test
    void shouldSearchWithPQViaSearchHandle(@TempDir Path tempDir) throws IOException {
        // Behavior: The PQ two-pass scoring path is exercised via openSearchHandle on an on-disk index.
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            OnHeapVectorGraphIndex heapIndex = new OnHeapVectorGraphIndex(PQ_DIMENSIONS, VSF, PQ_THRESHOLD, PQ_DIVISOR);
            Random rng = new Random(42);
            float[] target = randomVector(rng);
            heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(1), target, executor).join();
            for (int i = 1; i < PQ_THRESHOLD; i++) {
                heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(i + 1), randomVector(rng), executor).join();
            }

            try (OnDiskVectorGraphIndex diskIndex = flushAndOpen(tempDir, heapIndex)) {
                SearchHandle handle = diskIndex.openSearchHandle(target);
                try {
                    SearchResult result = handle.searcher().search(handle.ssp(), 1, Bits.ALL);
                    assertEquals(1, result.getNodes().length);
                    assertTrue(result.getNodes()[0].score > 0);
                } finally {
                    handle.searcher().close();
                    if (handle.extraResource() != null) {
                        handle.extraResource().close();
                    }
                }
            }
        }
    }

    @Test
    void shouldReturnCorrectSizeWithPQ(@TempDir Path tempDir) throws IOException {
        // Behavior: The on-disk index size matches the number of PQ-trained vectors that were flushed.
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            OnHeapVectorGraphIndex heapIndex = new OnHeapVectorGraphIndex(PQ_DIMENSIONS, VSF, PQ_THRESHOLD, PQ_DIVISOR);
            Random rng = new Random(42);
            int total = 60;
            for (int i = 0; i < total; i++) {
                heapIndex.addGraphNode(new ObjectId(), 0, newEntryMetadata(i + 1), randomVector(rng), executor).join();
            }
            assertTrue(heapIndex.isPqTrained());

            try (OnDiskVectorGraphIndex diskIndex = flushAndOpen(tempDir, heapIndex)) {
                assertEquals(total, diskIndex.size());
            }
        }
    }

    @Test
    void shouldExcludeDeletedNodeFromMetadataLookup(@TempDir Path tempDir) throws IOException {
        // Behavior: Marking a node as deleted on an on-disk index causes findEntryMetadata to return null
        // for that ordinal while other ordinals remain accessible.
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            ObjectId oid1 = new ObjectId();
            ObjectId oid2 = new ObjectId();
            ObjectId oid3 = new ObjectId();

            OnHeapVectorGraphIndex heapIndex = new OnHeapVectorGraphIndex(3, VSF);
            heapIndex.addGraphNode(oid1, 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            heapIndex.addGraphNode(oid2, 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
            heapIndex.addGraphNode(oid3, 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

            try (OnDiskVectorGraphIndex diskIndex = flushAndOpen(tempDir, heapIndex)) {
                int ordinal = diskIndex.getMetadata().findOrdinal(oid2);
                assertTrue(ordinal >= 0);

                diskIndex.markNodeDeleted(ordinal);

                assertNull(diskIndex.getMetadata().findDocumentLocation(ordinal));
                assertNotNull(diskIndex.getMetadata().findDocumentLocation(diskIndex.getMetadata().findOrdinal(oid1)));
                assertNotNull(diskIndex.getMetadata().findDocumentLocation(diskIndex.getMetadata().findOrdinal(oid3)));
            }
        }
    }

    @Test
    void shouldExcludeDeletedNodeFromSearchResults(@TempDir Path tempDir) throws IOException {
        // Behavior: After marking a node as deleted, search results that include its ordinal
        // yield null from findEntryMetadata, effectively filtering it from visible results.
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            ObjectId oid1 = new ObjectId();
            ObjectId oid2 = new ObjectId();
            ObjectId oid3 = new ObjectId();

            OnHeapVectorGraphIndex heapIndex = new OnHeapVectorGraphIndex(3, VSF);
            heapIndex.addGraphNode(oid1, 0, newEntryMetadata(1), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
            heapIndex.addGraphNode(oid2, 0, newEntryMetadata(2), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
            heapIndex.addGraphNode(oid3, 0, newEntryMetadata(3), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

            try (OnDiskVectorGraphIndex diskIndex = flushAndOpen(tempDir, heapIndex)) {
                // Before deletion: all search results have valid metadata
                float[] queryVector = {0.9f, 0.1f, 0.0f};
                SearchResult before = diskIndex.search(queryVector, 3);
                for (SearchResult.NodeScore ns : before.getNodes()) {
                    assertNotNull(diskIndex.getMetadata().findDocumentLocation(ns.node));
                }

                // Delete oid1's node
                int oid1Ordinal = diskIndex.getMetadata().findOrdinal(oid1);
                diskIndex.markNodeDeleted(oid1Ordinal);

                // After deletion: metadata lookup returns null for the deleted ordinal
                SearchResult after = diskIndex.search(queryVector, 3);
                int visibleCount = 0;
                for (SearchResult.NodeScore ns : after.getNodes()) {
                    DocumentLocation loc = diskIndex.getMetadata().findDocumentLocation(ns.node);
                    if (loc != null) {
                        visibleCount++;
                        assertNotEquals(oid1, loc.objectId());
                    }
                }
                assertEquals(2, visibleCount);
            }
        }
    }
}
