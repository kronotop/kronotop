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

import com.kronotop.bucket.index.DistanceFunction;
import com.kronotop.bucket.pipeline.DocumentLocation;
import com.kronotop.volume.EntryMetadata;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class OnHeapVectorGraphIndexTest {
    private OnHeapVectorGraphIndex graph;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        graph = new OnHeapVectorGraphIndex(3, VectorSimilarityFunction.COSINE);
        executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @AfterEach
    void tearDown() throws IOException {
        graph.close();
        executor.close();
    }

    private EntryMetadata newEntryMetadata() {
        return new EntryMetadata(1L, new byte[8], 0L, 100L, 1L);
    }

    @Test
    void shouldAddGraphNodeAndIncreaseSize() {
        // Behavior: Adding a single graph node increases the graph size to 1.
        ObjectId objectId = new ObjectId();
        graph.addGraphNode(objectId, 0, newEntryMetadata(), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

        assertEquals(1, graph.size());
    }

    @Test
    void shouldAddMultipleNodes() {
        // Behavior: Adding multiple graph nodes increases the graph size accordingly.
        for (int i = 0; i < 3; i++) {
            graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), new float[]{i + 1.0f, 0.0f, 0.0f}, executor).join();
        }

        assertEquals(3, graph.size());
    }

    @Test
    void shouldStoreMetadataOnAdd() {
        // Behavior: After adding a graph node, metadata maps the ObjectId to ordinal 0 and stores the DocumentLocation.
        ObjectId objectId = new ObjectId();
        EntryMetadata entryMetadata = newEntryMetadata();
        graph.addGraphNode(objectId, 0, entryMetadata, new float[]{1.0f, 0.0f, 0.0f}, executor).join();

        VectorGraphIndexMetadata metadata = graph.getMetadata();
        assertEquals(0, metadata.findOrdinal(objectId));
        DocumentLocation location = metadata.findDocumentLocation(0);
        assertNotNull(location);
        assertEquals(objectId, location.objectId());
        assertEquals(0, location.shardId());
        assertEquals(entryMetadata, location.entryMetadata());
    }

    @Test
    void shouldSearchNearestNeighbor() {
        // Behavior: Searching with a query vector close to one of the inserted vectors returns that vector as the nearest neighbor.
        graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        SearchResult result = graph.search(new float[]{0.9f, 0.1f, 0.0f}, 1);
        assertEquals(1, result.getNodes().length);
        assertEquals(0, result.getNodes()[0].node);
    }

    @Test
    void shouldConvertDistanceFunctions() {
        // Behavior: toSimilarityFunction correctly maps all DistanceFunction values to their JVector equivalents.
        assertEquals(VectorSimilarityFunction.COSINE, OnHeapVectorGraphIndex.toSimilarityFunction(DistanceFunction.COSINE));
        assertEquals(VectorSimilarityFunction.EUCLIDEAN, OnHeapVectorGraphIndex.toSimilarityFunction(DistanceFunction.EUCLIDEAN));
        assertEquals(VectorSimilarityFunction.DOT_PRODUCT, OnHeapVectorGraphIndex.toSimilarityFunction(DistanceFunction.DOT_PRODUCT));
    }

    @Test
    void shouldFlushGraphToDisk(@TempDir Path tempDir) throws IOException {
        // Behavior: flush writes the in-memory graph to disk and transitions the index to FLUSHED state.
        graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        graph.flush(tempDir);

        assertTrue(graph.isFlushed());
    }

    @Test
    void shouldRejectAddAfterFlush(@TempDir Path tempDir) throws IOException {
        // Behavior: Adding a node to a FLUSHED index throws IllegalStateException wrapped in CompletionException.
        graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

        graph.flush(tempDir);

        CompletionException ex = assertThrows(CompletionException.class, () ->
                graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), new float[]{0.0f, 1.0f, 0.0f}, executor).join()
        );
        // The exceptionally handler wraps the original exception in another CompletionException
        Throwable cause = ex.getCause();
        while (cause instanceof CompletionException) {
            cause = cause.getCause();
        }
        assertInstanceOf(IllegalStateException.class, cause);
    }

    @Test
    void shouldRejectSearchAfterFlush(@TempDir Path tempDir) throws IOException {
        // Behavior: Searching a FLUSHED index throws IllegalStateException.
        graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

        graph.flush(tempDir);

        assertThrows(IllegalStateException.class, () -> graph.search(new float[]{1.0f, 0.0f, 0.0f}, 1));
    }

    @Test
    void shouldFindReAddedNodeAfterDeletingSoleNode() {
        // Behavior: Deleting the only node in the graph and re-adding the same ObjectId with a new
        // vector (the delete-then-add sequence produced by an update) must leave the new node
        // reachable: metadata maps the ObjectId to the new ordinal and search returns the new node.
        ObjectId objectId = new ObjectId();
        graph.addGraphNode(objectId, 0, newEntryMetadata(), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

        graph.markNodeDeleted(objectId, 0);
        graph.addGraphNode(objectId, 0, newEntryMetadata(), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        // ObjectId-ordinal bookkeeping points at the re-added node
        assertEquals(1, graph.getMetadata().findOrdinal(objectId));
        DocumentLocation location = graph.getMetadata().findDocumentLocation(1);
        assertNotNull(location);
        assertEquals(objectId, location.objectId());

        SearchResult result = graph.search(new float[]{0.0f, 0.0f, 1.0f}, 1);
        assertEquals(1, result.getNodes().length);
        assertEquals(1, result.getNodes()[0].node);
    }

    @Test
    void shouldFindNewNodeAfterDeletingSoleNodeWithDifferentObjectId() {
        // Behavior: Deleting the only node in the graph and adding a different ObjectId afterward
        // must leave the new node reachable by search.
        ObjectId first = new ObjectId();
        graph.addGraphNode(first, 0, newEntryMetadata(), new float[]{1.0f, 0.0f, 0.0f}, executor).join();

        graph.markNodeDeleted(first, 0);
        ObjectId second = new ObjectId();
        graph.addGraphNode(second, 0, newEntryMetadata(), new float[]{0.0f, 1.0f, 0.0f}, executor).join();

        SearchResult result = graph.search(new float[]{0.0f, 1.0f, 0.0f}, 1);
        assertEquals(1, result.getNodes().length);
        assertEquals(1, result.getNodes()[0].node);
        assertEquals(second, graph.getMetadata().findDocumentLocation(1).objectId());
    }

    @Test
    void shouldFindNewNodeAfterDeletingAllNodes() {
        // Behavior: After every node in a multi-node graph is deleted, a newly added node must be
        // reachable by search even though the old entry point is tombstoned.
        ObjectId[] objectIds = {new ObjectId(), new ObjectId(), new ObjectId()};
        float[][] vectors = {{1.0f, 0.0f, 0.0f}, {0.0f, 1.0f, 0.0f}, {0.5f, 0.5f, 0.0f}};
        for (int i = 0; i < objectIds.length; i++) {
            graph.addGraphNode(objectIds[i], 0, newEntryMetadata(), vectors[i], executor).join();
        }
        for (int i = 0; i < objectIds.length; i++) {
            graph.markNodeDeleted(objectIds[i], i);
        }

        ObjectId fresh = new ObjectId();
        graph.addGraphNode(fresh, 0, newEntryMetadata(), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        SearchResult result = graph.search(new float[]{0.0f, 0.0f, 1.0f}, 1);
        assertEquals(1, result.getNodes().length);
        assertEquals(3, result.getNodes()[0].node);
        assertEquals(fresh, graph.getMetadata().findDocumentLocation(3).objectId());
    }

    @Test
    void shouldFlushWithDeletedNodes(@TempDir Path tempDir) throws IOException {
        // Behavior: flush removes deleted nodes before writing to disk and transitions the index to FLUSHED state.
        graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), new float[]{1.0f, 0.0f, 0.0f}, executor).join();
        ObjectId oid1 = new ObjectId();
        graph.addGraphNode(oid1, 0, newEntryMetadata(), new float[]{0.0f, 1.0f, 0.0f}, executor).join();
        graph.addGraphNode(new ObjectId(), 0, newEntryMetadata(), new float[]{0.0f, 0.0f, 1.0f}, executor).join();

        graph.markNodeDeleted(oid1, 1);

        graph.flush(tempDir);

        assertTrue(graph.isFlushed());
    }
}
