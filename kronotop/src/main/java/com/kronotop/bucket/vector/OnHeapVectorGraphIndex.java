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

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.index.DistanceFunction;
import com.kronotop.volume.EntryMetadata;
import io.github.jbellis.jvector.disk.BufferedRandomAccessWriter;
import io.github.jbellis.jvector.graph.*;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.quantization.MutablePQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory vector graph index backed by JVector's GraphIndexBuilder for approximate nearest neighbor search.
 * Each instance corresponds to a single vector index within a bucket.
 */
public class OnHeapVectorGraphIndex implements SearchableVectorIndex {

    private static final Logger LOGGER = LoggerFactory.getLogger(OnHeapVectorGraphIndex.class);
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ObjectId id;
    private final AtomicInteger nextOrdinal = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, VectorFloat<?>> vectors = new ConcurrentHashMap<>();
    private final AtomicLong vectorBytesUsed = new AtomicLong(0);
    private final OnHeapVectorGraphIndexMetadata metadata = new OnHeapVectorGraphIndexMetadata();
    private final RandomAccessVectorValues ravv;
    private final GraphIndexBuilder builder;

    private final VectorSimilarityFunction similarityFunction;
    private final int pqTrainingThreshold;
    private final int pqSubspaceDivisor;
    private final DelegatingBuildScoreProvider delegatingBsp;
    private volatile boolean flushed;
    private volatile MutablePQVectors pqVectors;
    private volatile ProductQuantization pq;
    private volatile boolean pqTrained;

    public OnHeapVectorGraphIndex(int dimensions, VectorSimilarityFunction similarityFunction) {
        this(dimensions, similarityFunction, 0, 6);
    }

    public OnHeapVectorGraphIndex(
            int dimensions,
            VectorSimilarityFunction similarityFunction,
            int pqTrainingThreshold,
            int pqSubspaceDivisor
    ) {
        this.id = new ObjectId();
        this.similarityFunction = similarityFunction;
        this.pqTrainingThreshold = pqTrainingThreshold;
        this.pqSubspaceDivisor = pqSubspaceDivisor;

        this.ravv = new MapRandomAccessVectorValues(vectors, dimensions);
        BuildScoreProvider exactBsp = BuildScoreProvider.randomAccessScoreProvider(ravv, similarityFunction);
        this.delegatingBsp = new DelegatingBuildScoreProvider(exactBsp);
        this.builder = new GraphIndexBuilder(
                delegatingBsp,
                dimensions,
                16,     // M
                100,    // beamWidth
                1.2f,   // neighborOverflow
                1.4f,   // alpha
                true,   // addHierarchy
                false   // refineFinalGraph
        );
    }

    /**
     * Maps a Kronotop DistanceFunction to a JVector VectorSimilarityFunction.
     */
    public static VectorSimilarityFunction toSimilarityFunction(DistanceFunction distance) {
        return switch (distance) {
            case COSINE -> VectorSimilarityFunction.COSINE;
            case EUCLIDEAN -> VectorSimilarityFunction.EUCLIDEAN;
            case DOT_PRODUCT -> VectorSimilarityFunction.DOT_PRODUCT;
        };
    }

    public CompletableFuture<Void> addGraphNode(
            ObjectId objectId,
            int shardId,
            EntryMetadata entryMetadata,
            float[] vector,
            ExecutorService executor
    ) {
        final int[] ordinalHolder = {-1};
        return CompletableFuture.runAsync(() -> {
            rwLock.readLock().lock();
            try {
                if (flushed) {
                    throw new IllegalStateException("Cannot add nodes to a FLUSHED index");
                }
                int ordinal = nextOrdinal.getAndIncrement();
                ordinalHolder[0] = ordinal;
                VectorFloat<?> vectorFloat = vts.createFloatVector(vector);

                vectors.put(ordinal, vectorFloat);
                vectorBytesUsed.addAndGet(vectorFloat.ramBytesUsed());

                if (pqTrained) {
                    pqVectors.encodeAndSet(ordinal, vectorFloat);
                }

                resetEntryPointIfNoLiveNodes();
                builder.addGraphNode(ordinal, vectorFloat);

                if (!pqTrained && pqTrainingThreshold > 0 && nextOrdinal.get() >= pqTrainingThreshold) {
                    trainPQ();
                }
            } finally {
                rwLock.readLock().unlock();
            }
        }, executor).thenRun(() -> metadata.put(objectId, ordinalHolder[0], shardId, entryMetadata)).exceptionally((ex) -> {
            if (ordinalHolder[0] >= 0) {
                VectorFloat<?> removed = vectors.remove(ordinalHolder[0]);
                if (removed != null) {
                    vectorBytesUsed.addAndGet(-removed.ramBytesUsed());
                }
            }
            throw new CompletionException(ex);
        });
    }

    /**
     * Resets the graph's entry point to null when the entry node is tombstoned and no live
     * nodes remain. Neighbor candidates are filtered to live nodes during insertion, so adding
     * into an all-tombstoned graph would otherwise produce a node with no edges that stays
     * unreachable behind the dead entry point. A null entry makes the next insertion take the
     * empty-graph path and become the new entry.
     */
    private void resetEntryPointIfNoLiveNodes() {
        OnHeapGraphIndex graph = (OnHeapGraphIndex) builder.getGraph();
        ImmutableGraphIndex.NodeAtLevel entry = graph.entryNode();
        if (entry != null && graph.getDeletedNodes().get(entry.node) && !metadata.hasLiveMappings()) {
            graph.updateEntryNode(null);
        }
    }

    private synchronized void trainPQ() {
        if (pqTrained) return;
        int subspaces = Math.max(1, ravv.dimension() / pqSubspaceDivisor);
        int clusterCount = Math.min(256, ravv.size());

        boolean center = (similarityFunction == VectorSimilarityFunction.EUCLIDEAN);
        pq = ProductQuantization.compute(ravv, subspaces, clusterCount, center);

        pqVectors = new MutablePQVectors(pq);
        vectors.forEach((ord, vec) -> pqVectors.encodeAndSet(ord, vec));
        delegatingBsp.setDelegate(BuildScoreProvider.pqBuildScoreProvider(similarityFunction, pqVectors));
        pqTrained = true;

        LOGGER.debug("PQ training completed with {} subspaces for {} vectors", subspaces, vectors.size());
    }

    /**
     * Creates a GraphSearcher and SearchScoreProvider for resumable searching.
     * The caller is responsible for closing the returned SearchHandle.
     *
     * <p>The SearchHandle intentionally escapes the read lock. This is safe because JVector's
     * {@code ConcurrentGraphIndexView} (obtained via {@code getView()}) holds a consistency point:
     * open Views prevent {@code removeDeletedNodes()} from physically removing marked-deleted nodes.
     * The lock here only guards the {@code flushed} flag check.
     */
    public SearchHandle openSearchHandle(float[] queryVector) {
        rwLock.readLock().lock();
        try {
            if (flushed) {
                throw new IllegalStateException("Cannot search a FLUSHED index");
            }
            VectorFloat<?> query = vts.createFloatVector(queryVector);
            GraphSearcher searcher = new GraphSearcher(builder.getGraph());
            SearchScoreProvider ssp = DefaultSearchScoreProvider.exact(query, similarityFunction, ravv);
            return new SearchHandle(searcher, ssp, metadata, null);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Searches the graph for the top-K nearest neighbors with a minimum similarity threshold,
     * restricted to the given accepted ordinals. Collects rerankK candidates before reranking to topK.
     */
    public SearchResult search(float[] queryVector, int topK, int rerankK, float threshold, Bits acceptOrds) {
        rwLock.readLock().lock();
        try {
            if (flushed) {
                throw new IllegalStateException("Cannot search a FLUSHED index");
            }
            VectorFloat<?> query = vts.createFloatVector(queryVector);
            SearchScoreProvider scoreProvider = DefaultSearchScoreProvider.exact(query, similarityFunction, ravv);
            try (GraphSearcher searcher = new GraphSearcher(builder.getGraph())) {
                return searcher.search(scoreProvider, topK, rerankK, threshold, 0.0f, acceptOrds);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void markNodeDeleted(ObjectId objectId, int node) {
        rwLock.readLock().lock();
        try {
            builder.markNodeDeleted(node);
            metadata.addPendingDelete(node);
            metadata.removeMapping(objectId, node);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void advanceVersionstamp(Versionstamp versionstamp) {
        metadata.advanceVersionstamp(versionstamp);
    }

    public Versionstamp getFirstVersionstamp() {
        return metadata.getFirstVersionstamp();
    }

    public Versionstamp getLatestVersionstamp() {
        return metadata.getLatestVersionstamp();
    }

    public VectorGraphIndexMetadata getMetadata() {
        return metadata;
    }

    public int size() {
        return builder.getGraph().size(0);
    }

    /**
     * Estimates the total RAM usage of this index, including the graph structure and all stored vectors.
     */
    public long ramBytesUsed() {
        if (pqTrained) {
            return builder.getGraph().ramBytesUsed() + pqVectors.ramBytesUsed() + vectorBytesUsed.get();
        }
        return builder.getGraph().ramBytesUsed() + vectorBytesUsed.get();
    }

    public boolean isFlushed() {
        return flushed;
    }

    @Override
    public boolean isSearchable() {
        return !flushed && size() > 0;
    }

    @Override
    public void close() throws IOException {
        builder.close();
    }

    public ObjectId getId() {
        return id;
    }

    public VectorSimilarityFunction getSimilarityFunction() {
        return similarityFunction;
    }

    public boolean isPqTrained() {
        return pqTrained;
    }

    public MutablePQVectors getPqVectors() {
        return pqVectors;
    }

    public ProductQuantization getPq() {
        return pq;
    }

    /**
     * Removes deleted nodes, writes the graph and metadata to disk, and permanently marks this index as FLUSHED.
     * After flush, no further adds or searches are allowed.
     */
    public Path flush(Path path) throws IOException {
        rwLock.writeLock().lock();
        try {
            builder.cleanup();
            builder.removeDeletedNodes();
            metadata.clearPendingDeletes(ordinal -> {
                VectorFloat<?> removed = vectors.remove(ordinal);
                if (removed != null) {
                    vectorBytesUsed.addAndGet(-removed.ramBytesUsed());
                }
            });

            if (builder.getGraph().size(0) == 0) {
                flushed = true;
                return null;
            }

            String name = id.toHexString();
            Path indexPath = path.resolve(name + ".index");

            RandomAccessVectorValues rvv = new MapVectorValues(vectors, ravv.dimension());
            OnDiskGraphIndex.write(builder.getGraph(), rvv, indexPath);

            Path metadataPath = path.resolve(name + ".vmeta");
            OnDiskVectorGraphIndexMetadataWriter.write(metadataPath, metadata);

            if (pqVectors != null) {
                Path pqPath = path.resolve(name + ".pqv");
                try (var pqOut = new BufferedRandomAccessWriter(pqPath)) {
                    pqVectors.write(pqOut, OnDiskGraphIndex.CURRENT_VERSION);
                }
            }

            Path completePath = path.resolve(name + ".complete");
            Files.createFile(completePath);

            flushed = true;
            return indexPath;
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
