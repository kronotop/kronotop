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
import com.kronotop.KronotopException;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.ReaderSupplierFactory;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Disk-backed vector graph index that loads a flushed jvector graph and its metadata for search.
 * Thread-safe: each search call creates independent readers.
 */
public class OnDiskVectorGraphIndex implements SearchableVectorIndex {

    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private final ReaderSupplier readerSupplier;
    private final OnDiskGraphIndex graph;
    private final OnDiskVectorGraphIndexMetadata metadata;
    private final VectorSimilarityFunction similarityFunction;
    private final PQVectors pqVectors;

    public OnDiskVectorGraphIndex(Path indexPath, VectorSimilarityFunction similarityFunction) throws IOException {
        this.similarityFunction = similarityFunction;
        this.readerSupplier = ReaderSupplierFactory.open(indexPath);
        this.graph = OnDiskGraphIndex.load(readerSupplier);

        String fileName = indexPath.getFileName().toString();
        String baseName = fileName.substring(0, fileName.lastIndexOf('.'));
        Path metadataPath = indexPath.resolveSibling(baseName + ".vmeta");
        this.metadata = new OnDiskVectorGraphIndexMetadata(metadataPath);

        Path pqPath = indexPath.resolveSibling(baseName + ".pqv");
        if (Files.exists(pqPath)) {
            try (ReaderSupplier pqRs = ReaderSupplierFactory.open(pqPath);
                 RandomAccessReader reader = pqRs.get()) {
                this.pqVectors = PQVectors.load(reader);
            }
        } else {
            this.pqVectors = null;
        }
    }

    /**
     * Creates a GraphSearcher and SearchScoreProvider for resumable searching.
     * Uses PQ two-pass scoring when available, falling back to exact scoring.
     */
    public SearchHandle openSearchHandle(float[] queryVector) {
        VectorFloat<?> query = vts.createFloatVector(queryVector);
        var view = graph.getView();
        GraphSearcher searcher = new GraphSearcher(graph);
        SearchScoreProvider ssp;
        if (pqVectors != null) {
            var asf = pqVectors.precomputedScoreFunctionFor(query, similarityFunction);
            var reranker = view.rerankerFor(query, similarityFunction);
            ssp = new DefaultSearchScoreProvider(asf, reranker);
        } else {
            ssp = DefaultSearchScoreProvider.exact(query, similarityFunction, view);
        }
        return new SearchHandle(searcher, ssp, metadata, view);
    }

    /**
     * Searches the on-disk graph for the top-K nearest neighbors with a minimum similarity threshold,
     * restricted to the given accepted ordinals. Collects rerankK candidates before reranking to topK.
     * Uses PQ two-pass scoring when available, falling back to exact scoring.
     */
    public SearchResult search(float[] queryVector, int topK, int rerankK, float threshold, Bits acceptOrds) {
        VectorFloat<?> query = vts.createFloatVector(queryVector);
        try (var searcher = new GraphSearcher(graph);
             var view = graph.getView()) {
            SearchScoreProvider scoreProvider;
            if (pqVectors != null) {
                var asf = pqVectors.precomputedScoreFunctionFor(query, similarityFunction);
                var reranker = view.rerankerFor(query, similarityFunction);
                scoreProvider = new DefaultSearchScoreProvider(asf, reranker);
            } else {
                scoreProvider = DefaultSearchScoreProvider.exact(query, similarityFunction, view);
            }
            return searcher.search(scoreProvider, topK, rerankK, threshold, 0.0f, acceptOrds);
        } catch (Exception e) {
            throw new KronotopException(e);
        }
    }

    public Versionstamp getLatestVersionstamp() {
        return metadata.getLatestVersionstamp();
    }

    public VectorGraphIndexMetadata getMetadata() {
        return metadata;
    }

    /**
     * Marks a node as deleted in the on-disk metadata.
     */
    public void markNodeDeleted(int ordinal) {
        metadata.markDeleted(ordinal);
    }

    /**
     * Forces pending metadata changes to disk.
     */
    public void flushMetadata() {
        metadata.flush();
    }

    public int size() {
        return graph.size(0);
    }

    @Override
    public boolean isSearchable() {
        return size() > 0;
    }

    @Override
    public void close() throws IOException {
        metadata.close();
        graph.close();
        readerSupplier.close();
    }
}
