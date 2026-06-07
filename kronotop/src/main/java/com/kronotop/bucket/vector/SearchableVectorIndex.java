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
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;

import java.io.Closeable;

/**
 * Common search interface for on-heap and on-disk vector graph indexes.
 * Enables unified iteration over heterogeneous index types during search and lifecycle operations.
 */
public interface SearchableVectorIndex extends Closeable {

    /**
     * Opens a resumable search handle for the given query vector.
     * The caller is responsible for closing the returned handle.
     */
    SearchHandle openSearchHandle(float[] queryVector);

    /**
     * Searches for the top-K nearest neighbors with a minimum similarity threshold,
     * restricted to the accepted ordinals. Collects rerankK candidates before reranking to topK.
     */
    SearchResult search(float[] queryVector, int topK, int rerankK, float threshold, Bits acceptOrds);

    /**
     * Searches for the top-K nearest neighbors with rerankK equal to topK.
     */
    default SearchResult search(float[] queryVector, int topK, float threshold, Bits acceptOrds) {
        return search(queryVector, topK, topK, threshold, acceptOrds);
    }

    /**
     * Searches for the top-K nearest neighbors of the given query vector.
     */
    default SearchResult search(float[] queryVector, int topK) {
        return search(queryVector, topK, 0.0f, Bits.ALL);
    }

    /**
     * Searches for the top-K nearest neighbors, restricted to the accepted ordinals.
     */
    default SearchResult search(float[] queryVector, int topK, Bits acceptOrds) {
        return search(queryVector, topK, 0.0f, acceptOrds);
    }

    /**
     * Returns the metadata that maps ordinals to document locations.
     */
    VectorGraphIndexMetadata getMetadata();

    Versionstamp getLatestVersionstamp();

    /**
     * Returns the number of nodes in the graph at layer 0.
     */
    int size();

    /**
     * Returns whether this index is available for searching.
     * On-heap indexes are not searchable once flushed or when empty;
     * on-disk indexes are not searchable when empty.
     */
    boolean isSearchable();
}
