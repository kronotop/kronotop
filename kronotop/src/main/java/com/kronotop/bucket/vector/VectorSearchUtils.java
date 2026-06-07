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

package com.kronotop.bucket.vector;

import com.kronotop.bucket.pipeline.DocumentLocation;
import io.github.jbellis.jvector.graph.SearchResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Shared utilities for merging and collecting vector search results across multiple indexes.
 */
final class VectorSearchUtils {

    private VectorSearchUtils() {
    }

    /**
     * Merges node scores into a bounded min-heap of top-K results.
     */
    static void mergeResults(SearchResult.NodeScore[] nodes,
                             VectorGraphIndexMetadata metadata,
                             PriorityQueue<MergedNodeScore> heap,
                             int topK) {
        for (SearchResult.NodeScore ns : nodes) {
            DocumentLocation loc = metadata.findDocumentLocation(ns.node);
            if (loc == null) continue;

            MergedNodeScore merged = new MergedNodeScore(ns.score, loc);
            if (heap.size() < topK) {
                heap.add(merged);
            } else {
                assert heap.peek() != null;
                if (ns.score > heap.peek().score()) {
                    heap.poll();
                    heap.add(merged);
                }
            }
        }
    }

    /**
     * Drains the min-heap and returns results in descending score order.
     */
    static List<MergedNodeScore> drainHeap(PriorityQueue<MergedNodeScore> heap) {
        List<MergedNodeScore> results = new ArrayList<>(heap.size());
        while (!heap.isEmpty()) {
            results.add(heap.poll());
        }
        Collections.reverse(results);
        return results;
    }
}
