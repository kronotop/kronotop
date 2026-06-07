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

import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Wraps multiple {@link GraphSearcher} instances (one per on-heap/on-disk index) and supports
 * resumable search across all of them. The first call uses {@code search()}, subsequent calls
 * use {@code resume()} to continue from where the previous search left off.
 */
public class VectorSearchSession implements Closeable {
    private final List<SearchHandle> entries;
    private final Set<Integer> exhaustedEntries = new HashSet<>();
    private PriorityQueue<MergedNodeScore> heap;
    private boolean firstSearch = true;

    public VectorSearchSession(List<SearchHandle> entries) {
        this.entries = entries;
    }

    /**
     * Searches all graph indexes for the top-K results, merging them with a min-heap.
     * The first call performs a fresh search; later calls resume from the prior state.
     */
    public List<MergedNodeScore> search(int topK, float threshold, float overquery) {
        if (entries.isEmpty()) {
            return List.of();
        }

        int rerankK = (int) (topK * overquery);

        if (heap == null) {
            heap = new PriorityQueue<>(topK + 1, Comparator.comparingDouble(MergedNodeScore::score));
        }

        for (int i = 0; i < entries.size(); i++) {
            if (exhaustedEntries.contains(i)) continue;

            SearchHandle entry = entries.get(i);
            try {
                SearchResult result;
                if (firstSearch) {
                    result = entry.searcher().search(entry.ssp(), topK, rerankK, threshold, 0.0f, Bits.ALL);
                } else {
                    result = entry.searcher().resume(topK, rerankK);
                }
                VectorSearchUtils.mergeResults(result.getNodes(), entry.metadata(), heap, topK);
            } catch (Exception e) {
                // Searcher failed (e.g., underlying index was flushed) — treat as exhausted.
                exhaustedEntries.add(i);
            }
        }

        firstSearch = false;

        return VectorSearchUtils.drainHeap(heap);
    }

    @Override
    public void close() throws IOException {
        IOException firstException = null;
        for (SearchHandle entry : entries) {
            try {
                entry.searcher().close();
            } catch (IOException e) {
                if (firstException == null) firstException = e;
                else firstException.addSuppressed(e);
            }
            if (entry.extraResource() != null) {
                try {
                    entry.extraResource().close();
                } catch (IOException e) {
                    if (firstException == null) firstException = e;
                    else firstException.addSuppressed(e);
                }
            }
        }
        if (firstException != null) throw firstException;
    }
}
