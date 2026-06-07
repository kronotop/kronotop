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

import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;

import java.io.Closeable;

/**
 * A handle that bundles the resources required to search a single vector graph index.
 * Opened via {@code openSearchHandle} and managed by {@link VectorSearchSession}.
 *
 * @param searcher      the graph searcher bound to a specific index
 * @param ssp           the score provider configured for the query vector
 * @param metadata      the index metadata used to resolve ordinals to document locations
 * @param extraResource an optional resource to close alongside the searcher (e.g., an on-disk view)
 */
public record SearchHandle(
        GraphSearcher searcher,
        SearchScoreProvider ssp,
        VectorGraphIndexMetadata metadata,
        Closeable extraResource
) {
}