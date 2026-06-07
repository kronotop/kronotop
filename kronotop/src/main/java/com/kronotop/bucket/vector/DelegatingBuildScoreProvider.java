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

import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;

/**
 * A BuildScoreProvider that delegates all calls to an underlying provider.
 * The delegate can be swapped at runtime to transition from exact to PQ-based scoring.
 */
public class DelegatingBuildScoreProvider implements BuildScoreProvider {
    private volatile BuildScoreProvider delegate;

    public DelegatingBuildScoreProvider(BuildScoreProvider delegate) {
        this.delegate = delegate;
    }

    public void setDelegate(BuildScoreProvider newDelegate) {
        this.delegate = newDelegate;
    }

    @Override
    public boolean isExact() {
        return delegate.isExact();
    }

    @Override
    public VectorFloat<?> approximateCentroid() {
        return delegate.approximateCentroid();
    }

    @Override
    public SearchScoreProvider searchProviderFor(VectorFloat<?> vector) {
        return delegate.searchProviderFor(vector);
    }

    @Override
    public SearchScoreProvider searchProviderFor(int node1) {
        return delegate.searchProviderFor(node1);
    }

    @Override
    public SearchScoreProvider diversityProviderFor(int node1) {
        return delegate.diversityProviderFor(node1);
    }
}
