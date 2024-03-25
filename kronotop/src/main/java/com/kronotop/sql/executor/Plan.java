/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.sql.executor;

import com.kronotop.sql.optimizer.QueryOptimizationResult;

/**
 * The Plan class represents a query plan with caching functionality.
 * It stores a QueryOptimizationResult and tracks the last time it was accessed.
 */
public class Plan {
    private final QueryOptimizationResult result;
    private long lastAccessed;

    private void updateLastAccessed() {
        lastAccessed = System.currentTimeMillis();
    }

    public Plan(QueryOptimizationResult queryOptimizationResult) {
        this.result = queryOptimizationResult;
        updateLastAccessed();
    }

    public QueryOptimizationResult getQueryOptimizationResult() {
        updateLastAccessed();
        return result;
    }

    public long getLastAccessed() {
        return lastAccessed;
    }
}