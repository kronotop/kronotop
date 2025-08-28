/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.pipeline;

import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.DefaultIndexDefinition;

import java.util.concurrent.ConcurrentHashMap;

public class PipelineContext {
    public static final int MAXIMUM_LIMIT = 10000;
    public static final int DEFAULT_LIMIT = 100;
    private final Context context;
    private final BucketMetadata metadata;
    private final ConcurrentHashMap<Integer, ExecutionState> executionStates = new ConcurrentHashMap<>();
    private final PipelineEnv env;
    // Output
    private final Output output = new Output();
    // Mutable fields
    private volatile boolean reverse;
    private volatile int limit = DEFAULT_LIMIT;
    private volatile long readVersion;
    private volatile boolean pinReadVersion;
    private volatile String sortByField = DefaultIndexDefinition.ID.selector();

    public PipelineContext(Context context, BucketMetadata metadata, PipelineEnv env) {
        this.context = context;
        this.metadata = metadata;
        this.env = env;
    }

    /**
     * Sets the limit value, which defines the maximum number of bucket entries
     * that can be processed during plan execution.
     *
     * @param limit the maximum number of bucket entries allowed for processing; must be a non-negative integer
     */
    public void setLimit(int limit) {
        if (limit >= MAXIMUM_LIMIT) {
            throw new IllegalArgumentException("Maximum limit value is " + MAXIMUM_LIMIT);
        }
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be a non-negative integer");
        }
        this.limit = limit;
    }

    public ExecutionState getOrCreateExecutionState(int nodeId) {
        return executionStates.computeIfAbsent(nodeId, (ignored) -> new ExecutionState());
    }

    public Context context() {
        return context;
    }

    public BucketMetadata getMetadata() {
        return metadata;
    }

    public int limit() {
        return limit;
    }

    public long readVersion() {
        return readVersion;
    }

    public void setReadVersion(long readVersion) {
        this.readVersion = readVersion;
    }

    public void setPinReadVersion(boolean pinReadVersion) {
        this.pinReadVersion = pinReadVersion;
    }

    public boolean pinReadVersion() {
        return pinReadVersion;
    }

    public boolean isReverse() {
        return reverse;
    }

    public void setReverse(boolean reverse) {
        this.reverse = reverse;
    }

    public String getSortByField() {
        return sortByField;
    }

    public void setSortByField(String sortByField) {
        this.sortByField = sortByField;
    }

    public PipelineEnv env() {
        return env;
    }

    public Output output() {
        return output;
    }
}

