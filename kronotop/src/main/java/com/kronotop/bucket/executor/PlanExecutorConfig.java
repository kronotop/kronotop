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

package com.kronotop.bucket.executor;

/**
 * The PlanExecutorConfig class is responsible for storing and managing the configuration
 * of the plan executor, including both immutable and mutable settings.
 * <p>
 * The immutable configuration is encapsulated in the {@link PlanExecutorEnvironment}, which
 * contains static details about the environment in which the plan is executed.
 * <p>
 * This class provides flexibility by allowing modification of certain parameters, such as
 * the bucket batch size, to adjust the configuration dynamically as needed during execution.
 */
public class PlanExecutorConfig {
    // PlanExecutorEnvironment holds the immutable fields.
    private final PlanExecutorEnvironment environment;
    private final Cursor cursor = new Cursor();
    // Mutable fields
    private volatile boolean reverse;
    private volatile int limit;
    private volatile long readVersion;
    private volatile boolean pinReadVersion;

    public PlanExecutorConfig(PlanExecutorEnvironment environment) {
        this.environment = environment;
    }

    /**
     * Retrieves the immutable environment configuration used by the plan executor.
     *
     * @return the {@link PlanExecutorEnvironment} containing static details about the execution environment,
     * such as bucket, subspace, shard, and physical node configuration.
     */
    public PlanExecutorEnvironment environment() {
        return environment;
    }

    /**
     * Sets the limit value, which defines the maximum number of bucket entries
     * that can be processed during plan execution.
     *
     * @param limit the maximum number of bucket entries allowed for processing; must be a non-negative integer
     */
    public void setLimit(int limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be a non-negative integer");
        }
        this.limit = limit;
    }

    /**
     * Retrieves the current limit value, which determines the maximum number of
     * bucket entries that can be processed during plan execution.
     *
     * @return the current limit value as an integer; this defines the bucket batch size.
     */
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

    public Cursor cursor() {
        return cursor;
    }

    public boolean reverse() {
        return reverse;
    }

    public void setReverse(boolean reverse) {
        this.reverse = reverse;
    }
}
