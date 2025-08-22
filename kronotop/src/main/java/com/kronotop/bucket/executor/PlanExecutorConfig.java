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

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PlannerContext;

/**
 * The Config class is responsible for storing and managing the configuration
 * of the plan executor, including both immutable and mutable settings.
 * <p>
 * This class provides flexibility by allowing modification of certain parameters, such as
 * the bucket batch size, to adjust the configuration dynamically as needed during execution.
 */
public class PlanExecutorConfig {
    private final BucketMetadata metadata;
    private final PhysicalNode plan;
    private final PlannerContext plannerContext;

    private final Cursor cursor = new Cursor();

    // Mutable fields
    private volatile boolean reverse;
    private volatile int limit;
    private volatile long readVersion;
    private volatile boolean pinReadVersion;
    private volatile String sortByField = DefaultIndexDefinition.ID.selector();

    public PlanExecutorConfig(BucketMetadata metadata, PhysicalNode plan, PlannerContext plannerContext) {
        this.metadata = metadata;
        this.plan = plan;
        this.plannerContext = plannerContext;
    }

    public BucketMetadata getMetadata() {
        return metadata;
    }

    public PhysicalNode getPlan() {
        return plan;
    }

    public PlannerContext getPlannerContext() {
        return plannerContext;
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

    public boolean isReverse() {
        return reverse;
    }

    public void setReverse(boolean reverse) {
        this.reverse = reverse;
    }

    public void setSortByField(String sortByField) {
        this.sortByField = sortByField;
    }

    public String getSortByField() {
        return sortByField;
    }
}
