// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

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

    // Mutable fields
    private volatile boolean reverse;
    private volatile int limit;
    private volatile long readVersion;
    private volatile boolean pinReadVersion;
    private final Cursor cursor = new Cursor();

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
