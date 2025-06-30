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
    private volatile int bucketBatchSize;

    public PlanExecutorConfig(PlanExecutorEnvironment environment) {
        this.environment = environment;
    }

    /**
     * Retrieves the immutable environment configuration used by the plan executor.
     *
     * @return the {@link PlanExecutorEnvironment} containing static details about the execution environment,
     *         such as bucket, subspace, shard, and physical node configuration.
     */
    public PlanExecutorEnvironment environment() {
        return environment;
    }

    /**
     * Sets the bucket batch size, which determines the number of bucket entries to be processed in batch
     * operations during the plan execution.
     *
     * @param bucketBatchSize the desired batch size for processing bucket entries
     */
    public void setBucketBatchSize(int bucketBatchSize) {
        this.bucketBatchSize = bucketBatchSize;
    }

    /**
     * Retrieves the current bucket batch size configuration, which determines the number
     * of bucket entries to be processed during batch operations in plan execution.
     *
     * @return the configured bucket batch size
     */
    public int bucketBatchSize() {
        return bucketBatchSize;
    }
}
