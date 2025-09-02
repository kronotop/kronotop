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

import com.kronotop.bucket.BucketMetadata;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Context object that holds all necessary information and state for executing
 * a query in the pipeline system. This class manages query execution state,
 * configuration options, metadata access, and intermediate results.
 *
 * <p>QueryContext serves as the central coordination point during query execution,
 * providing access to:
 * <ul>
 *   <li>Bucket metadata and schema information</li>
 *   <li>Query configuration options (limits, sorting, etc.)</li>
 *   <li>Execution plan (PipelineNode tree)</li>
 *   <li>Per-node execution state management</li>
 *   <li>Pipeline environment and services</li>
 *   <li>Query output and intermediate results</li>
 * </ul>
 *
 * <h2>Thread Safety:</h2>
 * <p>QueryContext is designed to be used by a single query execution thread,
 * though it uses thread-safe collections for execution state management
 * to support concurrent access patterns within the pipeline.
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * // Create query plan
 * PipelineNode plan = createExecutionPlan(metadata, "{'age': {'$gt': 25}}");
 * 
 * // Configure query options
 * QueryOptions options = QueryOptions.builder()
 *     .limit(50)
 *     .reverse(true)
 *     .build();
 * 
 * // Create execution context
 * QueryContext context = new QueryContext(metadata, options, plan);
 * 
 * // Execute query
 * Map<Versionstamp, ByteBuffer> results = executor.execute(transaction, context);
 * }</pre>
 *
 * @since 1.0
 * @see QueryOptions
 * @see PipelineNode
 * @see BucketMetadata
 * @see PipelineExecutor
 */
public class QueryContext {
    /** 
     * Maximum allowed result limit for a single query batch.
     * This prevents excessive memory usage and ensures reasonable query performance.
     */
    public static final int MAXIMUM_LIMIT = 10000;
    
    /** 
     * Default result limit when no explicit limit is specified.
     * Provides a reasonable balance between performance and memory usage.
     */
    public static final int DEFAULT_LIMIT = 100;
    
    /** Bucket metadata containing schema information and configuration. */
    private final BucketMetadata metadata;
    
    /** Thread-safe map of the execution state keyed by pipeline node ID. */
    private final ConcurrentHashMap<Integer, ExecutionState> executionStates = new ConcurrentHashMap<>();
    
    /** The execution plan as a tree of pipeline nodes. */
    private final PipelineNode plan;
    
    /** Immutable query configuration options. */
    private final QueryOptions options;
    
    /** Output collector for query results and intermediate data. */
    private final Output output = new Output();

    /** 
     * Pipeline environment providing access to services and utilities.
     * Set lazily during execution and cached for reuse.
     */
    private volatile PipelineEnv env;

    /**
     * Constructs a new QueryContext with the specified metadata, options, and execution plan.
     * 
     * <p>The context is immutable once created, ensuring thread-safe access to
     * configuration during query execution.
     * 
     * @param metadata the bucket metadata containing schema and configuration information
     * @param options the query configuration options (limits, sorting, etc.)
     * @param plan the execution plan as a tree of pipeline nodes
     * @throws IllegalArgumentException if any parameter is null
     */
    public QueryContext(BucketMetadata metadata, QueryOptions options, PipelineNode plan) {
        this.metadata = metadata;
        this.plan = plan;
        this.options = options;
    }

    /**
     * Retrieves or creates an execution state for the specified pipeline node.
     * Each pipeline node maintains its own execution state to track progress,
     * cursors, and other execution-specific information.
     * 
     * <p>This method is thread-safe and will atomically create a new ExecutionState
     * if one doesn't already exist for the given node ID.
     * 
     * @param nodeId the unique identifier of the pipeline node
     * @return the ExecutionState for the specified node (never null)
     */
    public ExecutionState getOrCreateExecutionState(int nodeId) {
        return executionStates.computeIfAbsent(nodeId, (ignored) -> new ExecutionState());
    }

    /**
     * Returns the bucket metadata containing schema information and configuration.
     * The metadata provides access to index definitions, field types, and other
     * structural information about the target bucket.
     * 
     * @return the bucket metadata (never null)
     */
    public BucketMetadata metadata() {
        return metadata;
    }

    /**
     * Returns the pipeline environment providing access to services and utilities.
     * The environment is set lazily during query execution and provides access
     * to document retrieval services, cursor management, and other execution resources.
     * 
     * <p>This method may return null if called before the environment has been
     * initialized by the pipeline executor.
     * 
     * @return the pipeline environment, or null if not yet initialized
     */
    public PipelineEnv env() {
        return env;
    }

    /**
     * Returns the output collector for query results and intermediate data.
     * The output manages document collections, location mappings, and other
     * intermediate results produced during query execution.
     * 
     * @return the output collector (never null)
     */
    public Output output() {
        return output;
    }

    /**
     * Sets the pipeline environment if not already initialized.
     * This method implements lazy initialization with thread-safe semantics
     * to ensure the environment is set exactly once during query execution.
     * 
     * <p>Subsequent calls with different environments are ignored to maintain
     * consistency during query execution.
     * 
     * @param env the pipeline environment to set (must not be null)
     */
    public void setEnvironment(PipelineEnv env) {
        if (this.env == null) {
            this.env = env;
        }
    }

    /**
     * Returns the immutable query configuration options.
     * These options control query behavior such as result limits,
     * sort ordering, deletion flags, and field selection.
     * 
     * @return the query options (never null)
     */
    public QueryOptions options() {
        return options;
    }

    /**
     * Returns the execution plan as a tree of pipeline nodes.
     * The plan represents the optimized query execution strategy
     * including scan operations, filters, joins, and aggregations.
     * 
     * @return the pipeline execution plan (never null)
     */
    public PipelineNode plan() {
        return plan;
    }
}

