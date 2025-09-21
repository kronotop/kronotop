package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.Transaction;

/**
 * Sealed interface defining the contract for pipeline execution operations in the Kronotop Cluster.
 *
 * <p>Executors are responsible for performing specific database operations (read, delete, update)
 * on documents identified through query pipeline processing. Each executor implementation handles
 * a different type of operation while following a common execution pattern:
 * <ol>
 *   <li>Execute the underlying pipeline to populate data sinks</li>
 *   <li>Process the populated sinks to extract relevant document information</li>
 *   <li>Perform the specific operation (read, delete, or update)</li>
 *   <li>Return the operation results</li>
 *   <li>Clean up resources</li>
 * </ol>
 *
 * <p>This sealed interface ensures type safety and exhaustive pattern matching by restricting
 * implementations to the three permitted subtypes:
 * <ul>
 *   <li>{@link ReadExecutor} - Retrieves document content</li>
 *   <li>{@link DeleteExecutor} - Removes documents from storage</li>
 *   <li>{@link UpdateExecutor} - Modifies existing documents</li>
 * </ul>
 *
 * <p>All executors operate within FoundationDB transactions to ensure ACID properties
 * and maintain data consistency across the distributed storage system.
 *
 * @param <T> the type of result returned by the executor operation
 * @see ReadExecutor
 * @see DeleteExecutor
 * @see UpdateExecutor
 * @see QueryContext
 * @see PipelineExecutor
 */
public sealed interface Executor<T> permits ReadExecutor, DeleteExecutor, UpdateExecutor {

    /**
     * Executes the specific operation defined by the implementing executor.
     *
     * <p>This method processes the query pipeline to identify target documents and performs
     * the executor's specific operation (read, delete, or update) on those documents.
     * The execution is performed within the provided FoundationDB transaction context
     * to ensure transactional consistency.
     *
     * <p>Implementations must ensure proper resource cleanup, particularly clearing
     * data sinks after processing, even in the case of exceptions.
     *
     * @param tr  the FoundationDB transaction context for the operation
     * @param ctx the query context containing the pipeline plan, metadata, and execution environment
     * @return the result of the executor operation, with type determined by the specific executor
     * @throws RuntimeException if the operation fails due to storage errors, invalid state,
     *                          or other execution issues
     */
    T execute(Transaction tr, QueryContext ctx);
}
