/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.apple.foundationdb.Transaction;

/**
 * Runs one database operation on the documents a query pipeline selects.
 *
 * <p>Permitted implementations:
 * <ul>
 *   <li>{@link ReadExecutor} - reads document content</li>
 *   <li>{@link DeleteExecutor} - removes documents</li>
 *   <li>{@link UpdateExecutor} - modifies documents</li>
 * </ul>
 *
 * <p>Executors run within a FoundationDB transaction.
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
     * Runs the pipeline to select documents and applies this executor's operation to them.
     *
     * <p>Implementations must clear their data sinks after processing, including on exceptions.
     *
     * @param tr  the FoundationDB transaction
     * @param ctx the query context holding the pipeline plan, metadata, and execution environment
     * @return the operation result
     * @throws RuntimeException if the operation fails
     */
    T execute(Transaction tr, QueryContext ctx);
}
