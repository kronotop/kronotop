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

package com.kronotop.bucket;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.pipeline.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Unified facade for executing queries against a bucket.
 * <p>
 * QueryExecutor provides a single entry point for all query operations (read, delete, update)
 * by coordinating the pipeline execution infrastructure. It initializes shared components
 * like document retrieval and cursor management, then delegates to specialized executors
 * for each operation type.
 *
 * @see ReadExecutor for document retrieval
 * @see DeleteExecutor for document deletion
 * @see UpdateExecutor for document modification
 */
public class QueryExecutor {
    private final ReadExecutor readExecutor;
    private final DeleteExecutor deleteExecutor;
    private final UpdateExecutor updateExecutor;

    /**
     * Creates a new query executor for the specified bucket service.
     * <p>
     * Initializes the pipeline environment with document retrieval and cursor management,
     * then creates specialized executors for each operation type.
     *
     * @param service the bucket service providing storage and index access
     */
    public QueryExecutor(BucketService service) {
        DocumentRetriever documentRetriever = new DocumentRetriever(service);
        CursorManager cursorManager = new CursorManager();
        PipelineEnv env = new PipelineEnv(service, documentRetriever, cursorManager);
        PipelineExecutor executor = new PipelineExecutor(env);
        this.readExecutor = new ReadExecutor(executor);
        this.deleteExecutor = new DeleteExecutor(executor);
        this.updateExecutor = new UpdateExecutor(executor);
    }

    /**
     * Executes a read query and returns matching documents.
     *
     * @param tr  the FoundationDB transaction
     * @param ctx the query context containing the plan and options
     * @return map of versionstamps to document bodies for matching documents
     */
    public Map<Versionstamp, ByteBuffer> read(Transaction tr, QueryContext ctx) {
        return readExecutor.execute(tr, ctx);
    }

    /**
     * Executes a delete query and removes matching documents.
     *
     * @param tr  the FoundationDB transaction
     * @param ctx the query context containing the plan and options
     * @return list of versionstamps for deleted documents
     */
    public List<Versionstamp> delete(Transaction tr, QueryContext ctx) {
        return deleteExecutor.execute(tr, ctx);
    }

    /**
     * Executes an update query and modifies matching documents.
     *
     * @param tr  the FoundationDB transaction
     * @param ctx the query context containing the plan, options, and update specification
     * @return list of versionstamps for updated documents
     */
    public List<Versionstamp> update(Transaction tr, QueryContext ctx) {
        return updateExecutor.execute(tr, ctx);
    }
}
