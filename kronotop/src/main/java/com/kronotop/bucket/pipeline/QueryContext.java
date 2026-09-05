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

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.server.Session;
import org.bson.BsonDocument;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

/**
 * Holds the shared state for a single query execution: bucket metadata, query options,
 * the execution plan, per-node execution state, data sinks, and parameter values.
 *
 * <p>Used by a single query execution thread; the internal collections are not
 * thread-safe.
 *
 * @see QueryOptions
 * @see PipelineNode
 * @see PipelineExecutor
 */
public class QueryContext {
    /**
     * Default limit when none is specified. Zero means unlimited.
     */
    public static final int DEFAULT_LIMIT = 0;

    /**
     * Maximum allowed batch size.
     */
    public static final int MAXIMUM_BATCH = 10000;

    /**
     * Default batch size when none is specified.
     */
    public static final int DEFAULT_BATCH = 100;

    /**
     * Execution state keyed by pipeline node ID.
     */
    private final Map<Integer, ExecutionState> executionStates = new HashMap<>();

    private final Map<Integer, Integer> relations = new HashMap<>();

    /**
     * The execution plan as a tree of pipeline nodes.
     */
    private final PipelineNode plan;

    /**
     * Immutable query configuration options.
     */
    private final QueryOptions options;

    /**
     * Holds the data sinks used during query execution. Provides or creates a sink
     * per pipeline node for document locations or byte buffers.
     */
    private final DataSinkRegistry sinks = new DataSinkRegistry();

    /**
     * Parameter values extracted from the query for resolving Operand.Param references.
     */
    private final List<BqlValue> parameters;
    private final Session session;
    private BucketMetadata metadata;
    private int currentNodeId;

    /**
     * Pipeline environment providing access to services and utilities.
     * Set lazily during execution and cached for reuse.
     */
    private PipelineEnv env;

    /**
     * The field used by the driving index scan. Used to determine if in-memory sorting is needed
     * when the SORTBY field differs from the scanned field.
     */
    private String scannedIndexField;

    /**
     * Tracks whether any scan node used a multi-key index. Used to conditionally
     * enable ObjectId deduplication in executors.
     */
    private boolean scannedIndexIsMultiKey;

    /**
     * Raw query bytes for upsert document construction.
     * Stored when upsert is enabled to extract equality conditions.
     */
    private byte[] queryBytes;

    /**
     * Holds the result of an upsert operation for versionstamp resolution after commit.
     */
    private UpsertResult upsertResult;

    /**
     * Whether index scans should use snapshot isolation (no read conflict ranges).
     */
    private boolean snapshotRead;

    /**
     * Supplier for generating unique user versions within a transaction.
     */
    private IntSupplier userVersionSupplier;

    /**
     * Parsed projection specification for field-level projection.
     */
    private BsonDocument projectionSpec;

    /**
     * Parsed BQL expression, stored for positional {@code $} operator resolution in projection.
     */
    private BqlExpr parsedQuery;

    /**
     * The total number of documents returned to the client across all batches.
     * Compared against the LIMIT option to stop the query.
     */
    private int returnedCount;

    /**
     * Constructs a new QueryContext with parameters for parameterized query execution.
     *
     * @param session    the client session for registering post-commit hooks
     * @param metadata   the bucket metadata containing schema and configuration information
     * @param options    the query configuration options (batch, limit, sorting, etc.)
     * @param plan       the execution plan as a tree of pipeline nodes
     * @param parameters the parameter values for resolving Operand.Param references
     */
    public QueryContext(Session session, BucketMetadata metadata, QueryOptions options, PipelineNode plan, List<BqlValue> parameters) {
        this.session = session;
        this.metadata = metadata;
        this.plan = plan;
        this.options = options;
        this.parameters = parameters != null ? parameters : Collections.emptyList();
    }

    /**
     * Returns the execution state for the given pipeline node, creating it on first access.
     *
     * @param nodeId the pipeline node ID
     * @return the ExecutionState for the node (never null)
     */
    public ExecutionState getOrCreateExecutionState(int nodeId) {
        return executionStates.computeIfAbsent(nodeId, (ignored) -> new ExecutionState());
    }

    /**
     * Returns the bucket metadata (index definitions, schema, and configuration).
     *
     * @return the bucket metadata (never null)
     */
    public BucketMetadata metadata() {
        return metadata;
    }

    public void updateMetadata(BucketMetadata metadata) {
        this.metadata = metadata;
    }

    /**
     * Returns the pipeline environment, or null if it has not been set yet.
     * The environment is set lazily by the pipeline executor during execution.
     *
     * @return the pipeline environment, or null if not yet initialized
     */
    public PipelineEnv env() {
        return env;
    }

    public DataSinkRegistry sinks() {
        return sinks;
    }

    /**
     * Sets the pipeline environment if not already initialized.
     * Subsequent calls are ignored to ensure the environment is set once.
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
     *
     * @return the query options (never null)
     */
    public QueryOptions options() {
        return options;
    }

    /**
     * Returns the execution plan as a tree of pipeline nodes.
     *
     * @return the pipeline execution plan (never null)
     */
    public PipelineNode plan() {
        return plan;
    }

    public int getParentId(int childId) {
        return relations.get(childId);
    }

    public void setRelation(int childId, int parentId) {
        relations.put(childId, parentId);
    }

    public void setCurrentNodeId(int currentNodeId) {
        this.currentNodeId = currentNodeId;
    }

    public int currentNodeId() {
        return currentNodeId;
    }

    /**
     * Returns the session associated with this query context.
     */
    public Session getSession() {
        return session;
    }

    /**
     * Returns the field used by the driving index scan, or null if not set.
     */
    public String getScannedIndexField() {
        return scannedIndexField;
    }

    /**
     * Sets the field used by the driving index scan. Should only be set once
     * by the first executing scan node.
     */
    public void setScannedIndexField(String field) {
        if (this.scannedIndexField == null) {
            this.scannedIndexField = field;
        }
    }

    /**
     * Returns whether any scan node used a multi-key index.
     */
    public boolean isScannedIndexMultiKey() {
        return scannedIndexIsMultiKey;
    }

    /**
     * Sets the multi-key flag. Uses OR-latch semantics: once true, stays true.
     */
    public void setScannedIndexIsMultiKey(boolean multiKey) {
        if (multiKey) {
            this.scannedIndexIsMultiKey = true;
        }
    }

    /**
     * Returns the raw query bytes for upsert document construction.
     */
    public byte[] queryBytes() {
        return queryBytes;
    }

    /**
     * Sets the raw query bytes for upsert document construction.
     */
    public void setQueryBytes(byte[] queryBytes) {
        this.queryBytes = queryBytes;
    }

    /**
     * Returns the upsert result for versionstamp resolution after commit.
     */
    public UpsertResult upsertResult() {
        return upsertResult;
    }

    /**
     * Sets the upsert result for versionstamp resolution after commit.
     */
    public void setUpsertResult(UpsertResult upsertResult) {
        this.upsertResult = upsertResult;
    }

    /**
     * Returns the parameter values for resolving Operand.Param references during query execution.
     */
    public List<BqlValue> getParameters() {
        return parameters;
    }

    /**
     * Returns whether index scans should use snapshot isolation.
     */
    public boolean isSnapshotRead() {
        return snapshotRead;
    }

    /**
     * Sets whether index scans should use snapshot isolation.
     */
    public void setSnapshotRead(boolean snapshotRead) {
        this.snapshotRead = snapshotRead;
    }

    /**
     * Sets the supplier used to generate unique user versions within a transaction.
     */
    public void setUserVersionSupplier(IntSupplier userVersionSupplier) {
        this.userVersionSupplier = userVersionSupplier;
    }

    /**
     * Returns the next user version from the supplier.
     */
    public int getAndIncrementUserVersion() {
        return userVersionSupplier.getAsInt();
    }

    /**
     * Returns the parsed projection specification, or null if not specified.
     */
    public BsonDocument getProjectionSpec() {
        return projectionSpec;
    }

    /**
     * Sets the parsed projection specification.
     */
    public void setProjectionSpec(BsonDocument projectionSpec) {
        this.projectionSpec = projectionSpec;
    }

    /**
     * Returns the parsed BQL expression for positional {@code $} operator resolution.
     */
    public BqlExpr getParsedQuery() {
        return parsedQuery;
    }

    /**
     * Sets the parsed BQL expression.
     */
    public void setParsedQuery(BqlExpr parsedQuery) {
        this.parsedQuery = parsedQuery;
    }

    /**
     * Returns the total number of documents returned to the client so far.
     */
    public int getReturnedCount() {
        return returnedCount;
    }

    /**
     * Adds the number of documents returned in the latest batch to the running total.
     */
    public void addReturnedCount(int n) {
        this.returnedCount += n;
    }

    /**
     * Returns how many more documents can be returned before the LIMIT is reached.
     * Returns -1 when no limit is set.
     */
    public int remainingLimit() {
        int limit = options.limit();
        if (limit == DEFAULT_LIMIT) {
            return -1;
        }
        return Math.max(0, limit - returnedCount);
    }
}

