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
 * Central coordination point for a single query execution in the pipeline system.
 * Holds bucket metadata, query options, the execution plan, per-node execution state,
 * data sinks, and post-commit hooks.
 *
 * <p>Designed to be used by a single query execution thread; all internal collections
 * are non-concurrent.
 *
 * @see QueryOptions
 * @see PipelineNode
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
     * A registry that holds and manages data sinks utilized during query execution.
     * The registry provides access to existing sinks related to specific pipeline nodes
     * or creates new sinks for handling byte buffers or document locations as needed.
     * <p>
     * This field is immutable after creation, ensuring the consistent state throughout
     * the lifecycle of the query context.
     * <p>
     * The {@link DataSinkRegistry} supports operations such as:
     * - Retrieving existing sinks for a specified node ID.
     * - Creating new sinks dynamically for document locations or byte buffers.
     * - Writing entries or locations to the appropriate sink type.
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
     * Constructs a new QueryContext with parameters for parameterized query execution.
     *
     * @param session    the client session for registering post-commit hooks
     * @param metadata   the bucket metadata containing schema and configuration information
     * @param options    the query configuration options (limits, sorting, etc.)
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
     * Retrieves or creates an execution state for the specified pipeline node.
     * Each pipeline node maintains its own execution state to track progress,
     * cursors, and other execution-specific information.
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

    public void updateMetadata(BucketMetadata metadata) {
        this.metadata = metadata;
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
}

