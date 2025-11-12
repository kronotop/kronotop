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


package com.kronotop.bucket.index;

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.*;
import com.kronotop.bucket.index.maintenance.*;
import com.kronotop.bucket.index.statistics.IndexAnalyzeTask;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utility class for managing index lifecycle and operations in FoundationDB.
 *
 * <p>Provides comprehensive index management including creation, deletion, status updates,
 * statistics tracking, and background task orchestration. All index metadata is stored in
 * FoundationDB directory subspaces under the bucket metadata structure.
 *
 * <p><strong>Core Capabilities:</strong>
 * <ul>
 *   <li>Index creation with automatic background building task creation</li>
 *   <li>Index deletion with asynchronous cleanup coordination</li>
 *   <li>Index status transitions (WAITING → BUILDING → READY → DROPPED)</li>
 *   <li>Cardinality tracking for query optimization</li>
 *   <li>Statistical analysis task coordination</li>
 *   <li>Building task orchestration across shards</li>
 * </ul>
 *
 * <p><strong>Directory Structure:</strong>
 * <pre>
 * bucketMetadataSubspace/
 *   indexes/
 *     {indexName}/
 *       INDEX_DEFINITION → IndexDefinition JSON
 *       TASKS/{versionstamp} → back pointers to maintenance tasks
 *       {indexData} → actual index entries
 *   HEADER/
 *     INDEX_STATISTICS/
 *       {indexId}/
 *         CARDINALITY → long value
 *         HISTOGRAM → histogram data
 * </pre>
 *
 * <p><strong>Task Coordination:</strong> Uses random shard selection for single-point
 * coordination of boundary detection, drop, and analyze operations. Building tasks are
 * created for all shards with identical boundaries.
 *
 * <p><strong>Version Management:</strong> All index operations increment bucket metadata
 * version to invalidate caches and trigger metadata convergence across the cluster.
 *
 * @see IndexDefinition
 * @see BucketMetadata
 * @see IndexBoundaryTask
 * @see IndexBuildingTask
 * @see IndexDropTask
 */
public class IndexUtil {
    public static final byte[] NULL_BYTES = new byte[]{};
    public static final byte[] POSITIVE_DELTA_ONE = new byte[]{1, 0, 0, 0, 0, 0, 0, 0}; // 1L, little-endian
    public static final byte[] NEGATIVE_DELTA_ONE = new byte[]{-1, -1, -1, -1, -1, -1, -1, -1}; // -1L, little-endian

    private static final Random random = new Random(System.nanoTime());

    /**
     * Creates index directory subspace and persists index definition.
     *
     * <p>Low-level helper that creates the index subspace under bucket metadata, saves
     * the definition, and increments bucket version. Used internally by the public create
     * method and for primary index creation.
     *
     * <p>Directory path: {@code bucketMetadataSubspace/indexes/{indexName}/}
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace as base path
     * @param definition             index definition to persist
     * @return newly created index directory subspace
     * @throws KronotopException   if index name already exists
     * @throws CompletionException on directory creation errors
     */
    public static DirectorySubspace create(Transaction tr, DirectorySubspace bucketMetadataSubspace, IndexDefinition definition) {
        List<String> subpath = new ArrayList<>(bucketMetadataSubspace.getPath());
        subpath.add(BucketMetadataUtil.INDEXES_DIRECTORY);
        subpath.add(definition.name());
        try {
            DirectorySubspace indexSubspace = DirectoryLayer.getDefault().create(tr, subpath).join();
            saveIndexDefinition(tr, definition, indexSubspace);
            BucketMetadataUtil.increaseVersion(tr, bucketMetadataSubspace, POSITIVE_DELTA_ONE);
            return indexSubspace;
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new KronotopException("'" + definition.name() + "' has already exist");
            }
            throw e;
        }
    }

    /**
     * Creates an index and initiates background building if needed.
     *
     * <p>Primary entry point for index creation. Creates index subspace and definition,
     * then coordinates background building for secondary indexes.
     *
     * <p><strong>Primary Index:</strong> Returns immediately after creating subspace.
     * Primary index is built inline during document insertion.
     *
     * <p><strong>Secondary Index:</strong> Creates {@link IndexBoundaryTask} to:
     * <ol>
     *   <li>Wait for metadata convergence across cluster</li>
     *   <li>Determine scan boundaries from primary index</li>
     *   <li>Create {@link IndexBuildingTask} for each shard</li>
     *   <li>Coordinate parallel background building</li>
     * </ol>
     *
     * <p><strong>Random Shard Selection:</strong> Boundary task assigned to random shard
     * for load distribution and single-point coordination.
     *
     * <p><strong>Back Pointer:</strong> Creates versionstamped key in index subspace
     * linking to boundary task for tracking and inspection.
     *
     * @param tx         transactional context with transaction and application context
     * @param namespace  bucket namespace
     * @param bucket     bucket name
     * @param definition index definition with name, selector, and BSON type
     * @return newly created index directory subspace
     * @throws KronotopException if index name already exists
     * @see IndexBoundaryRoutine
     * @see IndexBuildingRoutine
     */
    public static DirectorySubspace create(TransactionalContext tx,
                                           String namespace,
                                           String bucket,
                                           IndexDefinition definition) {

        BucketMetadata bucketMetadata = BucketMetadataUtil.open(tx.context(), tx.tr(), namespace, bucket);
        DirectorySubspace indexSubspace = create(tx.tr(), bucketMetadata.subspace(), definition);

        if (definition.id() == DefaultIndexDefinition.ID.id()) {
            // Primary index built inline, no background task needed
            return indexSubspace;
        }

        BucketService service = tx.context().getService(BucketService.NAME);
        int shardId = random.nextInt(service.getNumberOfShards());

        IndexBoundaryTask task = new IndexBoundaryTask(namespace, bucket, definition.id(), shardId);
        byte[] encoded = JSONUtil.writeValueAsBytes(task);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);
        int userVersion = tx.getAndIncreaseUserVersion();
        TaskStorage.create(tx.tr(), userVersion, taskSubspace, encoded);
        IndexTaskUtil.setBackPointer(tx.tr(), indexSubspace, userVersion, shardId);

        BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, bucketMetadata);

        return indexSubspace;
    }

    /**
     * Opens existing index directory subspace by name.
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace as base path
     * @param name                   index name
     * @return index directory subspace
     * @throws NoSuchIndexException if index does not exist
     * @throws CompletionException  on directory operation errors
     */
    public static DirectorySubspace open(Transaction tr, DirectorySubspace bucketMetadataSubspace, String name) {
        List<String> subpath = new ArrayList<>(bucketMetadataSubspace.getPath());
        subpath.add(BucketMetadataUtil.INDEXES_DIRECTORY);
        subpath.add(name);
        try {
            return DirectoryLayer.getDefault().open(tr, subpath).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchIndexException(name);
            }
            throw e;
        }
    }

    /**
     * Loads index definition from index subspace.
     *
     * <p>Retrieves and deserializes INDEX_DEFINITION key from subspace.
     *
     * @param tr            transaction for database operations
     * @param indexSubspace index directory subspace
     * @return deserialized index definition
     */
    public static IndexDefinition loadIndexDefinition(Transaction tr, DirectorySubspace indexSubspace) {
        byte[] indexDefinitionKey = indexSubspace.pack(BucketMetadataMagic.INDEX_DEFINITION.getValue());
        byte[] value = tr.get(indexDefinitionKey).join();
        return JSONUtil.readValue(value, IndexDefinition.class);
    }

    /**
     * Persists index definition to index subspace.
     *
     * <p>Internal helper for saving definition during creation and updates.
     *
     * @param tr            transaction for database operations
     * @param definition    index definition to persist
     * @param indexSubspace index directory subspace
     */
    private static void saveIndexDefinition(Transaction tr, IndexDefinition definition, DirectorySubspace indexSubspace) {
        byte[] indexDefinitionKey = indexSubspace.pack(BucketMetadataMagic.INDEX_DEFINITION.getValue());
        tr.set(indexDefinitionKey, JSONUtil.writeValueAsBytes(definition));
    }

    /**
     * Updates index definition and increments bucket metadata version.
     *
     * <p>Public method for updating existing index definitions (typically status changes).
     * Locates index by ID, persists updated definition, and increments bucket version to
     * trigger metadata convergence.
     *
     * @param tr         transaction for database operations
     * @param metadata   bucket metadata containing the index
     * @param definition updated index definition
     */
    public static void saveIndexDefinition(Transaction tr, BucketMetadata metadata, IndexDefinition definition) {
        Index index = metadata.indexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
        saveIndexDefinition(tr, definition, index.subspace());
        BucketMetadataUtil.increaseVersion(tr, metadata.subspace(), POSITIVE_DELTA_ONE);
    }

    /**
     * Lists all index names in the bucket.
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace
     * @return list of index names (directory names under indexes/)
     * @throws KronotopException   if indexes directory does not exist
     * @throws CompletionException on directory operation errors
     */
    public static List<String> list(Transaction tr, DirectorySubspace bucketMetadataSubspace) {
        List<String> subpath = new ArrayList<>(bucketMetadataSubspace.getPath());
        subpath.add(BucketMetadataUtil.INDEXES_DIRECTORY);
        try {
            return DirectoryLayer.getDefault().list(tr, subpath).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new KronotopException("No such index directory: '" + bucketMetadataSubspace.getPath() + "'");
            }
            throw e;
        }
    }

    /**
     * Constructs the FoundationDB key for storing an index's cardinality value.
     *
     * <p>This method builds a tuple-encoded key within the bucket metadata subspace
     * that identifies the cardinality statistic for a specific index. The key structure
     * includes:
     * <ul>
     *   <li>HEADER magic value</li>
     *   <li>INDEX_STATISTICS magic value</li>
     *   <li>INDEX_CARDINALITY magic value</li>
     *   <li>The index ID</li>
     * </ul>
     *
     * <p>Cardinality represents the approximate number of unique values in the index,
     * which is useful for query optimization and statistics.
     *
     * @param bucketMetadataSubspace the directory subspace representing the bucket's metadata
     * @param indexId                the unique identifier of the index
     * @return the byte array key for accessing the index's cardinality value
     */
    public static byte[] cardinalityKey(DirectorySubspace bucketMetadataSubspace, long indexId) {
        Tuple tuple = Tuple.from(
                BucketMetadataMagic.HEADER.getValue(),
                BucketMetadataMagic.INDEX_STATISTICS.getValue(),
                indexId,
                BucketMetadataMagic.CARDINALITY.getValue()
        );
        return bucketMetadataSubspace.pack(tuple);
    }

    /**
     * Constructs key for index histogram statistics.
     *
     * <p>Key structure: HEADER/INDEX_STATISTICS/{indexId}/HISTOGRAM
     *
     * @param bucketMetadataSubspace bucket metadata subspace
     * @param indexId                index identifier
     * @return byte array key for histogram data
     */
    public static byte[] histogramKey(DirectorySubspace bucketMetadataSubspace, long indexId) {
        Tuple tuple = Tuple.from(
                BucketMetadataMagic.HEADER.getValue(),
                BucketMetadataMagic.INDEX_STATISTICS.getValue(),
                indexId,
                BucketMetadataMagic.HISTOGRAM.getValue()
        );
        return bucketMetadataSubspace.pack(tuple);
    }

    /**
     * Atomically modifies index cardinality using byte-encoded delta.
     *
     * <p>Internal helper using FoundationDB ADD mutation.
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace
     * @param indexId                index identifier
     * @param delta                  byte array delta (little-endian long)
     */
    private static void mutateCardinality(Transaction tr, DirectorySubspace bucketMetadataSubspace, long indexId, byte[] delta) {
        byte[] key = cardinalityKey(bucketMetadataSubspace, indexId);
        tr.mutate(MutationType.ADD, key, delta);
    }

    /**
     * Atomically modifies index cardinality by specified delta.
     *
     * <p>Optimizes common cases (+1, -1) using pre-encoded constants.
     * Uses FoundationDB ADD mutation for atomic updates.
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace
     * @param indexId                index identifier
     * @param delta                  cardinality change (positive or negative)
     */
    public static void mutateCardinality(Transaction tr, DirectorySubspace bucketMetadataSubspace, long indexId, long delta) {
        if (delta == 1) {
            mutateCardinality(tr, bucketMetadataSubspace, indexId, POSITIVE_DELTA_ONE);
        } else if (delta == -1) {
            mutateCardinality(tr, bucketMetadataSubspace, indexId, NEGATIVE_DELTA_ONE);
        } else {
            byte[] param = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(delta).array();
            mutateCardinality(tr, bucketMetadataSubspace, indexId, param);
        }
    }

    /**
     * Physically removes index and all associated metadata.
     *
     * <p>Deletes index definition, cardinality, and entire index directory subspace.
     * Increments bucket version to trigger metadata convergence.
     *
     * <p>Called by {@link IndexDropRoutine} after marking index as DROPPED
     * and waiting for in-flight transactions to complete.
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace
     * @param name                   index name to remove
     * @throws NoSuchIndexException if index does not exist
     * @see IndexDropRoutine
     * @see #drop(TransactionalContext, BucketMetadata, String)
     */
    public static void clear(Transaction tr, DirectorySubspace bucketMetadataSubspace, String name) {
        DirectorySubspace indexSubspace = open(tr, bucketMetadataSubspace, name);
        if (indexSubspace == null) {
            throw new NoSuchIndexException(name);
        }
        IndexDefinition definition = loadIndexDefinition(tr, indexSubspace);

        // Delete index metadata first
        byte[] indexDefinitionKey = indexSubspace.pack(BucketMetadataMagic.INDEX_DEFINITION.getValue());
        tr.clear(indexDefinitionKey);

        // Remove cardinality
        byte[] cardinalityKey = cardinalityKey(bucketMetadataSubspace, definition.id());
        tr.clear(cardinalityKey);

        // Clear the index
        List<String> subpath = new ArrayList<>(bucketMetadataSubspace.getPath());
        subpath.add(BucketMetadataUtil.INDEXES_DIRECTORY);
        subpath.add(definition.name());
        DirectoryLayer.getDefault().remove(tr, subpath).join();
        BucketMetadataUtil.increaseVersion(tr, bucketMetadataSubspace, POSITIVE_DELTA_ONE);
    }

    /**
     * Marks an index as dropped and initiates asynchronous cleanup.
     *
     * <p>This method performs a two-phase deletion:
     * <ol>
     *   <li>Marks the index as {@link IndexStatus#DROPPED}, making it inaccessible for queries</li>
     *   <li>Creates an {@link IndexDropTask} in a randomly selected shard to coordinate background cleanup</li>
     * </ol>
     *
     * <p>Random shard selection ensures single-point coordination and load distribution.
     *
     * @param tx       the transactional context providing access to both the transaction and application context
     * @param metadata the bucket metadata containing the index to be dropped
     * @param name     the name of the index to be dropped
     * @throws NoSuchIndexException  if the specified index does not exist
     * @throws IllegalStateException if the index is already in DROPPED status
     * @see #clear(Transaction, DirectorySubspace, String)
     * @see IndexDropRoutine
     */
    public static void drop(TransactionalContext tx, BucketMetadata metadata, String name) {
        DirectorySubspace indexSubspace = IndexUtil.open(tx.tr(), metadata.subspace(), name);

        IndexDefinition latestDef = IndexUtil.loadIndexDefinition(tx.tr(), indexSubspace);
        if (latestDef.status() == IndexStatus.DROPPED) {
            throw new KronotopException(
                    String.format("Index is already in the '%s' status", IndexStatus.DROPPED)
            );
        }

        List<Versionstamp> tasks = IndexTaskUtil.getTaskIds(tx, metadata.namespace(), metadata.name(), name);
        if (!tasks.isEmpty()) {
            throw new KronotopException("Index has active tasks");
        }

        IndexUtil.saveIndexDefinition(tx.tr(), metadata, latestDef.updateStatus(IndexStatus.DROPPED));
        BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);

        IndexDropTask task = new IndexDropTask(metadata.namespace(), metadata.name(), latestDef.id());
        byte[] definition = JSONUtil.writeValueAsBytes(task);

        BucketService service = tx.context().getService(BucketService.NAME);
        int shardId = random.nextInt(service.getNumberOfShards());

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);
        int userVersion = tx.getAndIncreaseUserVersion();
        TaskStorage.create(tx.tr(), userVersion, taskSubspace, definition);
        IndexTaskUtil.setBackPointer(tx.tr(), indexSubspace, userVersion, shardId);

        TaskStorage.triggerWatchers(tx.tr(), taskSubspace);
    }

    /**
     * Initiates asynchronous statistical analysis of an index to collect cardinality
     * and histogram data for query optimization.
     *
     * @param tx       the transactional context
     * @param metadata the bucket metadata containing the index
     * @param name     the name of the index to analyze
     * @throws NoSuchIndexException if the specified index does not exist
     */
    public static void analyze(TransactionalContext tx, BucketMetadata metadata, String name) {
        DirectorySubspace indexSubspace = IndexUtil.open(tx.tr(), metadata.subspace(), name);
        IndexDefinition definition = IndexUtil.loadIndexDefinition(tx.tr(), indexSubspace);
        if (definition.status() != IndexStatus.READY) {
            throw new KronotopException(
                    String.format("Cannot analyze index: index is not in READY state (current=%s)", definition.status())
            );
        }

        BucketService service = tx.context().getService(BucketService.NAME);
        int shardId = random.nextInt(service.getNumberOfShards());

        IndexAnalyzeTask task = new IndexAnalyzeTask(metadata.namespace(), metadata.name(), definition.id(), shardId);
        byte[] encodedTask = JSONUtil.writeValueAsBytes(task);

        int userVersion = tx.getAndIncreaseUserVersion();
        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);
        TaskStorage.create(tx.tr(), userVersion, taskSubspace, encodedTask);
        IndexTaskUtil.setBackPointer(tx.tr(), indexSubspace, userVersion, shardId);

        TaskStorage.triggerWatchers(tx.tr(), taskSubspace);
    }

    /**
     * Creates building tasks for all shards with identical boundaries.
     *
     * <p>Called by {@link IndexBoundaryRoutine} after determining scan boundaries.
     * Creates one {@link IndexBuildingTask} per shard, each with the same lower/upper
     * versionstamp range. Each shard processes its portion of the volume independently.
     *
     * <p><strong>Task Identifiers:</strong> Uses shard ID as userVersion to create
     * predictable versionstamped task IDs that match across all shards.
     *
     * <p><strong>Back Pointers:</strong> Creates versionstamped keys in index subspace
     * linking to each building task for tracking and inspection.
     *
     * @param tx         transactional context
     * @param metadata   bucket metadata containing the index
     * @param indexId    index identifier
     * @param boundaries lower and upper versionstamp boundaries from primary index
     * @throws KronotopException if index not found
     * @see IndexBoundaryRoutine
     * @see IndexBuildingRoutine
     */
    public static void createIndexBuildingTasks(TransactionalContext tx, BucketMetadata metadata, long indexId, Boundaries boundaries) {
        Index index = metadata.indexes().getIndexById(indexId, IndexSelectionPolicy.ALL);
        if (index == null) {
            throw new KronotopException(String.format("Index with id: '%d' could not be found", indexId));
        }

        BucketService service = tx.context().getService(BucketService.NAME);
        for (int shardId = 0; shardId < service.getNumberOfShards(); shardId++) {
            DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);

            IndexBuildingTask task = new IndexBuildingTask(
                    metadata.namespace(),
                    metadata.name(),
                    indexId,
                    shardId,
                    boundaries.lower(),
                    boundaries.upper()
            );
            byte[] definition = JSONUtil.writeValueAsBytes(task);

            TaskStorage.create(tx.tr(), shardId, taskSubspace, definition);
            IndexTaskUtil.setBackPointer(tx.tr(), index.subspace(), shardId, shardId);
        }
    }

    /**
     * Transitions index to READY if all building tasks completed.
     *
     * <p>Called by {@link IndexMaintenanceWatchDog} after detecting task completion events.
     * Iterates through all task back pointers in index subspace, validates each task's
     * status, and marks index as READY only if all non-BOUNDARY tasks are complete.
     *
     * <p><strong>Back Pointer Structure:</strong>
     * <pre>
     * Key: TASKS/{taskId}/{shardId}
     * Unpacked tuple: [TASKS, versionstamp, shardId]
     * </pre>
     *
     * <p><strong>Validation Logic:</strong>
     * <ol>
     *   <li>Scan all back pointers under TASKS/ prefix</li>
     *   <li>For each back pointer, unpack taskId and shardId</li>
     *   <li>Load task definition from corresponding task subspace</li>
     *   <li>Skip if task definition is null (should not happen)</li>
     *   <li>Skip if task kind is BOUNDARY (leftover, will be swept)</li>
     *   <li>Return false if any BUILD task still exists</li>
     *   <li>If loop completes, all building tasks done → mark READY</li>
     * </ol>
     *
     * <p><strong>Return Values:</strong>
     * <ul>
     *   <li>true: Index transitioned to READY (was BUILDING, all tasks done)</li>
     *   <li>false: Index not found, already READY, or building tasks still active</li>
     * </ul>
     *
     * @param tx        transactional context
     * @param namespace bucket namespace
     * @param bucket    bucket name
     * @param indexId   index identifier
     * @return true if index transitioned to READY, false otherwise
     * @see IndexMaintenanceWatchDog
     */
    public static boolean markIndexAsReadyIfBuildDone(TransactionalContext tx, String namespace, String bucket, long indexId) {
        BucketMetadata metadata = BucketMetadataUtil.forceOpen(tx.context(), tx.tr(), namespace, bucket);
        Index index = metadata.indexes().getIndexById(indexId, IndexSelectionPolicy.READWRITE);
        if (index == null) {
            return false;
        }
        if (index.definition().status() == IndexStatus.READY) {
            return false;
        }

        AtomicBoolean stop = new AtomicBoolean();
        IndexTaskUtil.scanTaskBackPointers(tx.tr(), index.subspace(), ((taskId, shardId) -> {
            DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);
            byte[] definition = TaskStorage.getDefinition(tx.tr(), taskSubspace, taskId);
            if (definition == null) {
                // this should never happen
                return true; // continue;
            }
            IndexMaintenanceTask task = JSONUtil.readValue(definition, IndexMaintenanceTask.class);
            // Leftover task, sweeper will drop it soon.
            if (task.getKind() == IndexMaintenanceTaskKind.BOUNDARY) {
                return true; // continue
            }
            // Any other task means that we cannot make the index ready to query.
            stop.set(true);
            return false;
        }));

        if (stop.get()) {
            return false;
        }
        IndexDefinition definition = index.definition().updateStatus(IndexStatus.READY);
        IndexUtil.saveIndexDefinition(tx.tr(), metadata, definition);
        BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
        return true;
    }
}
