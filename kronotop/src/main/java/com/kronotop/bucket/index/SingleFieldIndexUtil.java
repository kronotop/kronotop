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


package com.kronotop.bucket.index;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataMagic;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.maintenance.*;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.kronotop.bucket.BucketMetadataUtil.POSITIVE_DELTA_ONE;

/**
 * Utility class for index lifecycle management: creation, deletion, status transitions,
 * cardinality tracking, and background task orchestration.
 *
 * @see SingleFieldIndexDefinition
 * @see BucketMetadata
 */
public class SingleFieldIndexUtil {

    /**
     * Creates index directory subspace and persists index definition.
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace as the base path
     * @param definition             index definition to persist
     * @return newly created index directory subspace
     * @throws KronotopException if the index name already exists
     */
    public static DirectorySubspace create(Transaction tr, DirectorySubspace bucketMetadataSubspace, SingleFieldIndexDefinition definition) {
        return BucketMetadataUtil.createIndexSubspace(tr, bucketMetadataSubspace, definition.name(),
                indexSubspace -> saveIndexDefinition(tr, definition, indexSubspace));
    }

    /**
     * Creates an index using preloaded bucket metadata. When the definition's status is READY,
     * skips background building since there are no documents to index.
     *
     * @param tx         transactional context
     * @param metadata   bucket metadata (already opened)
     * @param definition index definition to create
     * @return newly created index directory subspace
     * @throws KronotopException        if the index name already exists
     * @throws IllegalArgumentException if index status is not READY or WAITING
     */
    public static DirectorySubspace create(TransactionalContext tx,
                                           BucketMetadata metadata,
                                           SingleFieldIndexDefinition definition) {
        if (definition.status() != IndexStatus.READY && definition.status() != IndexStatus.WAITING) {
            throw new IllegalArgumentException("Index status must be READY or WAITING at creation time, got: " + definition.status());
        }

        DirectorySubspace indexSubspace = create(tx.tr(), metadata.subspace(), definition);

        if (PrimaryIndex.isPrimary(definition) || definition.status() == IndexStatus.READY) {
            BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
            return indexSubspace;
        }

        int shardId = metadata.shards().get(ThreadLocalRandom.current().nextInt(metadata.shards().size()));

        IndexBoundaryTask task = new IndexBoundaryTask(metadata.namespace(), metadata.name(), definition.id(), shardId);
        byte[] encoded = JSONUtil.writeValueAsBytes(task);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);
        int userVersion = tx.getAndIncreaseUserVersion();
        TaskStorage.create(tx.tr(), userVersion, taskSubspace, encoded);
        IndexTaskUtil.setBackPointer(tx.tr(), indexSubspace, userVersion, shardId);

        BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);

        return indexSubspace;
    }

    /**
     * Creates an index and initiates background building for single field indexes.
     *
     * <p>Primary indexes return immediately; single field indexes create an {@link IndexBoundaryTask}
     * on a random shard to coordinate background building.
     *
     * @param tx         transactional context
     * @param namespace  bucket namespace
     * @param bucket     bucket name
     * @param definition index definition
     * @return newly created index directory subspace
     * @throws KronotopException if the index name already exists
     */
    public static DirectorySubspace create(TransactionalContext tx,
                                           String namespace,
                                           String bucket,
                                           SingleFieldIndexDefinition definition) {
        BucketMetadata metadata = BucketMetadataUtil.open(tx.context(), tx.tr(), namespace, bucket);
        return create(tx, metadata, definition);
    }


    /**
     * Loads index definition from index subspace.
     *
     * @param tr            transaction for database operations
     * @param indexSubspace index directory subspace
     * @return deserialized index definition
     */
    public static SingleFieldIndexDefinition loadIndexDefinition(ReadTransaction tr, DirectorySubspace indexSubspace) {
        byte[] indexDefinitionKey = indexSubspace.pack(BucketMetadataMagic.SINGLE_FIELD_INDEX_DEFINITION.getValue());
        byte[] value = tr.get(indexDefinitionKey).join();
        return JSONUtil.readValue(value, SingleFieldIndexDefinition.class);
    }

    /**
     * Persists index definition to index subspace.
     *
     * @param tr            transaction for database operations
     * @param definition    index definition to persist
     * @param indexSubspace index directory subspace
     */
    private static void saveIndexDefinition(Transaction tr, SingleFieldIndexDefinition definition, DirectorySubspace indexSubspace) {
        byte[] indexDefinitionKey = indexSubspace.pack(BucketMetadataMagic.SINGLE_FIELD_INDEX_DEFINITION.getValue());
        tr.set(indexDefinitionKey, JSONUtil.writeValueAsBytes(definition));
    }

    /**
     * Updates index definition and increments the bucket metadata version.
     *
     * @param tr         transaction for database operations
     * @param metadata   bucket metadata containing the index
     * @param definition updated index definition
     */
    public static void saveIndexDefinition(Transaction tr, BucketMetadata metadata, SingleFieldIndexDefinition definition) {
        Index index = metadata.indexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
        saveIndexDefinition(tr, definition, index.subspace());
        BucketMetadataUtil.increaseVersion(tr, metadata.subspace(), POSITIVE_DELTA_ONE);
    }

    /**
     * Physically removes index directory, definition, and cardinality data.
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace
     * @param name                   index name to remove
     * @throws NoSuchIndexException if the index does not exist
     * @see IndexDropRoutine
     */
    public static void clear(Transaction tr, DirectorySubspace bucketMetadataSubspace, String name) {
        DirectorySubspace indexSubspace = IndexUtil.open(tr, bucketMetadataSubspace, name);
        if (indexSubspace == null) {
            throw new NoSuchIndexException(name);
        }
        SingleFieldIndexDefinition definition = loadIndexDefinition(tr, indexSubspace);

        // Delete index metadata first
        byte[] indexDefinitionKey = indexSubspace.pack(BucketMetadataMagic.SINGLE_FIELD_INDEX_DEFINITION.getValue());
        tr.clear(indexDefinitionKey);

        // Remove cardinality
        byte[] cardinalityKey = IndexUtil.cardinalityKey(bucketMetadataSubspace, definition.id());
        tr.clear(cardinalityKey);

        // Clear the index
        bucketMetadataSubspace.remove(tr, List.of(BucketMetadataUtil.INDEXES_DIRECTORY, definition.name())).join();
        BucketMetadataUtil.increaseVersion(tr, bucketMetadataSubspace, POSITIVE_DELTA_ONE);
    }

    /**
     * Marks index as DROPPED and creates an {@link IndexDropTask} for asynchronous cleanup.
     *
     * @param tx       transactional context
     * @param metadata bucket metadata containing the index
     * @param name     index name to drop
     * @throws NoSuchIndexException if the index does not exist
     * @throws KronotopException    if the index is already DROPPED or has active tasks
     * @see IndexDropRoutine
     */
    public static void drop(TransactionalContext tx, BucketMetadata metadata, String name) {
        DirectorySubspace indexSubspace = IndexUtil.open(tx.tr(), metadata.subspace(), name);

        SingleFieldIndexDefinition latestDef = SingleFieldIndexUtil.loadIndexDefinition(tx.tr(), indexSubspace);
        if (latestDef.status() == IndexStatus.DROPPED) {
            throw new KronotopException(
                    String.format("Index is already in the '%s' status", IndexStatus.DROPPED)
            );
        }

        List<Versionstamp> tasks = IndexTaskUtil.getTaskIds(tx, metadata.namespace(), metadata.name(), name);
        if (!tasks.isEmpty()) {
            throw new KronotopException("Index has active tasks");
        }

        SingleFieldIndexUtil.saveIndexDefinition(tx.tr(), metadata, latestDef.updateStatus(IndexStatus.DROPPED));
        IndexTaskUtil.scheduleDropTask(tx, metadata, latestDef.id(), indexSubspace);
    }

    /**
     * Initiates asynchronous statistical analysis of a single-field index to collect
     * cardinality and histogram data for query optimization.
     *
     * @param tx       the transactional context
     * @param metadata the bucket metadata containing the index
     * @param name     the name of the index to analyze
     * @throws NoSuchIndexException if the specified index does not exist
     * @throws KronotopException    if the index definition cannot be loaded
     */
    public static void analyze(TransactionalContext tx, BucketMetadata metadata, String name) {
        DirectorySubspace indexSubspace = IndexUtil.open(tx.tr(), metadata.subspace(), name);

        SingleFieldIndexDefinition indexDefinition = loadIndexDefinition(tx.tr(), indexSubspace);
        if (indexDefinition == null) {
            throw new KronotopException("Cannot load index definition for: " + name);
        }

        IndexAnalyzeUtil.scheduleAnalyzeTask(tx, metadata, indexSubspace, indexDefinition);
    }

    public static void createIndexBuildingTask(TransactionalContext tx, BucketMetadata metadata, long indexId, int shardId, Boundaries boundaries) {
        Index index = metadata.indexes().getIndexById(indexId, IndexSelectionPolicy.ALL);
        if (index == null) {
            throw new KronotopException(String.format("Index with id: '%d' could not be found", indexId));
        }

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

    /**
     * Transitions index to READY if all building tasks have completed.
     *
     * <p>Scans task back pointers and marks index READY only when no BUILD tasks remain.
     * BOUNDARY tasks are ignored as they will be swept later.
     *
     * @param tx        transactional context
     * @param namespace bucket namespace
     * @param bucket    bucket name
     * @param indexId   index identifier
     * @return true if index transitioned to READY, false if not found, already READY, or tasks active
     */
    public static boolean markIndexAsReadyIfBuildDone(TransactionalContext tx, String namespace, String bucket, long indexId) {
        BucketMetadata metadata = BucketMetadataUtil.reload(tx.context(), tx.tr(), namespace, bucket);
        Index index = metadata.indexes().getIndexById(indexId, IndexSelectionPolicy.READWRITE);
        if (index == null) {
            return false;
        }
        if (index.definition().status() == IndexStatus.READY) {
            return false;
        }

        if (IndexTaskUtil.hasActiveNonBoundaryTasks(tx, index.subspace())) {
            return false;
        }
        SingleFieldIndexDefinition definition = index.definition().updateStatus(IndexStatus.READY);
        SingleFieldIndexUtil.saveIndexDefinition(tx.tr(), metadata, definition);
        BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
        return true;
    }
}
