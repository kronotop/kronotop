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
 * Utility class for compound index lifecycle: creation, loading, and storage.
 */
public class CompoundIndexUtil {

    /**
     * Creates a compound index directory and persists its definition.
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace as base path
     * @param definition             compound index definition to persist
     * @return newly created index directory subspace
     * @throws KronotopException if the index name already exists
     */
    public static DirectorySubspace create(Transaction tr, DirectorySubspace bucketMetadataSubspace, CompoundIndexDefinition definition) {
        return BucketMetadataUtil.createIndexSubspace(tr, bucketMetadataSubspace, definition.name(),
                indexSubspace -> saveIndexDefinition(tr, definition, indexSubspace));
    }

    /**
     * Creates a compound index. When the definition's status is READY, skips background
     * building since there are no documents to index.
     *
     * @param tx         transactional context
     * @param metadata   bucket metadata (already opened)
     * @param definition compound index definition to create
     * @return newly created index directory subspace
     * @throws IllegalArgumentException if index status is not READY or WAITING
     */
    public static DirectorySubspace create(TransactionalContext tx, BucketMetadata metadata, CompoundIndexDefinition definition) {
        if (definition.status() != IndexStatus.READY && definition.status() != IndexStatus.WAITING) {
            throw new IllegalArgumentException("Index status must be READY or WAITING at creation time, got: " + definition.status());
        }

        DirectorySubspace indexSubspace = create(tx.tr(), metadata.subspace(), definition);

        if (definition.status() == IndexStatus.READY) {
            BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
            return indexSubspace;
        }

        int shardId = metadata.shards().get(ThreadLocalRandom.current().nextInt(metadata.shards().size()));

        IndexBoundaryTask task = new IndexBoundaryTask(metadata.namespace(), metadata.name(), definition.id(), shardId, IndexType.COMPOUND);
        byte[] encoded = JSONUtil.writeValueAsBytes(task);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);
        int userVersion = tx.getAndIncreaseUserVersion();
        TaskStorage.create(tx.tr(), userVersion, taskSubspace, encoded);
        IndexTaskUtil.setBackPointer(tx.tr(), indexSubspace, userVersion, shardId);

        BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);

        return indexSubspace;
    }

    /**
     * Creates a compound index by namespace and bucket name.
     *
     * @param tx         transactional context
     * @param namespace  bucket namespace
     * @param bucket     bucket name
     * @param definition compound index definition
     * @return newly created index directory subspace
     */
    public static DirectorySubspace create(TransactionalContext tx, String namespace, String bucket, CompoundIndexDefinition definition) {
        BucketMetadata metadata = BucketMetadataUtil.open(tx.context(), tx.tr(), namespace, bucket);
        return create(tx, metadata, definition);
    }

    /**
     * Loads a compound index definition from its subspace.
     *
     * @param tr            transaction for database operations
     * @param indexSubspace index directory subspace
     * @return deserialized compound index definition
     */
    public static CompoundIndexDefinition loadIndexDefinition(ReadTransaction tr, DirectorySubspace indexSubspace) {
        byte[] key = indexSubspace.pack(BucketMetadataMagic.COMPOUND_INDEX_DEFINITION.getValue());
        byte[] value = tr.get(key).join();
        return JSONUtil.readValue(value, CompoundIndexDefinition.class);
    }

    /**
     * Initiates asynchronous statistical analysis of a compound index to collect
     * cardinality and histogram data for query optimization.
     *
     * @param tx       the transactional context
     * @param metadata the bucket metadata containing the index
     * @param name     the name of the compound index to analyze
     * @throws NoSuchIndexException if the specified index does not exist
     * @throws KronotopException    if the index definition cannot be loaded
     */
    public static void analyze(TransactionalContext tx, BucketMetadata metadata, String name) {
        DirectorySubspace indexSubspace = IndexUtil.open(tx.tr(), metadata.subspace(), name);

        CompoundIndexDefinition indexDefinition = loadIndexDefinition(tx.tr(), indexSubspace);
        if (indexDefinition == null) {
            throw new KronotopException("Cannot load index definition for: " + name);
        }

        IndexAnalyzeUtil.scheduleAnalyzeTask(tx, metadata, indexSubspace, indexDefinition);
    }

    /**
     * Updates compound index definition and increments the bucket metadata version.
     *
     * @param tr         transaction for database operations
     * @param metadata   bucket metadata containing the index
     * @param definition updated compound index definition
     */
    public static void saveIndexDefinition(Transaction tr, BucketMetadata metadata, CompoundIndexDefinition definition) {
        CompoundIndex index = metadata.compoundIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
        saveIndexDefinition(tr, definition, index.subspace());
        BucketMetadataUtil.increaseVersion(tr, metadata.subspace(), POSITIVE_DELTA_ONE);
    }

    private static void saveIndexDefinition(Transaction tr, CompoundIndexDefinition definition, DirectorySubspace indexSubspace) {
        byte[] key = indexSubspace.pack(BucketMetadataMagic.COMPOUND_INDEX_DEFINITION.getValue());
        tr.set(key, JSONUtil.writeValueAsBytes(definition));
    }

    /**
     * Creates a compound index building task for a specific shard.
     *
     * @param tx         transactional context
     * @param metadata   bucket metadata
     * @param indexId    compound index identifier
     * @param shardId    target shard
     * @param boundaries scan range for the building task
     */
    public static void createCompoundIndexBuildingTask(TransactionalContext tx, BucketMetadata metadata, long indexId, int shardId, Boundaries boundaries) {
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(indexId, IndexSelectionPolicy.ALL);
        if (compoundIndex == null) {
            throw new KronotopException(String.format("Compound index with id: '%d' could not be found", indexId));
        }

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);
        IndexBuildingTask task = new IndexBuildingTask(
                metadata.namespace(),
                metadata.name(),
                indexId,
                shardId,
                boundaries.lower(),
                boundaries.upper(),
                IndexType.COMPOUND
        );
        byte[] definition = JSONUtil.writeValueAsBytes(task);

        TaskStorage.create(tx.tr(), shardId, taskSubspace, definition);
        IndexTaskUtil.setBackPointer(tx.tr(), compoundIndex.subspace(), shardId, shardId);
    }

    /**
     * Physically removes the compound index directory, definition, cardinality, and directory entry.
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace
     * @param name                   compound index name to remove
     * @throws NoSuchIndexException if the index does not exist
     * @see IndexDropRoutine
     */
    public static void clear(Transaction tr, DirectorySubspace bucketMetadataSubspace, String name) {
        DirectorySubspace indexSubspace = IndexUtil.open(tr, bucketMetadataSubspace, name);

        CompoundIndexDefinition definition = loadIndexDefinition(tr, indexSubspace);

        byte[] indexDefinitionKey = indexSubspace.pack(BucketMetadataMagic.COMPOUND_INDEX_DEFINITION.getValue());
        tr.clear(indexDefinitionKey);

        byte[] cardinalityKey = IndexUtil.cardinalityKey(bucketMetadataSubspace, definition.id());
        tr.clear(cardinalityKey);

        byte[] histogramKey = IndexUtil.histogramKey(bucketMetadataSubspace, definition.id());
        tr.clear(histogramKey);

        bucketMetadataSubspace.remove(tr, List.of(BucketMetadataUtil.INDEXES_DIRECTORY, definition.name())).join();
        BucketMetadataUtil.increaseVersion(tr, bucketMetadataSubspace, POSITIVE_DELTA_ONE);
    }

    /**
     * Marks compound index as DROPPED and creates an {@link IndexDropTask} for asynchronous cleanup.
     *
     * @param tx       transactional context
     * @param metadata bucket metadata containing the index
     * @param name     compound index name to drop
     * @throws NoSuchIndexException if index does not exist
     * @throws KronotopException    if index is already DROPPED or has active tasks
     * @see IndexDropRoutine
     */
    public static void drop(TransactionalContext tx, BucketMetadata metadata, String name) {
        DirectorySubspace indexSubspace = IndexUtil.open(tx.tr(), metadata.subspace(), name);

        CompoundIndexDefinition latestDef = loadIndexDefinition(tx.tr(), indexSubspace);
        if (latestDef.status() == IndexStatus.DROPPED) {
            throw new KronotopException(
                    String.format("Index is already in the '%s' status", IndexStatus.DROPPED)
            );
        }

        List<Versionstamp> tasks = IndexTaskUtil.getTaskIds(tx, metadata.namespace(), metadata.name(), name);
        if (!tasks.isEmpty()) {
            throw new KronotopException("Index has active tasks");
        }

        saveIndexDefinition(tx.tr(), metadata, latestDef.updateStatus(IndexStatus.DROPPED));
        IndexTaskUtil.scheduleDropTask(tx, metadata, latestDef.id(), indexSubspace);
    }

    /**
     * Transitions compound index to READY if all building tasks have completed.
     *
     * @param tx        transactional context
     * @param namespace bucket namespace
     * @param bucket    bucket name
     * @param indexId   compound index identifier
     * @return true if index transitioned to READY
     */
    public static boolean markCompoundIndexAsReadyIfBuildDone(TransactionalContext tx, String namespace, String bucket, long indexId) {
        BucketMetadata metadata = BucketMetadataUtil.reload(tx.context(), tx.tr(), namespace, bucket);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexById(indexId, IndexSelectionPolicy.READWRITE);
        if (compoundIndex == null) {
            return false;
        }
        if (compoundIndex.definition().status() == IndexStatus.READY) {
            return false;
        }

        if (IndexTaskUtil.hasActiveNonBoundaryTasks(tx, compoundIndex.subspace())) {
            return false;
        }
        CompoundIndexDefinition updated = compoundIndex.definition().updateStatus(IndexStatus.READY);
        CompoundIndexUtil.saveIndexDefinition(tx.tr(), metadata, updated);
        BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
        return true;
    }
}
