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
import com.kronotop.bucket.VectorFieldValidationException;
import com.kronotop.bucket.index.maintenance.*;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.util.List;

import static com.kronotop.bucket.BucketMetadataUtil.POSITIVE_DELTA_ONE;

/**
 * Utility class for vector index lifecycle: creation, loading, dropping, background task
 * orchestration, and storage. Unlike single-field and compound indexes, vector indexes
 * have no cardinality tracking and no entries subspace.
 */
public class VectorIndexUtil {

    /**
     * Creates a vector index directory and persists its definition.
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace as a base path
     * @param definition             vector index definition to persist
     * @return newly created index directory subspace
     * @throws KronotopException if the index name already exists
     */
    public static DirectorySubspace create(Transaction tr, DirectorySubspace bucketMetadataSubspace, VectorIndexDefinition definition) {
        return BucketMetadataUtil.createIndexSubspace(tr, bucketMetadataSubspace, definition.name(),
                indexSubspace -> saveIndexDefinition(tr, definition, indexSubspace));
    }

    /**
     * Creates a vector index using preloaded bucket metadata. When the definition's status is READY,
     * skips background building since there are no documents to index. When WAITING, schedules an
     * {@link IndexBoundaryTask} to coordinate background building.
     *
     * @param tx         transactional context
     * @param metadata   bucket metadata (already opened)
     * @param definition vector index definition to create
     * @return newly created index directory subspace
     * @throws IllegalArgumentException if index status is not READY or WAITING
     */
    public static DirectorySubspace create(TransactionalContext tx, BucketMetadata metadata, VectorIndexDefinition definition) {
        if (definition.status() != IndexStatus.READY && definition.status() != IndexStatus.WAITING) {
            throw new IllegalArgumentException("Index status must be READY or WAITING at creation time, got: " + definition.status());
        }

        DirectorySubspace indexSubspace = create(tx.tr(), metadata.subspace(), definition);
        metadata.vectorIndexes().register(definition, indexSubspace);

        if (definition.status() == IndexStatus.READY) {
            BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
            return indexSubspace;
        }

        // Vector indexes require single-shard buckets — schedule the boundary task there.
        int shardId = metadata.shards().getFirst();

        IndexBoundaryTask task = new IndexBoundaryTask(metadata.namespace(), metadata.name(), definition.id(), shardId, IndexType.VECTOR);
        byte[] encoded = JSONUtil.writeValueAsBytes(task);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);
        int userVersion = tx.getAndIncreaseUserVersion();
        TaskStorage.create(tx.tr(), userVersion, taskSubspace, encoded);
        IndexTaskUtil.setBackPointer(tx.tr(), indexSubspace, userVersion, shardId);

        BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);

        return indexSubspace;
    }

    /**
     * Loads a vector index definition from its subspace.
     *
     * @param tr            transaction for database operations
     * @param indexSubspace index directory subspace
     * @return deserialized vector index definition
     */
    public static VectorIndexDefinition loadIndexDefinition(ReadTransaction tr, DirectorySubspace indexSubspace) {
        byte[] key = indexSubspace.pack(BucketMetadataMagic.VECTOR_INDEX_DEFINITION.getValue());
        byte[] value = tr.get(key).join();
        return JSONUtil.readValue(value, VectorIndexDefinition.class);
    }

    private static void saveIndexDefinition(Transaction tr, VectorIndexDefinition definition, DirectorySubspace indexSubspace) {
        byte[] key = indexSubspace.pack(BucketMetadataMagic.VECTOR_INDEX_DEFINITION.getValue());
        tr.set(key, JSONUtil.writeValueAsBytes(definition));
    }

    /**
     * Updates vector index definition and increments the bucket metadata version.
     *
     * @param tr         transaction for database operations
     * @param metadata   bucket metadata containing the index
     * @param definition updated vector index definition
     */
    public static void saveIndexDefinition(Transaction tr, BucketMetadata metadata, VectorIndexDefinition definition) {
        VectorIndex index = metadata.vectorIndexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
        saveIndexDefinition(tr, definition, index.subspace());
        BucketMetadataUtil.increaseVersion(tr, metadata.subspace(), POSITIVE_DELTA_ONE);
    }

    /**
     * Physically removes the vector index directory and definition from FDB.
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace
     * @param name                   vector index name to remove
     * @throws NoSuchIndexException if the index does not exist
     */
    public static void clear(Transaction tr, DirectorySubspace bucketMetadataSubspace, String name) {
        DirectorySubspace indexSubspace = IndexUtil.open(tr, bucketMetadataSubspace, name);

        byte[] indexDefinitionKey = indexSubspace.pack(BucketMetadataMagic.VECTOR_INDEX_DEFINITION.getValue());
        tr.clear(indexDefinitionKey);

        bucketMetadataSubspace.remove(tr, List.of(BucketMetadataUtil.INDEXES_DIRECTORY, name)).join();
        BucketMetadataUtil.increaseVersion(tr, bucketMetadataSubspace, POSITIVE_DELTA_ONE);
    }

    /**
     * Marks a vector index as DROPPED and creates an {@link IndexDropTask} for asynchronous cleanup.
     *
     * @param tx       transactional context
     * @param metadata bucket metadata containing the index
     * @param name     vector index name to drop
     * @throws NoSuchIndexException if the index does not exist
     * @throws KronotopException    if the index is already DROPPED or has active tasks
     */
    public static void drop(TransactionalContext tx, BucketMetadata metadata, String name) {
        DirectorySubspace indexSubspace = IndexUtil.open(tx.tr(), metadata.subspace(), name);

        VectorIndexDefinition latestDef = loadIndexDefinition(tx.tr(), indexSubspace);
        if (latestDef.status() == IndexStatus.DROPPED) {
            throw new KronotopException(
                    String.format("Index is already in the '%s' status", IndexStatus.DROPPED)
            );
        }

        List<Versionstamp> tasks = IndexTaskUtil.getTaskIds(tx, metadata.namespace(), metadata.name(), name);
        if (!tasks.isEmpty()) {
            throw new KronotopException("Index has active tasks");
        }

        saveIndexDefinition(tx.tr(), latestDef.updateStatus(IndexStatus.DROPPED), indexSubspace);
        BucketMetadataUtil.increaseVersion(tx.tr(), metadata.subspace(), POSITIVE_DELTA_ONE);
        BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);

        IndexDropTask task = new IndexDropTask(metadata.namespace(), metadata.name(), latestDef.id());
        byte[] definition = JSONUtil.writeValueAsBytes(task);

        // Vector index buckets have exactly one shard — schedule the task there.
        int shardId = metadata.shards().getFirst();

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);
        int userVersion = tx.getAndIncreaseUserVersion();
        TaskStorage.create(tx.tr(), userVersion, taskSubspace, definition);
        IndexTaskUtil.setBackPointer(tx.tr(), indexSubspace, userVersion, shardId);

        TaskStorage.triggerWatchers(tx.tr(), taskSubspace);
    }

    /**
     * Validates that a value conforms to a vector index definition: must be a BsonArray of BsonDouble
     * with the correct number of dimensions. Null or missing values are allowed (vector field is optional).
     */
    public static void validateVectorField(VectorIndexDefinition definition, BsonValue value) {
        if (value == null || value.isNull()) {
            return;
        }
        if (value.getBsonType() != BsonType.ARRAY) {
            throw VectorFieldValidationException.notAnArray(definition.selector());
        }
        BsonArray array = value.asArray();
        if (array.size() != definition.dimensions()) {
            throw VectorFieldValidationException.wrongDimensions(definition.selector(), definition.dimensions(), array.size());
        }
        for (BsonValue element : array) {
            if (element.getBsonType() != BsonType.DOUBLE) {
                throw VectorFieldValidationException.elementsNotDouble(definition.selector());
            }
        }
    }

    /**
     * Validates all vector fields in a document against the bucket's vector index definitions.
     */
    public static void validateVectorFields(BucketMetadata metadata, BsonDocument document) {
        for (VectorIndex vi : metadata.vectorIndexes().getIndexes(IndexSelectionPolicy.READWRITE)) {
            BsonValue value = SelectorMatcher.match(vi.definition().selector(), document);
            validateVectorField(vi.definition(), value);
        }
    }

    /**
     * Creates an {@link IndexBuildingTask} for a vector index on a specific shard.
     *
     * @param tx         transactional context
     * @param metadata   bucket metadata containing the index
     * @param indexId    vector index identifier
     * @param shardId    target shard
     * @param boundaries versionstamp scan boundaries
     */
    public static void createVectorIndexBuildingTask(TransactionalContext tx, BucketMetadata metadata, long indexId, int shardId, Boundaries boundaries) {
        VectorIndex index = metadata.vectorIndexes().getIndexById(indexId, IndexSelectionPolicy.ALL);
        if (index == null) {
            throw new KronotopException(String.format("Vector index with id: '%d' could not be found", indexId));
        }

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);
        IndexBuildingTask task = new IndexBuildingTask(
                metadata.namespace(),
                metadata.name(),
                indexId,
                shardId,
                boundaries.lower(),
                boundaries.upper(),
                IndexType.VECTOR
        );
        byte[] definition = JSONUtil.writeValueAsBytes(task);

        TaskStorage.create(tx.tr(), shardId, taskSubspace, definition);
        IndexTaskUtil.setBackPointer(tx.tr(), index.subspace(), shardId, shardId);
    }

    /**
     * Transitions vector index to READY if all building tasks have completed.
     *
     * @param tx        transactional context
     * @param namespace bucket namespace
     * @param bucket    bucket name
     * @param indexId   vector index identifier
     * @return true if index transitioned to READY, false if not found, already READY, or tasks active
     */
    public static boolean markVectorIndexAsReadyIfBuildDone(TransactionalContext tx, String namespace, String bucket, long indexId) {
        BucketMetadata metadata = BucketMetadataUtil.reload(tx.context(), tx.tr(), namespace, bucket);
        VectorIndex index = metadata.vectorIndexes().getIndexById(indexId, IndexSelectionPolicy.READWRITE);
        if (index == null) {
            return false;
        }
        if (index.definition().status() == IndexStatus.READY) {
            return false;
        }

        if (IndexTaskUtil.hasActiveNonBoundaryTasks(tx, index.subspace())) {
            return false;
        }
        IndexMaintainer.cleanWatermark(tx.tr(), index.subspace());
        VectorIndexDefinition definition = index.definition().updateStatus(IndexStatus.READY);
        VectorIndexUtil.saveIndexDefinition(tx.tr(), metadata, definition);
        BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
        return true;
    }
}
