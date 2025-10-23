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
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.*;
import com.kronotop.bucket.index.maintenance.*;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionException;

/**
 * The IndexUtil class provides utility methods for managing and interacting with directory
 * subspaces that represent indexes in a FoundationDB database. It includes functionality
 * for reading the global index version, creating indexes, and opening existing index subspaces.
 */
public class IndexUtil {
    public static final byte[] POSITIVE_DELTA_ONE = new byte[]{1, 0, 0, 0, 0, 0, 0, 0}; // 1L, little-endian
    public static final byte[] NEGATIVE_DELTA_ONE = new byte[]{-1, -1, -1, -1, -1, -1, -1, -1}; // -1L, little-endian

    /**
     * Creates a new directory subspace for an index within the bucket metadata subspace.
     * This method constructs the path for the index subspace, initializes it using the default
     * DirectoryLayer, stores the index definition as metadata, and increments the version of the
     * bucket metadata subspace.
     *
     * @param tr                     the transaction instance used to interact with the database
     * @param bucketMetadataSubspace the bucket metadata subspace serving as the base path for the index
     * @param definition             the definition of the index to be created
     * @return the newly created directory subspace for the specified index
     * @throws KronotopException   if the index with the specified name already exists
     * @throws CompletionException if an error occurs during the operation
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

    public static DirectorySubspace create(TransactionalContext tx,
                                           String namespace,
                                           String bucket,
                                           IndexDefinition definition) {

        BucketMetadata bucketMetadata = BucketMetadataUtil.open(tx.context(), tx.tr(), namespace, bucket);
        // Create the index
        DirectorySubspace indexSubspace = create(tx.tr(), bucketMetadata.subspace(), definition);

        if (definition.id() == DefaultIndexDefinition.ID.id()) {
            // Primary index doesn't require a background index building procedure
            return indexSubspace;
        }

        // Create background build tasks for all shards
        IndexBuildingTask task = new IndexBuildingTask(namespace, bucket, definition.id());
        byte[] encodedTask = JSONUtil.writeValueAsBytes(task);

        BucketService service = tx.context().getService(BucketService.NAME);
        int userVersion = tx.getAndIncreaseUserVersion(); // increases the user version
        for (int shardId = 0; shardId < service.getNumberOfShards(); shardId++) {
            DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);
            // create tasks in the task subspaces with the same ID
            TaskStorage.create(tx.tr(), userVersion, taskSubspace, encodedTask);
        }

        return indexSubspace;
    }

    /**
     * Opens a directory subspace for the specified name within the provided bucket metadata subspace.
     * This method constructs the full path using the bucket metadata subspace and the index name,
     * then attempts to open the directory using the default DirectoryLayer. If the directory
     * does not exist, a KronotopException is thrown.
     *
     * @param tr                     the transaction instance used to interact with the database
     * @param bucketMetadataSubspace the bucket metadata subspace serving as the base path
     * @param name                   the name of the index directory to be opened
     * @return the directory subspace corresponding to the specified name
     * @throws KronotopException   if the specified index directory does not exist
     * @throws CompletionException if an error occurs during the operation
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
     * Loads the index definition associated with a specific index subspace.
     * This method retrieves the serialized index definition from the database,
     * deserializes it, and returns the corresponding IndexDefinition object.
     *
     * @param tr            the transaction instance used to interact with the database
     * @param indexSubspace the directory subspace for the index whose definition is being loaded
     * @return the deserialized IndexDefinition instance representing the index
     */
    public static IndexDefinition loadIndexDefinition(Transaction tr, DirectorySubspace indexSubspace) {
        byte[] indexDefinitionKey = indexSubspace.pack(BucketMetadataMagic.INDEX_DEFINITION.getValue());
        byte[] value = tr.get(indexDefinitionKey).join();
        return JSONUtil.readValue(value, IndexDefinition.class);
    }

    /**
     * Saves the index definition to the specified index subspace.
     *
     * <p>This private helper method serializes the index definition to JSON and stores
     * it in FoundationDB under the INDEX_DEFINITION key within the index subspace.
     * This method is used internally during index creation to persist the definition.
     *
     * @param tr            the transaction instance used to interact with the database
     * @param definition    the index definition to be saved
     * @param indexSubspace the directory subspace for the index where the definition will be stored
     */
    private static void saveIndexDefinition(Transaction tr, IndexDefinition definition, DirectorySubspace indexSubspace) {
        byte[] indexDefinitionKey = indexSubspace.pack(BucketMetadataMagic.INDEX_DEFINITION.getValue());
        tr.set(indexDefinitionKey, JSONUtil.writeValueAsBytes(definition));
    }

    /**
     * Saves an updated index definition and increments the bucket metadata version.
     *
     * <p>This public method is used to update an existing index definition within the bucket.
     * It performs the following operations:
     * <ul>
     *   <li>Locates the index by its ID in the bucket metadata</li>
     *   <li>Saves the updated definition to the index subspace</li>
     *   <li>Increments the bucket metadata version to reflect the change</li>
     * </ul>
     *
     * <p>The version increment ensures that cached metadata is invalidated and clients
     * receive the updated index definition.
     *
     * @param tr         the transaction instance used to interact with the database
     * @param metadata   the bucket metadata containing the index to be updated
     * @param definition the updated index definition to be saved
     */
    public static void saveIndexDefinition(Transaction tr, BucketMetadata metadata, IndexDefinition definition) {
        Index index = metadata.indexes().getIndexById(definition.id(), IndexSelectionPolicy.ALL);
        saveIndexDefinition(tr, definition, index.subspace());
        BucketMetadataUtil.increaseVersion(tr, metadata.subspace(), POSITIVE_DELTA_ONE);
    }

    /**
     * Lists the subdirectories within the bucket metadata subspace's index directory.
     * This method constructs the subpath for the index directory, then fetches
     * the list of subdirectories using the DirectoryLayer. If the index directory does not exist,
     * a KronotopException is thrown.
     *
     * @param tr                     the transaction instance used to interact with the database
     * @param bucketMetadataSubspace the bucket metadata subspace serving as the base path for the index directory
     * @return a list of subdirectory names within the index directory
     * @throws KronotopException   if the index directory does not exist
     * @throws CompletionException if an error occurs during the operation
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
    public static byte[] getCardinalityKey(DirectorySubspace bucketMetadataSubspace, long indexId) {
        Tuple tuple = Tuple.from(
                BucketMetadataMagic.HEADER.getValue(),
                BucketMetadataMagic.INDEX_STATISTICS.getValue(),
                BucketMetadataMagic.INDEX_CARDINALITY.getValue(),
                indexId
        );
        return bucketMetadataSubspace.pack(tuple);
    }

    /**
     * Updates the cardinality value of a specified index within the given bucket metadata subspace.
     * This method modifies the cardinality metadata in the database by applying the specified delta.
     *
     * @param tr                     the transaction instance used to interact with the database
     * @param bucketMetadataSubspace the directory subspace representing the bucket's metadata
     * @param indexId                the unique identifier of the index whose cardinality is being updated
     * @param delta                  the byte array representing the delta to be applied (positive or negative)
     */
    private static void mutateCardinality(Transaction tr, DirectorySubspace bucketMetadataSubspace, long indexId, byte[] delta) {
        byte[] key = getCardinalityKey(bucketMetadataSubspace, indexId);
        tr.mutate(MutationType.ADD, key, delta);
    }

    /**
     * Updates the cardinality value of a specified index within the given bucket metadata subspace.
     * This method determines the appropriate delta value based on the input and modifies the
     * cardinality metadata in the database accordingly.
     *
     * @param tr                     the transaction instance used to interact with the database
     * @param bucketMetadataSubspace the directory subspace representing the bucket's metadata
     * @param indexId                the unique identifier of the index whose cardinality is being updated
     * @param delta                  the change in cardinality value, expressed as a long (positive or negative)
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
     * Clears an index and its associated metadata from a bucket metadata subspace.
     * This method removes the index definition, cardinality information, and the actual index subspace.
     *
     * @param tr                     the transaction instance used to interact with the database
     * @param bucketMetadataSubspace the bucket metadata subspace serving as the base path for the index
     * @param name                   the name of the index to be cleared
     * @throws KronotopException if the specified index does not exist
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
        byte[] cardinalityKey = getCardinalityKey(bucketMetadataSubspace, definition.id());
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
     * <p>The {@link IndexDropRoutine} waits 6 seconds for existing transactions to expire before
     * physically removing the index data via {@link #clear}.
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
        IndexUtil.saveIndexDefinition(tx.tr(), metadata, latestDef.updateStatus(IndexStatus.DROPPED));

        IndexDropTask task = new IndexDropTask(metadata.namespace(), metadata.name(), latestDef.id());
        byte[] definition = JSONUtil.writeValueAsBytes(task);

        BucketService service = tx.context().getService(BucketService.NAME);

        Random random = new Random();
        int shardId = random.nextInt(service.getNumberOfShards());
        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(tx.context(), shardId);
        TaskStorage.tasks(tx.tr(), taskSubspace, (taskId) -> {
            IndexBuildingTaskState.setStatus(tx.tr(), taskSubspace, taskId, IndexTaskStatus.STOPPED);
            return true;
        });
        TaskStorage.create(tx.tr(), tx.getAndIncreaseUserVersion(), taskSubspace, definition);
        TaskStorage.triggerWatchers(tx.tr(), taskSubspace);
    }
}
