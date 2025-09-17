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
import com.kronotop.bucket.BucketMetadataMagic;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.internal.JSONUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
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
            byte[] indexDefinitionKey = indexSubspace.pack(BucketMetadataMagic.INDEX_DEFINITION.getValue());
            tr.set(indexDefinitionKey, JSONUtil.writeValueAsBytes(definition));
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
     * Generates a tuple representing the header, index statistics, index cardinality,
     * and a given identifier. This tuple is typically used for indexing metadata operations.
     *
     * @param id the unique identifier of the index whose cardinality tuple is being generated
     * @return a tuple containing the header, index statistics, index cardinality, and the provided identifier
     */
    private static Tuple getIndexCardinalityTuple(long id) {
        return Tuple.from(
                BucketMetadataMagic.HEADER.getValue(),
                BucketMetadataMagic.INDEX_STATISTICS.getValue(),
                BucketMetadataMagic.INDEX_CARDINALITY.getValue(),
                id
        );
    }

    private static byte[] getCardinalityKey(DirectorySubspace bucketMetadataSubspace, long indexId) {
        return bucketMetadataSubspace.pack(getIndexCardinalityTuple(indexId));
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
    private static void cardinalityCommon(Transaction tr, DirectorySubspace bucketMetadataSubspace, long indexId, byte[] delta) {
        byte[] key = getCardinalityKey(bucketMetadataSubspace, indexId);
        tr.mutate(MutationType.ADD, key, delta);
    }

    public static void cardinalityCommon(Transaction tr, DirectorySubspace bucketMetadataSubspace, long indexId, long delta) {
        byte[] key = getCardinalityKey(bucketMetadataSubspace, indexId);
        byte[] param = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(delta).array();
        tr.mutate(MutationType.ADD, key, param);
    }

    /**
     * Increases the cardinality of the specified index within the given bucket metadata subspace.
     * This method modifies the cardinality metadata in the database, incrementing it by a predefined positive delta.
     *
     * @param tr                     the transaction instance used to interact with the database
     * @param bucketMetadataSubspace the directory subspace representing the bucket's metadata
     * @param indexId                the unique identifier of the index whose cardinality is to be increased
     */
    public static void increaseCardinality(Transaction tr, DirectorySubspace bucketMetadataSubspace, long indexId) {
        cardinalityCommon(tr, bucketMetadataSubspace, indexId, POSITIVE_DELTA_ONE);
    }

    /**
     * Decreases the cardinality of the specified index within the given bucket metadata subspace.
     * This method modifies the cardinality metadata in the database, decrementing it by a predefined negative delta.
     *
     * @param tr                     the transaction instance used to interact with the database
     * @param bucketMetadataSubspace the directory subspace representing the bucket's metadata
     * @param indexId                the unique identifier of the index whose cardinality is to be decreased
     */
    public static void decreaseCardinality(Transaction tr, DirectorySubspace bucketMetadataSubspace, long indexId) {
        cardinalityCommon(tr, bucketMetadataSubspace, indexId, NEGATIVE_DELTA_ONE);
    }

    /**
     * Drops an index and its associated metadata from a bucket metadata subspace.
     * This method removes the index definition, cardinality information, and the actual index subspace.
     *
     * @param tr                     the transaction instance used to interact with the database
     * @param bucketMetadataSubspace the bucket metadata subspace serving as the base path for the index
     * @param name                   the name of the index to be dropped
     * @throws KronotopException if the specified index does not exist
     */
    public static void drop(Transaction tr, DirectorySubspace bucketMetadataSubspace, String name) {
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

        // Drop the index
        List<String> subpath = new ArrayList<>(bucketMetadataSubspace.getPath());
        subpath.add(BucketMetadataUtil.INDEXES_DIRECTORY);
        subpath.add(definition.name());
        DirectoryLayer.getDefault().remove(tr, subpath).join();
    }
}
