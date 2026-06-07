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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketMetadataMagic;
import com.kronotop.bucket.BucketMetadataUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CompletionException;

import static com.kronotop.bucket.BucketMetadataUtil.NEGATIVE_DELTA_ONE;
import static com.kronotop.bucket.BucketMetadataUtil.POSITIVE_DELTA_ONE;

/**
 * Shared index utilities used across all index types (single-field, compound, vector).
 */
public class IndexUtil {

    /**
     * Opens the existing index directory subspace by name.
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace as a base path
     * @param name                   index name
     * @return index directory subspace
     * @throws NoSuchIndexException if index does not exist
     * @throws CompletionException  on directory operation errors
     */
    public static DirectorySubspace open(ReadTransaction tr, DirectorySubspace bucketMetadataSubspace, String name) {
        try {
            return bucketMetadataSubspace.open(tr, List.of(BucketMetadataUtil.INDEXES_DIRECTORY, name)).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchIndexException(name);
            }
            throw e;
        }
    }

    /**
     * Lists all index names in the bucket.
     *
     * @param tr                     transaction for database operations
     * @param bucketMetadataSubspace bucket metadata subspace
     * @return list of index names (directory names under indexes/)
     * @throws KronotopException   if the indexes directory does not exist
     * @throws CompletionException on directory operation errors
     */
    public static List<String> list(ReadTransaction tr, DirectorySubspace bucketMetadataSubspace) {
        try {
            return bucketMetadataSubspace.list(tr, List.of(BucketMetadataUtil.INDEXES_DIRECTORY)).join();
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
     * @param bucketMetadataSubspace bucket metadata subspace
     * @param indexId                index identifier
     * @return byte array key for the cardinality value
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
     * Atomically modifies index cardinality using FoundationDB ADD mutation.
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
     * Atomically modifies index cardinality by specified delta using FoundationDB ADD mutation.
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
}
