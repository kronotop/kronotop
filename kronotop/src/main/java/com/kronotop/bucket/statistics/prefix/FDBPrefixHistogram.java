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

package com.kronotop.bucket.statistics.prefix;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;

import java.util.Arrays;

/**
 * FoundationDB-based Fixed N-Byte Prefix Histogram implementation.
 * <p>
 * Key features:
 * - O(1) write operations using atomic ADD mutations (no reads required)
 * - No sharding - simple atomic counters per prefix bucket
 * - Peek-first optimization for equality queries
 * - (N+1)th byte fractioning for range estimation
 * <p>
 * Key Schema: HIST/<idx>/N/<N>/T/<pN> = <i64>
 * Where:
 * - pN: first N bytes of the value (right-padded with 0x00 if shorter)
 * - i64: little-endian counter value
 */
public class FDBPrefixHistogram {
    private Database database;
    private Subspace histogramSubspace;

    public FDBPrefixHistogram(Database database) {
        this.database = database;
    }

    public FDBPrefixHistogram(Subspace histogramSubspace) {
        this.histogramSubspace = histogramSubspace;
    }

    public static void create(Transaction tr, Subspace subspace) {
        byte[] metaKey = PrefixHistogramKeySchema.metadataKey(subspace);
        byte[] metaValue = PrefixHistogramKeySchema.encodeMetadata(PrefixHistogramMetadata.defaultMetadata());
        tr.set(metaKey, metaValue);
    }

    public Database getDatabase() {
        return database;
    }

    /**
     * Initializes a histogram for the given index with specified metadata.
     * This should be called once during index setup.
     */
    public void initialize(String indexName, PrefixHistogramMetadata metadata) {
        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace metaSubspace = getHistogramMetaSubspace(tr, indexName);
            byte[] metaKey = PrefixHistogramKeySchema.metadataKey(metaSubspace);
            byte[] metaValue = PrefixHistogramKeySchema.encodeMetadata(metadata);
            tr.set(metaKey, metaValue);
            tr.commit().join();
        }
    }

    /**
     * Adds a value to the histogram using atomic mutations (O(1), read-free).
     */
    public void add(String indexName, byte[] key) {
        PrefixHistogramMetadata metadata = getMetadata(indexName);
        if (metadata == null) {
            metadata = PrefixHistogramMetadata.defaultMetadata();
            initialize(indexName, metadata);
        }

        try (Transaction tr = database.createTransaction()) {
            addValue(tr, indexName, key, metadata);
            tr.commit().join();
        }
    }

    /**
     * Adds a value within an existing transaction
     */
    public void addValue(Transaction tr, String indexName, byte[] key, PrefixHistogramMetadata metadata) {
        DirectorySubspace subspace = getHistogramSubspace(tr, indexName, metadata.N());

        // Compute prefix bucket
        byte[] pN = PrefixHistogramUtils.pN(key, metadata.N());

        // Atomic ADD operation (no reads required)
        tr.mutate(MutationType.ADD,
                PrefixHistogramKeySchema.bucketCountKey(subspace, pN),
                PrefixHistogramKeySchema.ONE_LE);
    }

    /**
     * Deletes a value using atomic ADD(-1) operation - exact inverse of add
     */
    public void delete(String indexName, byte[] key) {
        PrefixHistogramMetadata metadata = getMetadata(indexName);
        if (metadata == null) {
            return; // Nothing to delete if histogram doesn't exist
        }

        try (Transaction tr = database.createTransaction()) {
            deleteValue(tr, indexName, key, metadata);
            tr.commit().join();
        }
    }

    /**
     * Deletes a value within an existing transaction
     */
    public void deleteValue(Transaction tr, String indexName, byte[] key, PrefixHistogramMetadata metadata) {
        DirectorySubspace subspace = getHistogramSubspace(tr, indexName, metadata.N());

        // Compute prefix bucket
        byte[] pN = PrefixHistogramUtils.pN(key, metadata.N());

        // Atomic ADD(-1) operation - exact inverse of add
        tr.mutate(MutationType.ADD,
                PrefixHistogramKeySchema.bucketCountKey(subspace, pN),
                PrefixHistogramKeySchema.NEGATIVE_ONE_LE);
    }

    /**
     * Updates a value atomically (delete old + add new in single transaction)
     */
    public void update(String indexName, byte[] oldKey, byte[] newKey) {
        if (Arrays.equals(oldKey, newKey)) {
            return; // No change needed
        }

        PrefixHistogramMetadata metadata = getMetadata(indexName);
        if (metadata == null) {
            metadata = PrefixHistogramMetadata.defaultMetadata();
            initialize(indexName, metadata);
        }

        try (Transaction tr = database.createTransaction()) {
            updateValue(tr, indexName, oldKey, newKey, metadata);
            tr.commit().join();
        }
    }

    /**
     * Updates a value within an existing transaction
     */
    public void updateValue(Transaction tr, String indexName, byte[] oldKey, byte[] newKey, PrefixHistogramMetadata metadata) {
        if (Arrays.equals(oldKey, newKey)) {
            return; // No change needed
        }

        // Atomic delete old + add new
        deleteValue(tr, indexName, oldKey, metadata);
        addValue(tr, indexName, newKey, metadata);
    }

    /**
     * Gets histogram metadata, returning null if not found
     */
    public PrefixHistogramMetadata getMetadata(String indexName) {
        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace metaSubspace = getHistogramMetaSubspace(tr, indexName);
            byte[] metaData = tr.get(PrefixHistogramKeySchema.metadataKey(metaSubspace)).join();
            return PrefixHistogramKeySchema.decodeMetadata(metaData);
        }
    }

    /**
     * Creates histogram estimator for selectivity calculations
     */
    public PrefixHistogramEstimator createEstimator(String indexName) {
        return new PrefixHistogramEstimator(this, indexName);
    }

    /**
     * Gets or creates the histogram subspace for given index and N value
     * Schema: HIST/<indexName>/N/<N>/
     */
    public DirectorySubspace getHistogramSubspace(Transaction tr, String indexName, int N) {
        return DirectoryLayer.getDefault().createOrOpen(tr, Arrays.asList(
                PrefixHistogramKeySchema.HIST_PREFIX, indexName,
                PrefixHistogramKeySchema.N_PREFIX, String.valueOf(N)
        )).join();
    }

    /**
     * Gets or creates the histogram metadata subspace (independent of N parameter)
     * Schema: HIST/<indexName>/
     */
    public DirectorySubspace getHistogramMetaSubspace(Transaction tr, String indexName) {
        return DirectoryLayer.getDefault().createOrOpen(tr, Arrays.asList(
                PrefixHistogramKeySchema.HIST_PREFIX, indexName
        )).join();
    }

    /**
     * Gets bucket count for a specific prefix (for testing/debugging)
     */
    public long getBucketCount(String indexName, byte[] pN) {
        PrefixHistogramMetadata metadata = getMetadata(indexName);
        if (metadata == null) {
            return 0;
        }

        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace subspace = getHistogramSubspace(tr, indexName, metadata.N());
            byte[] data = tr.get(PrefixHistogramKeySchema.bucketCountKey(subspace, pN)).join();
            return PrefixHistogramKeySchema.decodeCounterValue(data);
        }
    }

    /**
     * Gets total count across all buckets (for testing/debugging)
     */
    public long getTotalCount(String indexName) {
        PrefixHistogramMetadata metadata = getMetadata(indexName);
        if (metadata == null) {
            return 0;
        }

        try (Transaction tr = database.createTransaction()) {
            DirectorySubspace subspace = getHistogramSubspace(tr, indexName, metadata.N());

            long total = 0;
            byte[] beginKey = PrefixHistogramKeySchema.bucketCountRangeBegin(subspace);
            byte[] endKey = PrefixHistogramKeySchema.bucketCountRangeEnd(subspace);

            for (var kv : tr.getRange(beginKey, endKey)) {
                System.out.println(kv);
                total += PrefixHistogramKeySchema.decodeCounterValue(kv.getValue());
            }

            return total;
        }
    }
}