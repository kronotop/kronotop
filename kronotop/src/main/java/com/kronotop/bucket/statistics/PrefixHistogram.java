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

package com.kronotop.bucket.statistics;

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * FoundationDB-based prefix histogram implementation for approximate range selectivity estimation.
 * <p>
 * This implementation maintains counters for each byte value at each depth level of input keys:
 * - Each depth corresponds to one byte of the input key (max 8 bytes)
 * - Each bin is identified by (depth, byteValue)
 * - Writes increment counters along the path of the key
 * - Reads use these counters to approximate selectivity for range predicates
 * <p>
 * Key Schema:
 * /statistics/prefix_hist/D/{depth}/B/{byte} -> count
 * /statistics/prefix_hist/meta               -> JSON metadata
 * <p>
 * Key benefits:
 * - O(1) write operations using atomic ADD mutations (no reads required)
 * - Efficient range estimation for inequality predicates
 * - Fixed depth approach avoids complexity of dynamic structures
 * - ACID transactional consistency via FoundationDB
 */
public class PrefixHistogram {
    private final DirectorySubspace subspace;
    private final PrefixHistogramMetadata metadata;
    private final PrefixHistogramEstimator estimator;

    public PrefixHistogram(Transaction tr, List<String> root) {
        this.metadata = openMetadata(tr, root);
        this.subspace = openHistogramSubspace(tr, root);
        this.estimator = new PrefixHistogramEstimator(metadata, subspace);
    }

    public static void initialize(Transaction tr, List<String> root) {
        initialize(tr, root, PrefixHistogramMetadata.defaultMetadata());
    }

    public static void initialize(Transaction tr, List<String> root, PrefixHistogramMetadata metadata) {
        List<String> metaSubpath = new ArrayList<>(root);
        metaSubpath.addAll(Arrays.asList("statistics", "prefix_hist"));
        DirectorySubspace metaSubspace = DirectoryLayer.getDefault().createOrOpen(tr, metaSubpath).join();
        byte[] metaKey = PrefixHistogramKeySchema.metadataKey(metaSubspace);
        byte[] metaValue = PrefixHistogramKeySchema.encodeMetadata(metadata);
        tr.set(metaKey, metaValue);
    }

    private PrefixHistogramMetadata openMetadata(Transaction tr, List<String> root) {
        DirectorySubspace metaSubspace = openHistogramMetaSubspace(tr, root);
        byte[] metaData = tr.get(PrefixHistogramKeySchema.metadataKey(metaSubspace)).join();
        return PrefixHistogramKeySchema.decodeMetadata(metaData);
    }

    private DirectorySubspace openHistogramMetaSubspace(Transaction tr, List<String> root) {
        List<String> subpath = new ArrayList<>(root);
        subpath.addAll(Arrays.asList("statistics", "prefix_hist"));
        return DirectoryLayer.getDefault().open(tr, subpath).join();
    }

    private DirectorySubspace openHistogramSubspace(Transaction tr, List<String> root) {
        List<String> subpath = new ArrayList<>(root);
        subpath.addAll(Arrays.asList("statistics", "prefix_hist"));
        return DirectoryLayer.getDefault().open(tr, subpath).join();
    }

    /**
     * Adds a key within an existing transaction following the prefix histogram algorithm
     */
    public void add(Transaction tr, byte[] key) {
        if (key == null || key.length == 0) {
            return;
        }

        int maxDepth = Math.min(key.length, metadata.maxDepth());

        // Increment counters for each depth/byte combination along the key path
        for (int depth = 1; depth <= maxDepth; depth++) {
            int byteValue = key[depth - 1] & 0xFF; // Convert to unsigned byte (0-255)
            byte[] binKey = PrefixHistogramKeySchema.binCountKey(subspace, depth, byteValue);
            tr.mutate(MutationType.ADD, binKey, PrefixHistogramKeySchema.ONE_LE);
        }
    }

    /**
     * Deletes a key using atomic ADD(-1) operations - exact inverse of add
     */
    public void delete(Transaction tr, byte[] key) {
        if (key == null || key.length == 0) {
            return;
        }

        int maxDepth = Math.min(key.length, metadata.maxDepth());

        // Decrement counters for each depth/byte combination along the key path
        for (int depth = 1; depth <= maxDepth; depth++) {
            int byteValue = key[depth - 1] & 0xFF; // Convert to unsigned byte (0-255)
            byte[] binKey = PrefixHistogramKeySchema.binCountKey(subspace, depth, byteValue);
            tr.mutate(MutationType.ADD, binKey, PrefixHistogramKeySchema.NEGATIVE_ONE_LE);
        }
    }

    /**
     * Updates a key atomically (delete old + add new in a single transaction)
     */
    public void update(Transaction tr, byte[] oldKey, byte[] newKey) {
        if (Arrays.equals(oldKey, newKey)) {
            return; // No change needed
        }

        // Atomic delete old + add new
        if (oldKey != null && oldKey.length > 0) {
            delete(tr, oldKey);
        }
        if (newKey != null && newKey.length > 0) {
            add(tr, newKey);
        }
    }

    /**
     * Retrieves the PrefixHistogramEstimator instance associated with this histogram.
     *
     * @return the PrefixHistogramEstimator responsible for estimating histogram properties.
     */
    public PrefixHistogramEstimator getEstimator() {
        return estimator;
    }

    /**
     * Returns the metadata for this histogram
     */
    public PrefixHistogramMetadata getMetadata() {
        return metadata;
    }
}