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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.Arrays;

/**
 * Estimator for prefix histogram range selectivity calculations.
 * Implements the core algorithm for approximate range estimation over string or binary keys.
 */
public class PrefixHistogramEstimator {
    private final PrefixHistogramMetadata metadata;
    private final DirectorySubspace subspace;

    public PrefixHistogramEstimator(PrefixHistogramMetadata metadata, DirectorySubspace subspace) {
        this.metadata = metadata;
        this.subspace = subspace;
    }

    /**
     * Estimates the count of keys in the range [lowerBound, upperBound).
     *
     * @param tr Transaction to use for reads
     * @param lowerBound Lower bound (inclusive), null means -∞
     * @param upperBound Upper bound (exclusive), null means +∞
     * @return Estimated count of keys in the range
     */
    public long estimateRange(Transaction tr, byte[] lowerBound, byte[] upperBound) {
        // Handle special cases
        if (lowerBound == null && upperBound == null) {
            return getTotalCount(tr);
        }
        if (lowerBound != null && upperBound != null && Arrays.equals(lowerBound, upperBound)) {
            return 0; // Empty range
        }

        return estimateRangeRecursive(tr, lowerBound, upperBound, 1);
    }

    /**
     * Estimates count for inequality predicates
     */
    public long estimateLessThan(Transaction tr, byte[] value) {
        return estimateRange(tr, null, value);
    }

    public long estimateLessThanOrEqual(Transaction tr, byte[] value) {
        return estimateRange(tr, null, incrementKey(value));
    }

    public long estimateGreaterThan(Transaction tr, byte[] value) {
        return estimateRange(tr, incrementKey(value), null);
    }

    public long estimateGreaterThanOrEqual(Transaction tr, byte[] value) {
        return estimateRange(tr, value, null);
    }

    /**
     * Recursive range estimation algorithm following the specification
     */
    private long estimateRangeRecursive(Transaction tr, byte[] lowerBound, byte[] upperBound, int depth) {
        // Stop if we've reached maximum depth
        if (depth > metadata.maxDepth()) {
            return 0;
        }

        // Get byte values at current depth
        Integer lowerByte = getByteAtDepth(lowerBound, depth);
        Integer upperByte = getByteAtDepth(upperBound, depth);

        // Case 1: Both bounds are null at this depth (one or both keys are shorter)
        if (lowerByte == null && upperByte == null) {
            // Count all keys at this depth and below
            return getDepthTotalCount(tr, depth);
        }

        // Case 2: Lower bound is null, upper bound has a value
        if (lowerByte == null) {
            long count = 0;
            // Count all bins from 0 to upperByte-1
            for (int b = 0; b < upperByte; b++) {
                count += getBinCount(tr, depth, b);
            }
            // For the upperByte bin, recurse deeper
            count += estimateRangeRecursive(tr, null, upperBound, depth + 1);
            return count;
        }

        // Case 3: Upper bound is null, lower bound has a value
        if (upperByte == null) {
            long count = 0;
            // For the lowerByte bin, recurse deeper for the tail
            count += estimateRangeRecursive(tr, lowerBound, null, depth + 1);
            // Count all bins from lowerByte+1 to 255
            for (int b = lowerByte + 1; b <= 255; b++) {
                count += getBinCount(tr, depth, b);
            }
            return count;
        }

        // Case 4: Both bounds have values at this depth
        if (lowerByte.equals(upperByte)) {
            // Same byte value, recurse into next depth
            return estimateRangeRecursive(tr, lowerBound, upperBound, depth + 1);
        } else {
            // Different byte values
            long count = 0;

            // Tail of lower bound bin
            count += estimateRangeRecursive(tr, lowerBound, null, depth + 1);

            // Full counts from middle bins (lowerByte+1 to upperByte-1)
            for (int b = lowerByte + 1; b < upperByte; b++) {
                count += getBinCount(tr, depth, b);
            }

            // Head of upper bound bin
            count += estimateRangeRecursive(tr, null, upperBound, depth + 1);

            return count;
        }
    }

    /**
     * Gets the byte value at a specific depth (1-indexed), returns null if key is too short
     */
    private Integer getByteAtDepth(byte[] key, int depth) {
        if (key == null || key.length < depth) {
            return null;
        }
        return key[depth - 1] & 0xFF; // Convert to unsigned byte (0-255)
    }

    /**
     * Gets the count for a specific bin (depth, byte)
     */
    private long getBinCount(Transaction tr, int depth, int byteValue) {
        byte[] key = PrefixHistogramKeySchema.binCountKey(subspace, depth, byteValue);
        byte[] value = tr.get(key).join();
        return PrefixHistogramKeySchema.decodeCounterValue(value);
    }

    /**
     * Gets the total count of all bins at a specific depth
     */
    private long getDepthTotalCount(Transaction tr, int depth) {
        byte[] beginKey = PrefixHistogramKeySchema.depthRangeBegin(subspace, depth);
        byte[] endKey = PrefixHistogramKeySchema.depthRangeEnd(subspace, depth);

        long total = 0;
        for (KeyValue kv : tr.getRange(beginKey, endKey)) {
            total += PrefixHistogramKeySchema.decodeCounterValue(kv.getValue());
        }
        return total;
    }

    /**
     * Gets the total count across all depths and bins
     */
    private long getTotalCount(Transaction tr) {
        byte[] beginKey = PrefixHistogramKeySchema.allDataRangeBegin(subspace);
        byte[] endKey = PrefixHistogramKeySchema.allDataRangeEnd(subspace);

        long total = 0;
        for (KeyValue kv : tr.getRange(beginKey, endKey)) {
            // Each bin count represents entries that passed through that depth/byte
            // We need to count unique keys, so we sum counts only at depth 1
            Tuple tuple = subspace.unpack(kv.getKey());
            if (tuple.size() >= 4 && tuple.getLong(1) == 1) { // depth == 1
                total += PrefixHistogramKeySchema.decodeCounterValue(kv.getValue());
            }
        }
        return total;
    }

    /**
     * Increments a byte array by 1 for exclusive bound conversion
     * Returns null if overflow occurs (represents +∞)
     */
    private byte[] incrementKey(byte[] key) {
        if (key == null || key.length == 0) {
            return new byte[]{1}; // Minimum possible key
        }

        byte[] result = Arrays.copyOf(key, key.length);

        // Try to increment from the last byte
        for (int i = result.length - 1; i >= 0; i--) {
            if ((result[i] & 0xFF) < 255) {
                result[i]++;
                return result;
            } else {
                result[i] = 0; // Carry over
            }
        }

        // Overflow occurred, extend the array
        byte[] extended = new byte[result.length + 1];
        extended[0] = 1;
        System.arraycopy(result, 0, extended, 1, result.length);
        return extended;
    }
}