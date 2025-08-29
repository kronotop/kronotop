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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.internal.JSONUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Utility class for encoding/decoding histogram keys and values in FoundationDB.
 * Implements the key schema design for log10 histogram storage.
 */
public class HistogramKeySchema {
    // Key constants
    public static final String COUNTS_PREFIX = "d";
    public static final String DECADE_SUM_SUFFIX = "sum";
    public static final String GROUP_SUM_PREFIX = "g";
    public static final String TOTAL_PREFIX = "total";
    public static final String UNDERFLOW_KEY = "underflow_sum";
    public static final String OVERFLOW_KEY = "overflow_sum";
    public static final String ZERO_COUNT_KEY = "zero_count";
    public static final String METADATA_KEY = "meta";
    // Histogram type prefixes
    public static final String POS_HIST_PREFIX = "pos";
    public static final String NEG_HIST_PREFIX = "neg";
    // Little-endian encoded values for atomic ADD operations
    public static final byte[] ONE_LE = encodeCounterValue(1L);
    public static final byte[] NEGATIVE_ONE_LE = encodeCounterValue(-1L);

    /**
     * Encodes a long value in little-endian format for FoundationDB counter mutations
     */
    public static byte[] encodeCounterValue(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    /**
     * Decodes a little-endian long value from FoundationDB
     */
    public static long decodeCounterValue(byte[] data) {
        if (data == null || data.length != 8) {
            return 0;
        }
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    /**
     * Encodes histogram metadata as JSON
     */
    public static byte[] encodeMetadata(HistogramMetadata metadata) {
        return JSONUtil.writeValueAsBytes(metadata);
    }

    /**
     * Decodes histogram metadata from JSON
     */
    public static HistogramMetadata decodeMetadata(byte[] data) {
        if (data == null) {
            return null;
        }
        return JSONUtil.readValue(data, HistogramMetadata.class);
    }

    /**
     * Creates key for individual bucket count: {histType}/d/{decade}/j/{subBucket}
     */
    public static byte[] bucketCountKey(DirectorySubspace subspace, String histType, int decade, int subBucket) {
        return subspace.pack(Tuple.from(histType, COUNTS_PREFIX, decade, "j", subBucket));
    }

    /**
     * Creates key for decade sum: {histType}/d/{decade}/sum
     */
    public static byte[] decadeSumKey(DirectorySubspace subspace, String histType, int decade) {
        return subspace.pack(Tuple.from(histType, COUNTS_PREFIX, decade, DECADE_SUM_SUFFIX));
    }

    /**
     * Creates key for group sum: {histType}/d/{decade}/g/{group}
     */
    public static byte[] groupSumKey(DirectorySubspace subspace, String histType, int decade, int group) {
        return subspace.pack(Tuple.from(histType, COUNTS_PREFIX, decade, GROUP_SUM_PREFIX, group));
    }

    /**
     * Creates key for histogram-specific total shard: {histType}/total/{shardId}
     */
    public static byte[] totalShardKey(DirectorySubspace subspace, String histType, int shardId) {
        return subspace.pack(Tuple.from(histType, TOTAL_PREFIX, shardId));
    }

    /**
     * Creates key for underflow summary: {histType}/underflow_sum
     */
    public static byte[] underflowSumKey(DirectorySubspace subspace, String histType) {
        return subspace.pack(Tuple.from(histType, UNDERFLOW_KEY));
    }

    /**
     * Creates key for overflow summary: {histType}/overflow_sum
     */
    public static byte[] overflowSumKey(DirectorySubspace subspace, String histType) {
        return subspace.pack(Tuple.from(histType, OVERFLOW_KEY));
    }

    /**
     * Creates key for zero count
     */
    public static byte[] zeroCountKey(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(ZERO_COUNT_KEY));
    }

    /**
     * Creates key for metadata
     */
    public static byte[] metadataKey(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(METADATA_KEY));
    }

    /**
     * Creates range begin key for all histogram entries of a given type
     */
    public static byte[] histogramTypeRangeBegin(DirectorySubspace subspace, String histType) {
        return subspace.pack(Tuple.from(histType));
    }

    /**
     * Creates range end key for all histogram entries of a given type (exclusive)
     */
    public static byte[] histogramTypeRangeEnd(DirectorySubspace subspace, String histType) {
        return subspace.pack(Tuple.from(histType + "\u0000"));
    }

    /**
     * Creates range begin key for all decade entries of a histogram type
     */
    public static byte[] decadeRangeBegin(DirectorySubspace subspace, String histType, int decade) {
        return subspace.pack(Tuple.from(histType, COUNTS_PREFIX, decade));
    }

    /**
     * Creates range end key for all decade entries of a histogram type (exclusive)
     */
    public static byte[] decadeRangeEnd(DirectorySubspace subspace, String histType, int decade) {
        return subspace.pack(Tuple.from(histType, COUNTS_PREFIX, decade + 1));
    }
}