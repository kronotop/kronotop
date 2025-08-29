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
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Utility class for encoding/decoding histogram keys and values in FoundationDB.
 * Implements the key schema design for log10 histogram storage.
 */
public class HistogramKeySchema {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    // Key constants
    public static final String COUNTS_PREFIX = "d";
    public static final String DECADE_SUM_SUFFIX = "sum";
    public static final String GROUP_SUM_PREFIX = "g";
    public static final String TOTAL_PREFIX = "total";
    public static final String UNDERFLOW_KEY = "underflow_sum";
    public static final String OVERFLOW_KEY = "overflow_sum";
    public static final String ZERO_OR_NEG_KEY = "zero_or_neg";
    public static final String METADATA_KEY = "meta";
    
    // Little-endian encoded 1 for atomic ADD operations
    public static final byte[] ONE_LE = encodeCounterValue(1L);
    
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
        try {
            return OBJECT_MAPPER.writeValueAsBytes(metadata);
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode histogram metadata", e);
        }
    }
    
    /**
     * Decodes histogram metadata from JSON
     */
    public static HistogramMetadata decodeMetadata(byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(data, HistogramMetadata.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode histogram metadata", e);
        }
    }
    
    /**
     * Creates key for individual bucket count: d/{decade}/j/{subBucket}
     */
    public static byte[] bucketCountKey(DirectorySubspace subspace, int decade, int subBucket) {
        return subspace.pack(Tuple.from(COUNTS_PREFIX, decade, "j", subBucket));
    }
    
    /**
     * Creates key for decade sum: d/{decade}/sum
     */
    public static byte[] decadeSumKey(DirectorySubspace subspace, int decade) {
        return subspace.pack(Tuple.from(COUNTS_PREFIX, decade, DECADE_SUM_SUFFIX));
    }
    
    /**
     * Creates key for group sum: d/{decade}/g/{group}
     */
    public static byte[] groupSumKey(DirectorySubspace subspace, int decade, int group) {
        return subspace.pack(Tuple.from(COUNTS_PREFIX, decade, GROUP_SUM_PREFIX, group));
    }
    
    /**
     * Creates key for total shard: total/{shardId}
     */
    public static byte[] totalShardKey(DirectorySubspace subspace, int shardId) {
        return subspace.pack(Tuple.from(TOTAL_PREFIX, shardId));
    }
    
    /**
     * Creates key for underflow summary
     */
    public static byte[] underflowSumKey(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(UNDERFLOW_KEY));
    }
    
    /**
     * Creates key for overflow summary
     */
    public static byte[] overflowSumKey(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(OVERFLOW_KEY));
    }
    
    /**
     * Creates key for zero/negative count
     */
    public static byte[] zeroOrNegKey(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(ZERO_OR_NEG_KEY));
    }
    
    /**
     * Creates key for metadata
     */
    public static byte[] metadataKey(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(METADATA_KEY));
    }
    
    /**
     * Creates range begin key for decade sums starting from decade
     */
    public static byte[] decadeSumRangeBegin(DirectorySubspace subspace, int fromDecade) {
        return subspace.pack(Tuple.from(COUNTS_PREFIX, fromDecade, DECADE_SUM_SUFFIX));
    }
    
    /**
     * Creates range end key for decade sums up to decade (exclusive)
     */
    public static byte[] decadeSumRangeEnd(DirectorySubspace subspace, int toDecade) {
        return subspace.pack(Tuple.from(COUNTS_PREFIX, toDecade, DECADE_SUM_SUFFIX));
    }
    
    /**
     * Creates range begin key for group sums starting from group
     */
    public static byte[] groupSumRangeBegin(DirectorySubspace subspace, int decade, int fromGroup) {
        return subspace.pack(Tuple.from(COUNTS_PREFIX, decade, GROUP_SUM_PREFIX, fromGroup));
    }
    
    /**
     * Creates range end key for group sums up to group (exclusive)
     */
    public static byte[] groupSumRangeEnd(DirectorySubspace subspace, int decade, int toGroup) {
        return subspace.pack(Tuple.from(COUNTS_PREFIX, decade, GROUP_SUM_PREFIX, toGroup));
    }
    
    /**
     * Creates range begin key for all decade entries
     */
    public static byte[] decadeRangeBegin(DirectorySubspace subspace, int decade) {
        return subspace.pack(Tuple.from(COUNTS_PREFIX, decade));
    }
    
    /**
     * Creates range end key for all decade entries (exclusive)
     */
    public static byte[] decadeRangeEnd(DirectorySubspace subspace, int decade) {
        return subspace.pack(Tuple.from(COUNTS_PREFIX, decade + 1));
    }
}