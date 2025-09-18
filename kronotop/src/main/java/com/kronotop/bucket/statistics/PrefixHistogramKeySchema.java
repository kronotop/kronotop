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
 * Utility class for encoding/decoding prefix histogram keys and values in FoundationDB.
 * Implements the key schema: HIST/<index>/D/<depth>/B/<byte> → counter
 */
public class PrefixHistogramKeySchema {
    // Key constants
    public static final String DEPTH_PREFIX = "D";
    public static final String BYTE_PREFIX = "B";
    public static final String METADATA_KEY = "meta";

    // Little-endian encoded values for atomic ADD operations
    public static final byte[] ONE_LE = encodeCounterValue(1L);
    public static final byte[] NEGATIVE_ONE_LE = encodeCounterValue(-1L);

    /**
     * Encodes a long value in little-endian format for FoundationDB counter-mutations
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
     * Encodes prefix histogram metadata as JSON
     */
    public static byte[] encodeMetadata(PrefixHistogramMetadata metadata) {
        return JSONUtil.writeValueAsBytes(metadata);
    }

    /**
     * Decodes prefix histogram metadata from JSON
     */
    public static PrefixHistogramMetadata decodeMetadata(byte[] data) {
        if (data == null) {
            return null;
        }
        return JSONUtil.readValue(data, PrefixHistogramMetadata.class);
    }

    /**
     * Creates key for individual bin count: D/<depth>/B/<byte>
     */
    public static byte[] binCountKey(DirectorySubspace subspace, int depth, int byteValue) {
        return subspace.pack(Tuple.from(DEPTH_PREFIX, depth, BYTE_PREFIX, byteValue));
    }

    /**
     * Creates key for metadata
     */
    public static byte[] metadataKey(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(METADATA_KEY));
    }

    /**
     * Creates range begin key for all bins at a specific depth
     */
    public static byte[] depthRangeBegin(DirectorySubspace subspace, int depth) {
        return subspace.pack(Tuple.from(DEPTH_PREFIX, depth, BYTE_PREFIX));
    }

    /**
     * Creates range end key for all bins at a specific depth (exclusive)
     */
    public static byte[] depthRangeEnd(DirectorySubspace subspace, int depth) {
        return subspace.pack(Tuple.from(DEPTH_PREFIX, depth, BYTE_PREFIX + "\u0000"));
    }

    /**
     * Creates range begin key for all histogram data (excluding metadata)
     */
    public static byte[] allDataRangeBegin(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(DEPTH_PREFIX));
    }

    /**
     * Creates range end key for all histogram data (exclusive)
     */
    public static byte[] allDataRangeEnd(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(DEPTH_PREFIX + "\u0000"));
    }
}