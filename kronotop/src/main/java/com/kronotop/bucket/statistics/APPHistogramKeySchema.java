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
import java.util.Arrays;

/**
 * Utility class for encoding/decoding APP histogram keys and values in FoundationDB.
 * Implements the key schema design for Adaptive Prefix Partitioning histogram storage.
 * <p>
 * Key Schema:
 * HIST/<indexId>/L/<lowerPad><depthByte>                = <meta>   # leaf boundary record
 * HIST/<indexId>/C/<lowerPad><depthByte>/<shardId>      = <i64>    # leaf counter shards
 * HIST/<indexId>/F/<lowerPad><depthByte>                = <flags>  # maintenance flags
 * IDX/<indexId>/<valueBytes>/<docRef>                   = ø        # existing index entries
 */
public class APPHistogramKeySchema {

    // Key prefixes
    public static final String LEAF_BOUNDARY_PREFIX = "L";
    public static final String COUNTER_PREFIX = "C";
    public static final String FLAGS_PREFIX = "F";
    public static final String METADATA_KEY = "meta";

    // Maintenance flags (bitfield)
    public static final int NEEDS_SPLIT_FLAG = 1;
    public static final int NEEDS_MERGE_FLAG = 2;
    public static final int HOT_SHARDED_FLAG = 4;

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
        return HistogramKeySchema.decodeCounterValue(data);
    }

    /**
     * Encodes APP histogram metadata as JSON
     */
    public static byte[] encodeMetadata(APPHistogramMetadata metadata) {
        return JSONUtil.writeValueAsBytes(metadata);
    }

    /**
     * Decodes APP histogram metadata from JSON
     */
    public static APPHistogramMetadata decodeMetadata(byte[] data) {
        if (data == null) {
            return null;
        }
        return JSONUtil.readValue(data, APPHistogramMetadata.class);
    }

    /**
     * Right-pads a byte array to the specified length with the given pad byte.
     * If input is longer than targetLength, it's truncated.
     */
    public static byte[] rightPad(byte[] input, int targetLength, byte padByte) {
        if (input == null) {
            input = new byte[0];
        }

        byte[] result = new byte[targetLength];
        int copyLength = Math.min(input.length, targetLength);
        System.arraycopy(input, 0, result, 0, copyLength);

        // Fill remaining bytes with pad byte
        for (int i = copyLength; i < targetLength; i++) {
            result[i] = padByte;
        }

        return result;
    }

    /**
     * Creates a leaf identifier by combining lowerPad and depth into a single byte array
     */
    public static byte[] createLeafId(byte[] lowerPad, int depth) {
        byte[] leafId = new byte[lowerPad.length + 1];
        System.arraycopy(lowerPad, 0, leafId, 0, lowerPad.length);
        leafId[lowerPad.length] = (byte) depth;
        return leafId;
    }

    /**
     * Extracts the lower bound from a leaf identifier
     */
    public static byte[] extractLowerBound(byte[] leafId) {
        if (leafId == null || leafId.length <= 1) {
            return new byte[0];
        }
        return Arrays.copyOf(leafId, leafId.length - 1);
    }

    /**
     * Extracts the depth from a leaf identifier
     */
    public static int extractDepth(byte[] leafId) {
        if (leafId == null || leafId.length == 0) {
            return 1; // Default depth
        }
        return leafId[leafId.length - 1] & 0xFF;
    }

    /**
     * Creates key for leaf boundary record: L/<lowerPad><depthByte>
     */
    public static byte[] leafBoundaryKey(DirectorySubspace subspace, byte[] lowerPad, int depth) {
        byte[] leafId = createLeafId(lowerPad, depth);
        return subspace.pack(Tuple.from(LEAF_BOUNDARY_PREFIX, leafId));
    }

    /**
     * Creates key for leaf counter shard: C/<lowerPad><depthByte>/<shardId>
     */
    public static byte[] leafCounterKey(DirectorySubspace subspace, byte[] lowerPad, int depth, int shardId) {
        byte[] leafId = createLeafId(lowerPad, depth);
        return subspace.pack(Tuple.from(COUNTER_PREFIX, leafId, shardId));
    }

    /**
     * Creates key for maintenance flags: F/<lowerPad><depthByte>
     */
    public static byte[] leafFlagsKey(DirectorySubspace subspace, byte[] lowerPad, int depth) {
        byte[] leafId = createLeafId(lowerPad, depth);
        return subspace.pack(Tuple.from(FLAGS_PREFIX, leafId));
    }

    /**
     * Creates key for metadata
     */
    public static byte[] metadataKey(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(METADATA_KEY));
    }

    /**
     * Creates range begin key for reverse scanning leaf boundaries up to searchKey
     */
    public static byte[] leafBoundaryRangeBegin(DirectorySubspace subspace, byte[] searchKey) {
        // For reverse scan: we need to find the leaf boundary <= searchKey
        // Create a leaf ID with searchKey + maximum depth byte (0xFF)
        byte[] searchLeafId = new byte[searchKey.length + 1];
        System.arraycopy(searchKey, 0, searchLeafId, 0, searchKey.length);
        searchLeafId[searchKey.length] = (byte) 0xFF; // Maximum possible depth

        return subspace.pack(Tuple.from(LEAF_BOUNDARY_PREFIX, searchLeafId));
    }

    /**
     * Creates range end key for reverse scanning leaf boundaries (prefix start)
     */
    public static byte[] leafBoundaryRangeEnd(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(LEAF_BOUNDARY_PREFIX));
    }

    /**
     * Creates range begin key for all counter shards of a leaf
     */
    public static byte[] leafCounterRangeBegin(DirectorySubspace subspace, byte[] lowerPad, int depth) {
        byte[] leafId = createLeafId(lowerPad, depth);
        return subspace.pack(Tuple.from(COUNTER_PREFIX, leafId));
    }

    /**
     * Creates range end key for all counter shards of a leaf (exclusive)
     */
    public static byte[] leafCounterRangeEnd(DirectorySubspace subspace, byte[] lowerPad, int depth) {
        byte[] leafId = createLeafId(lowerPad, depth);
        return subspace.pack(Tuple.from(COUNTER_PREFIX, leafId, Integer.MAX_VALUE));
    }

    /**
     * Creates range begin key for index entries within a leaf's range
     */
    public static byte[] indexRangeBegin(DirectorySubspace indexSubspace, byte[] lowerPad) {
        return indexSubspace.pack(Tuple.from((Object) lowerPad));
    }

    /**
     * Creates range end key for index entries within a leaf's range (exclusive)
     */
    public static byte[] indexRangeEnd(DirectorySubspace indexSubspace, byte[] upperPad) {
        return indexSubspace.pack(Tuple.from((Object) upperPad));
    }

    /**
     * Encodes maintenance flags as a single byte
     */
    public static byte[] encodeFlags(int flags) {
        return new byte[]{(byte) (flags & 0xFF)};
    }

    /**
     * Decodes maintenance flags from a byte array
     */
    public static int decodeFlags(byte[] data) {
        if (data == null || data.length == 0) {
            return 0;
        }
        return data[0] & 0xFF;
    }

    /**
     * Encodes leaf metadata (currently just depth, could include checksum later)
     */
    public static byte[] encodeLeafMetadata(int depth) {
        return new byte[]{(byte) (depth & 0xFF)};
    }

    /**
     * Decodes leaf metadata
     */
    public static int decodeLeafMetadata(byte[] data) {
        if (data == null || data.length == 0) {
            return 1; // Default depth
        }
        return data[0] & 0xFF;
    }
}