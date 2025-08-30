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
 * 
 * Key Schema:
 * HIST/<indexId>/L/<lowerBoundPad><depthByte> = <meta>          # Leaf boundary record
 * HIST/<indexId>/C/<lowerBoundPad><depthByte>/<shardId> = <i64> # Leaf counter shards
 * HIST/<indexId>/F/<lowerBoundPad><depthByte> = <flags>         # Leaf maintenance flags
 */
public class APPKeySchema {
    
    // Key prefixes for different data types
    public static final String LEAF_PREFIX = "L";           // Leaf boundary records
    public static final String COUNTER_PREFIX = "C";        // Counter shards
    public static final String FLAGS_PREFIX = "F";          // Maintenance flags
    public static final String METADATA_KEY = "meta";       // Histogram metadata
    
    // Maintenance flags
    public static final int FLAG_NEEDS_SPLIT = 0x01;
    public static final int FLAG_NEEDS_MERGE = 0x02;
    
    // Little-endian encoded values for atomic operations
    public static final byte[] ONE_LE = encodeCounterValue(1L);
    public static final byte[] NEGATIVE_ONE_LE = encodeCounterValue(-1L);

    /**
     * Pads a byte array to the specified length by right-padding with the given byte value.
     */
    public static byte[] rightPad(byte[] input, int targetLength, byte padByte) {
        if (input.length >= targetLength) {
            return Arrays.copyOf(input, targetLength);
        }
        
        byte[] padded = new byte[targetLength];
        System.arraycopy(input, 0, padded, 0, input.length);
        Arrays.fill(padded, input.length, targetLength, padByte);
        return padded;
    }

    /**
     * Creates canonicalized lower-bound padded key for comparisons.
     */
    public static byte[] canonicalizeLowerBound(byte[] key, int maxDepth) {
        return rightPad(key, maxDepth, (byte) 0x00);
    }

    /**
     * Creates canonicalized upper-bound padded key for comparisons.
     */
    public static byte[] canonicalizeUpperBound(byte[] key, int maxDepth) {
        return rightPad(key, maxDepth, (byte) 0xFF);
    }

    /**
     * Computes the upper bound for a leaf given its lower bound and depth.
     */
    public static byte[] computeUpperBound(byte[] lowerBound, int depth, int maxDepth) {
        // Special case for root leaf: it should cover the entire space
        if (depth == 1 && Arrays.equals(lowerBound, new byte[maxDepth])) {
            // Root leaf covers [0x00...00, 0xFF...FF] + 1
            // Since we can't represent 0xFF...FF + 1 in the same byte array,
            // we'll use a large enough upper bound
            byte[] upperBound = new byte[maxDepth];
            Arrays.fill(upperBound, (byte) 0xFF);
            
            // Add 1 to create exclusive upper bound
            return addOne(upperBound);
        }
        
        // For non-root leaves, use proper arithmetic
        long leafWidth = (long) Math.pow(256, maxDepth - depth);
        return addToByteArray(lowerBound, leafWidth);
    }

    /**
     * Adds 1 to a byte array (big-endian arithmetic).
     * Returns a special marker array if overflow occurs.
     */
    public static byte[] addOne(byte[] input) {
        byte[] result = Arrays.copyOf(input, input.length);
        
        // Add 1 starting from the least significant byte
        for (int i = result.length - 1; i >= 0; i--) {
            int sum = (result[i] & 0xFF) + 1;
            result[i] = (byte) (sum & 0xFF);
            
            if (sum <= 0xFF) {
                // No carry needed
                return result;
            }
            // Continue with carry (result[i] is now 0)
        }
        
        // Overflow occurred - return a larger array or special marker
        // For simplicity, return the original max value - contains() will handle this
        return input;
    }

    /**
     * Adds a long value to a byte array (big-endian arithmetic).
     */
    public static byte[] addToByteArray(byte[] input, long value) {
        byte[] result = Arrays.copyOf(input, input.length);
        
        // Add value starting from the least significant byte
        for (int i = result.length - 1; i >= 0 && value > 0; i--) {
            long sum = (result[i] & 0xFF) + (value & 0xFF);
            result[i] = (byte) (sum & 0xFF);
            value = (value >>> 8) + (sum >>> 8); // Carry
        }
        
        return result;
    }

    /**
     * Converts a padded byte array to a long value for arithmetic operations.
     * Only works correctly for byte arrays up to 8 bytes.
     */
    public static long bytesToLong(byte[] bytes) {
        long result = 0;
        for (int i = 0; i < Math.min(bytes.length, 8); i++) {
            result = (result << 8) | (bytes[i] & 0xFF);
        }
        return result;
    }

    /**
     * Converts a long value back to a padded byte array.
     */
    public static byte[] longToBytes(long value, int length) {
        byte[] result = new byte[length];
        for (int i = length - 1; i >= 0; i--) {
            result[i] = (byte) (value & 0xFF);
            value >>>= 8;
        }
        return result;
    }

    /**
     * Creates a compound key from padded lower bound and depth byte.
     */
    public static byte[] createCompoundKey(byte[] lowerBoundPad, int depth) {
        byte[] compound = new byte[lowerBoundPad.length + 1];
        System.arraycopy(lowerBoundPad, 0, compound, 0, lowerBoundPad.length);
        compound[compound.length - 1] = (byte) depth;
        return compound;
    }

    /**
     * Extracts the depth from a compound key.
     */
    public static int extractDepth(byte[] compoundKey) {
        return compoundKey[compoundKey.length - 1] & 0xFF;
    }

    /**
     * Extracts the padded lower bound from a compound key.
     */
    public static byte[] extractLowerBound(byte[] compoundKey) {
        return Arrays.copyOf(compoundKey, compoundKey.length - 1);
    }

    /**
     * Encodes a long value in little-endian format for FoundationDB counter mutations.
     */
    public static byte[] encodeCounterValue(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    /**
     * Decodes a little-endian long value from FoundationDB.
     */
    public static long decodeCounterValue(byte[] data) {
        if (data == null || data.length != 8) {
            return 0;
        }
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    /**
     * Encodes maintenance flags as a single byte.
     */
    public static byte[] encodeFlags(int flags) {
        return new byte[]{(byte) flags};
    }

    /**
     * Decodes maintenance flags from a byte array.
     */
    public static int decodeFlags(byte[] data) {
        if (data == null || data.length == 0) {
            return 0;
        }
        return data[0] & 0xFF;
    }

    /**
     * Encodes histogram metadata as JSON.
     */
    public static byte[] encodeMetadata(APPHistogramMetadata metadata) {
        return JSONUtil.writeValueAsBytes(metadata);
    }

    /**
     * Decodes histogram metadata from JSON.
     */
    public static APPHistogramMetadata decodeMetadata(byte[] data) {
        if (data == null) {
            return null;
        }
        return JSONUtil.readValue(data, APPHistogramMetadata.class);
    }

    /**
     * Creates key for leaf boundary record: L/<lowerBoundPad><depthByte>
     */
    public static byte[] leafBoundaryKey(DirectorySubspace subspace, byte[] lowerBoundPad, int depth) {
        byte[] compoundKey = createCompoundKey(lowerBoundPad, depth);
        return subspace.pack(Tuple.from(LEAF_PREFIX, compoundKey));
    }

    /**
     * Creates key for counter shard: C/<lowerBoundPad><depthByte>/<shardId>
     */
    public static byte[] counterShardKey(DirectorySubspace subspace, byte[] lowerBoundPad, int depth, int shardId) {
        byte[] compoundKey = createCompoundKey(lowerBoundPad, depth);
        return subspace.pack(Tuple.from(COUNTER_PREFIX, compoundKey, shardId));
    }

    /**
     * Creates key for maintenance flags: F/<lowerBoundPad><depthByte>
     */
    public static byte[] maintenanceFlagsKey(DirectorySubspace subspace, byte[] lowerBoundPad, int depth) {
        byte[] compoundKey = createCompoundKey(lowerBoundPad, depth);
        return subspace.pack(Tuple.from(FLAGS_PREFIX, compoundKey));
    }

    /**
     * Creates key for histogram metadata.
     */
    public static byte[] metadataKey(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(METADATA_KEY));
    }

    /**
     * Creates range begin key for reverse leaf lookup.
     */
    public static byte[] leafLookupRangeBegin(DirectorySubspace subspace, byte[] keyPad) {
        // Create a compound key with max depth byte for reverse lookup
        byte[] compoundKey = createCompoundKey(keyPad, 0xFF);
        return subspace.pack(Tuple.from(LEAF_PREFIX, compoundKey));
    }

    /**
     * Creates range end key for reverse leaf lookup.
     */
    public static byte[] leafLookupRangeEnd(DirectorySubspace subspace) {
        return subspace.pack(Tuple.from(LEAF_PREFIX));
    }

    /**
     * Creates range begin key for counter shard range operations.
     */
    public static byte[] counterRangeBegin(DirectorySubspace subspace, byte[] lowerBoundPad, int depth) {
        byte[] compoundKey = createCompoundKey(lowerBoundPad, depth);
        return subspace.pack(Tuple.from(COUNTER_PREFIX, compoundKey));
    }

    /**
     * Creates range end key for counter shard range operations.
     */
    public static byte[] counterRangeEnd(DirectorySubspace subspace, byte[] lowerBoundPad, int depth) {
        byte[] compoundKey = createCompoundKey(lowerBoundPad, depth);
        // Create a range end by using the next possible compound key
        byte[] compoundKeyEnd = new byte[compoundKey.length + 1];
        System.arraycopy(compoundKey, 0, compoundKeyEnd, 0, compoundKey.length);
        compoundKeyEnd[compoundKey.length] = 0; // Add null terminator
        return subspace.pack(Tuple.from(COUNTER_PREFIX, compoundKeyEnd));
    }
}