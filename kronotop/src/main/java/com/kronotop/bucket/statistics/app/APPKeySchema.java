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

package com.kronotop.bucket.statistics.app;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Key schema for Adaptive Prefix Partitioning (APP) histogram storage in FoundationDB.
 * 
 * Implements the key layout from the APP specification:
 * - HIST/<indexId>/L/<lowerPad><depthByte> = <meta>   # leaf boundary record
 * - HIST/<indexId>/C/<lowerPad><depthByte>/<shardId> = <i64>  # leaf counter shards
 * - HIST/<indexId>/F/<lowerPad><depthByte> = <flags>  # maintenance flags
 * - IDX/<indexId>/<valueBytes>/<docRef> = ø          # existing index entries
 * 
 * All keys are constructed using FoundationDB's Tuple layer for proper ordering.
 */
public final class APPKeySchema {
    
    // Key prefixes
    public static final String HIST_PREFIX = "HIST";
    public static final String LEAF_PREFIX = "L";
    public static final String COUNTER_PREFIX = "C"; 
    public static final String FLAGS_PREFIX = "F";
    public static final String INDEX_PREFIX = "IDX";
    
    // Little-endian encoded values for atomic operations
    public static final byte[] ONE_LE = encodeCounterValue(1L);
    public static final byte[] NEGATIVE_ONE_LE = encodeCounterValue(-1L);
    
    private APPKeySchema() {
        // Utility class
    }
    
    /**
     * Creates the key for a leaf boundary record.
     * Format: HIST/<indexId>/L/<lowerPad><depthByte>
     * 
     * @param subspace The histogram subspace 
     * @param indexId The index identifier
     * @param leafId The leaf identifier containing lowerPad and depth
     * @return FoundationDB key for the leaf boundary
     */
    public static byte[] leafBoundaryKey(DirectorySubspace subspace, String indexId, APPLeafId leafId) {
        byte[] keyPart = createLeafKeyPart(leafId);
        return subspace.pack(Tuple.from(HIST_PREFIX, indexId, LEAF_PREFIX).addAll(Tuple.fromBytes(keyPart)));
    }
    
    /**
     * Creates the key for a leaf counter shard.
     * Format: HIST/<indexId>/C/<lowerPad><depthByte>/<shardId>
     * 
     * @param subspace The histogram subspace
     * @param indexId The index identifier  
     * @param leafId The leaf identifier
     * @param shardId The shard identifier (0 for unsharded)
     * @return FoundationDB key for the counter shard
     */
    public static byte[] counterKey(DirectorySubspace subspace, String indexId, APPLeafId leafId, int shardId) {
        byte[] keyPart = createLeafKeyPart(leafId);
        return subspace.pack(Tuple.from(HIST_PREFIX, indexId, COUNTER_PREFIX)
            .addAll(Tuple.fromBytes(keyPart))
            .add(shardId));
    }
    
    /**
     * Creates the key for leaf maintenance flags.
     * Format: HIST/<indexId>/F/<lowerPad><depthByte>
     * 
     * @param subspace The histogram subspace
     * @param indexId The index identifier
     * @param leafId The leaf identifier
     * @return FoundationDB key for the flags
     */
    public static byte[] flagsKey(DirectorySubspace subspace, String indexId, APPLeafId leafId) {
        byte[] keyPart = createLeafKeyPart(leafId);
        return subspace.pack(Tuple.from(HIST_PREFIX, indexId, FLAGS_PREFIX).addAll(Tuple.fromBytes(keyPart)));
    }
    
    /**
     * Creates the key for an index entry.
     * Format: IDX/<indexId>/<valueBytes>/<docRef>
     * 
     * @param subspace The histogram subspace
     * @param indexId The index identifier
     * @param valueBytes The indexed value as bytes
     * @param docRef The document reference
     * @return FoundationDB key for the index entry
     */
    public static byte[] indexEntryKey(DirectorySubspace subspace, String indexId, byte[] valueBytes, byte[] docRef) {
        return subspace.pack(Tuple.from(INDEX_PREFIX, indexId)
            .addAll(Tuple.fromBytes(valueBytes))
            .addAll(Tuple.fromBytes(docRef)));
    }
    
    /**
     * Creates a range begin key for scanning leaf boundaries.
     * Used for finding leaves containing a specific key.
     * 
     * @param subspace The histogram subspace
     * @param indexId The index identifier
     * @return Range begin key for all leaf boundaries
     */
    public static byte[] leafBoundaryRangeBegin(DirectorySubspace subspace, String indexId) {
        return subspace.pack(Tuple.from(HIST_PREFIX, indexId, LEAF_PREFIX));
    }
    
    /**
     * Creates a range end key for scanning leaf boundaries (exclusive).
     * 
     * @param subspace The histogram subspace  
     * @param indexId The index identifier
     * @return Range end key for all leaf boundaries
     */
    public static byte[] leafBoundaryRangeEnd(DirectorySubspace subspace, String indexId) {
        return subspace.pack(Tuple.from(HIST_PREFIX, indexId, LEAF_PREFIX + "\u0000"));
    }
    
    /**
     * Creates a range begin key for scanning index entries within a range.
     * 
     * @param subspace The histogram subspace
     * @param indexId The index identifier  
     * @param lowerBound The lower bound of the range (inclusive)
     * @return Range begin key for index scanning
     */
    public static byte[] indexRangeBegin(DirectorySubspace subspace, String indexId, byte[] lowerBound) {
        return subspace.pack(Tuple.from(INDEX_PREFIX, indexId).addAll(Tuple.fromBytes(lowerBound)));
    }
    
    /**
     * Creates a range end key for scanning index entries within a range (exclusive).
     * 
     * @param subspace The histogram subspace
     * @param indexId The index identifier
     * @param upperBound The upper bound of the range (exclusive)  
     * @return Range end key for index scanning
     */
    public static byte[] indexRangeEnd(DirectorySubspace subspace, String indexId, byte[] upperBound) {
        return subspace.pack(Tuple.from(INDEX_PREFIX, indexId).addAll(Tuple.fromBytes(upperBound)));
    }
    
    /**
     * Creates a range begin key for scanning counter shards of a leaf.
     * 
     * @param subspace The histogram subspace
     * @param indexId The index identifier
     * @param leafId The leaf identifier
     * @return Range begin key for counter shards
     */
    public static byte[] counterRangeBegin(DirectorySubspace subspace, String indexId, APPLeafId leafId) {
        byte[] keyPart = createLeafKeyPart(leafId);
        return subspace.pack(Tuple.from(HIST_PREFIX, indexId, COUNTER_PREFIX).addAll(Tuple.fromBytes(keyPart)));
    }
    
    /**
     * Creates a range end key for scanning counter shards of a leaf (exclusive).
     * 
     * @param subspace The histogram subspace
     * @param indexId The index identifier
     * @param leafId The leaf identifier
     * @return Range end key for counter shards
     */
    public static byte[] counterRangeEnd(DirectorySubspace subspace, String indexId, APPLeafId leafId) {
        byte[] keyPart = createLeafKeyPart(leafId);
        return subspace.pack(Tuple.from(HIST_PREFIX, indexId, COUNTER_PREFIX)
            .addAll(Tuple.fromBytes(keyPart))
            .add("\u0000"));
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
            return 0L;
        }
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }
    
    /**
     * Creates the combined key part <lowerPad><depthByte> for leaf identification.
     * This follows the APP specification format.
     */
    private static byte[] createLeafKeyPart(APPLeafId leafId) {
        byte[] lowerPad = leafId.lowerPad();
        byte[] keyPart = new byte[lowerPad.length + 1];
        
        // Copy lowerPad
        System.arraycopy(lowerPad, 0, keyPart, 0, lowerPad.length);
        
        // Append depth byte
        keyPart[lowerPad.length] = (byte) leafId.depth();
        
        return keyPart;
    }
    
    /**
     * Extracts the leaf ID from a leaf key part.
     * Inverse of createLeafKeyPart().
     */
    public static APPLeafId extractLeafId(byte[] keyPart, int maxDepth) {
        if (keyPart == null || keyPart.length != maxDepth + 1) {
            throw new IllegalArgumentException("Invalid key part length");
        }
        
        byte[] lowerPad = new byte[maxDepth];
        System.arraycopy(keyPart, 0, lowerPad, 0, maxDepth);
        
        int depth = keyPart[maxDepth] & 0xFF;
        
        return new APPLeafId(lowerPad, depth);
    }
}