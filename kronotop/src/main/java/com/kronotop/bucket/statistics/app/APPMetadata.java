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

import java.nio.ByteBuffer;

/**
 * Metadata stored with each leaf boundary record in FoundationDB.
 * 
 * Contains minimal information needed to reconstruct leaf properties:
 * - depth: The depth of this leaf in the APP tree
 * - checksum: Optional integrity check (can be 0 if not used)
 * 
 * This is stored as a small blob in the HIST/.../L/ keys.
 */
public record APPMetadata(int depth, long checksum) {
    
    public APPMetadata {
        if (depth < 1) {
            throw new IllegalArgumentException("depth must be >= 1, got: " + depth);
        }
    }
    
    /**
     * Creates metadata with just depth (no checksum).
     */
    public static APPMetadata withDepth(int depth) {
        return new APPMetadata(depth, 0L);
    }
    
    /**
     * Serializes the metadata to bytes for storage in FoundationDB.
     * Format: [4 bytes depth][8 bytes checksum]
     */
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.putInt(depth);
        buffer.putLong(checksum);
        return buffer.array();
    }
    
    /**
     * Deserializes metadata from bytes stored in FoundationDB.
     */
    public static APPMetadata fromBytes(byte[] data) {
        if (data == null || data.length != 12) {
            throw new IllegalArgumentException("Invalid metadata bytes: expected 12 bytes, got " + 
                (data == null ? "null" : data.length));
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int depth = buffer.getInt();
        long checksum = buffer.getLong();
        
        return new APPMetadata(depth, checksum);
    }
    
    /**
     * Updates the checksum value while keeping the same depth.
     */
    public APPMetadata withChecksum(long newChecksum) {
        return new APPMetadata(depth, newChecksum);
    }
}