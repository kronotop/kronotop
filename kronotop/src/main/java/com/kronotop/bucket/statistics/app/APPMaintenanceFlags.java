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

/**
 * Maintenance flags stored for each leaf to coordinate split/merge operations.
 * 
 * Flags are stored as bitfields in FoundationDB under HIST/.../F/ keys.
 * This allows deferring heavy maintenance work without requiring background workers.
 * 
 * Operations set flags opportunistically during writes, and maintenance is performed
 * by subsequent write operations that encounter the flags.
 */
public enum APPMaintenanceFlags {
    
    /**
     * Indicates this leaf should be split when convenient.
     * Set when leaf count reaches or exceeds T_SPLIT threshold.
     */
    NEEDS_SPLIT(1),
    
    /**
     * Indicates this leaf and its siblings should be merged when convenient.
     * Set when leaf count drops to or below T_MERGE threshold.
     */
    NEEDS_MERGE(2),
    
    /**
     * Indicates this leaf uses sharded counters to reduce write contention.
     * When set, counter writes are distributed across multiple shard keys.
     */
    HOT_SHARDED(4);
    
    private final int bitValue;
    
    APPMaintenanceFlags(int bitValue) {
        this.bitValue = bitValue;
    }
    
    public int getBitValue() {
        return bitValue;
    }
    
    /**
     * Creates a bitfield from a set of flags.
     */
    public static int toBitfield(APPMaintenanceFlags... flags) {
        int result = 0;
        for (APPMaintenanceFlags flag : flags) {
            result |= flag.bitValue;
        }
        return result;
    }
    
    /**
     * Checks if a bitfield contains a specific flag.
     */
    public static boolean hasFlag(int bitfield, APPMaintenanceFlags flag) {
        return (bitfield & flag.bitValue) != 0;
    }
    
    /**
     * Adds a flag to a bitfield.
     */
    public static int addFlag(int bitfield, APPMaintenanceFlags flag) {
        return bitfield | flag.bitValue;
    }
    
    /**
     * Removes a flag from a bitfield.
     */
    public static int removeFlag(int bitfield, APPMaintenanceFlags flag) {
        return bitfield & ~flag.bitValue;
    }
    
    /**
     * Converts a bitfield to a byte for storage.
     */
    public static byte[] toBytes(int bitfield) {
        return new byte[]{(byte) bitfield};
    }
    
    /**
     * Converts stored bytes back to a bitfield.
     */
    public static int fromBytes(byte[] data) {
        if (data == null || data.length != 1) {
            return 0; // No flags set
        }
        return data[0] & 0xFF;
    }
}