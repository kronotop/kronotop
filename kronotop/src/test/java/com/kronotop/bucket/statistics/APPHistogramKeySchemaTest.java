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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("APPHistogramKeySchema Tests")
class APPHistogramKeySchemaTest {

    @Test
    @DisplayName("Counter value encoding/decoding should be correct")
    void testCounterValueEncoding() {
        // Test positive values
        long value = 12345L;
        byte[] encoded = APPHistogramKeySchema.encodeCounterValue(value);
        assertEquals(8, encoded.length);
        assertEquals(value, APPHistogramKeySchema.decodeCounterValue(encoded));

        // Test negative values
        value = -6789L;
        encoded = APPHistogramKeySchema.encodeCounterValue(value);
        assertEquals(value, APPHistogramKeySchema.decodeCounterValue(encoded));

        // Test zero
        value = 0L;
        encoded = APPHistogramKeySchema.encodeCounterValue(value);
        assertEquals(value, APPHistogramKeySchema.decodeCounterValue(encoded));

        // Test max/min values
        value = Long.MAX_VALUE;
        encoded = APPHistogramKeySchema.encodeCounterValue(value);
        assertEquals(value, APPHistogramKeySchema.decodeCounterValue(encoded));

        value = Long.MIN_VALUE;
        encoded = APPHistogramKeySchema.encodeCounterValue(value);
        assertEquals(value, APPHistogramKeySchema.decodeCounterValue(encoded));

        // Test predefined constants
        assertEquals(1L, APPHistogramKeySchema.decodeCounterValue(APPHistogramKeySchema.ONE_LE));
        assertEquals(-1L, APPHistogramKeySchema.decodeCounterValue(APPHistogramKeySchema.NEGATIVE_ONE_LE));
    }

    @Test
    @DisplayName("Counter value decoding should handle null and invalid input")
    void testCounterValueDecodingEdgeCases() {
        // Null input
        assertEquals(0L, APPHistogramKeySchema.decodeCounterValue(null));

        // Empty array
        assertEquals(0L, APPHistogramKeySchema.decodeCounterValue(new byte[0]));

        // Wrong length arrays
        assertEquals(0L, APPHistogramKeySchema.decodeCounterValue(new byte[4]));
        assertEquals(0L, APPHistogramKeySchema.decodeCounterValue(new byte[12]));
    }

    @Test
    @DisplayName("Metadata encoding/decoding should preserve all fields")
    void testMetadataEncoding() {
        APPHistogramMetadata original = APPHistogramMetadata.defaultMetadata();

        byte[] encoded = APPHistogramKeySchema.encodeMetadata(original);
        assertNotNull(encoded);
        assertTrue(encoded.length > 0);

        APPHistogramMetadata decoded = APPHistogramKeySchema.decodeMetadata(encoded);
        assertNotNull(decoded);

        assertEquals(original.maxDepth(), decoded.maxDepth());
        assertEquals(original.fanout(), decoded.fanout());
        assertEquals(original.splitThreshold(), decoded.splitThreshold());
        assertEquals(original.mergeThreshold(), decoded.mergeThreshold());
        assertEquals(original.maxShardCount(), decoded.maxShardCount());
        assertEquals(original.version(), decoded.version());
    }

    @Test
    @DisplayName("Metadata decoding should handle null input")
    void testMetadataDecodingEdgeCases() {
        assertNull(APPHistogramKeySchema.decodeMetadata(null));
    }

    @Test
    @DisplayName("Right padding should work correctly")
    void testRightPadding() {
        // Pad with zeros
        byte[] input = {0x12, 0x34};
        byte[] result = APPHistogramKeySchema.rightPad(input, 4, (byte) 0x00);
        assertArrayEquals(new byte[]{0x12, 0x34, 0x00, 0x00}, result);

        // Pad with 0xFF
        input = new byte[]{0x12};
        result = APPHistogramKeySchema.rightPad(input, 3, (byte) 0xFF);
        assertArrayEquals(new byte[]{0x12, (byte) 0xFF, (byte) 0xFF}, result);

        // No padding needed (exact length)
        input = new byte[]{0x12, 0x34, 0x56};
        result = APPHistogramKeySchema.rightPad(input, 3, (byte) 0x00);
        assertArrayEquals(new byte[]{0x12, 0x34, 0x56}, result);

        // Truncation (input longer than target)
        input = new byte[]{0x12, 0x34, 0x56, 0x78};
        result = APPHistogramKeySchema.rightPad(input, 2, (byte) 0x00);
        assertArrayEquals(new byte[]{0x12, 0x34}, result);

        // Null input
        result = APPHistogramKeySchema.rightPad(null, 3, (byte) 0x00);
        assertArrayEquals(new byte[]{0x00, 0x00, 0x00}, result);

        // Empty input
        input = new byte[0];
        result = APPHistogramKeySchema.rightPad(input, 2, (byte) 0xFF);
        assertArrayEquals(new byte[]{(byte) 0xFF, (byte) 0xFF}, result);
    }

    @Test
    @DisplayName("Leaf ID creation and extraction should be consistent")
    void testLeafIdOperations() {
        byte[] lowerPad = {0x12, 0x34, 0x56};
        int depth = 2;

        // Create leaf ID
        byte[] leafId = APPHistogramKeySchema.createLeafId(lowerPad, depth);
        assertEquals(4, leafId.length); // 3 bytes + 1 depth byte
        assertArrayEquals(new byte[]{0x12, 0x34, 0x56, 0x02}, leafId);

        // Extract components
        byte[] extractedLower = APPHistogramKeySchema.extractLowerBound(leafId);
        assertArrayEquals(lowerPad, extractedLower);

        int extractedDepth = APPHistogramKeySchema.extractDepth(leafId);
        assertEquals(depth, extractedDepth);

        // Test with different depth
        depth = 255; // Max byte value
        leafId = APPHistogramKeySchema.createLeafId(lowerPad, depth);
        assertEquals(255, APPHistogramKeySchema.extractDepth(leafId));
    }

    @Test
    @DisplayName("Leaf ID extraction should handle edge cases")
    void testLeafIdExtractionEdgeCases() {
        // Null input
        byte[] result = APPHistogramKeySchema.extractLowerBound(null);
        assertArrayEquals(new byte[0], result);

        int depth = APPHistogramKeySchema.extractDepth(null);
        assertEquals(1, depth); // Default depth

        // Empty input
        result = APPHistogramKeySchema.extractLowerBound(new byte[0]);
        assertArrayEquals(new byte[0], result);

        depth = APPHistogramKeySchema.extractDepth(new byte[0]);
        assertEquals(1, depth); // Default depth

        // Single byte (only depth)
        depth = APPHistogramKeySchema.extractDepth(new byte[]{0x03});
        assertEquals(3, depth);

        result = APPHistogramKeySchema.extractLowerBound(new byte[]{0x03});
        assertArrayEquals(new byte[0], result);
    }

    @Test
    @DisplayName("Flags encoding/decoding should preserve bit patterns")
    void testFlagsEncoding() {
        // Test individual flags
        int needsSplit = APPHistogramKeySchema.NEEDS_SPLIT_FLAG;
        byte[] encoded = APPHistogramKeySchema.encodeFlags(needsSplit);
        assertEquals(needsSplit, APPHistogramKeySchema.decodeFlags(encoded));

        int needsMerge = APPHistogramKeySchema.NEEDS_MERGE_FLAG;
        encoded = APPHistogramKeySchema.encodeFlags(needsMerge);
        assertEquals(needsMerge, APPHistogramKeySchema.decodeFlags(encoded));

        int hotSharded = APPHistogramKeySchema.HOT_SHARDED_FLAG;
        encoded = APPHistogramKeySchema.encodeFlags(hotSharded);
        assertEquals(hotSharded, APPHistogramKeySchema.decodeFlags(encoded));

        // Test combined flags
        int combined = needsSplit | needsMerge | hotSharded;
        encoded = APPHistogramKeySchema.encodeFlags(combined);
        assertEquals(combined, APPHistogramKeySchema.decodeFlags(encoded));

        // Test zero flags
        encoded = APPHistogramKeySchema.encodeFlags(0);
        assertEquals(0, APPHistogramKeySchema.decodeFlags(encoded));

        // Test all bits set (255)
        encoded = APPHistogramKeySchema.encodeFlags(255);
        assertEquals(255, APPHistogramKeySchema.decodeFlags(encoded));
    }

    @Test
    @DisplayName("Flags decoding should handle edge cases")
    void testFlagsDecodingEdgeCases() {
        // Null input
        assertEquals(0, APPHistogramKeySchema.decodeFlags(null));

        // Empty input
        assertEquals(0, APPHistogramKeySchema.decodeFlags(new byte[0]));
    }

    @Test
    @DisplayName("Leaf metadata encoding/decoding should preserve depth")
    void testLeafMetadataEncoding() {
        // Test various depth values
        for (int depth = 1; depth <= 255; depth += 13) { // Test representative values
            byte[] encoded = APPHistogramKeySchema.encodeLeafMetadata(depth);
            assertEquals(1, encoded.length);
            assertEquals(depth, APPHistogramKeySchema.decodeLeafMetadata(encoded));
        }

        // Test boundary values
        byte[] encoded = APPHistogramKeySchema.encodeLeafMetadata(1);
        assertEquals(1, APPHistogramKeySchema.decodeLeafMetadata(encoded));

        encoded = APPHistogramKeySchema.encodeLeafMetadata(255);
        assertEquals(255, APPHistogramKeySchema.decodeLeafMetadata(encoded));
    }

    @Test
    @DisplayName("Leaf metadata decoding should handle edge cases")
    void testLeafMetadataDecodingEdgeCases() {
        // Null input
        assertEquals(1, APPHistogramKeySchema.decodeLeafMetadata(null)); // Default depth

        // Empty input
        assertEquals(1, APPHistogramKeySchema.decodeLeafMetadata(new byte[0])); // Default depth
    }

    @Test
    @DisplayName("Flag constants should have correct bit patterns")
    void testFlagConstants() {
        // Verify flag values are powers of 2 (single bits)
        assertEquals(1, APPHistogramKeySchema.NEEDS_SPLIT_FLAG);
        assertEquals(2, APPHistogramKeySchema.NEEDS_MERGE_FLAG);
        assertEquals(4, APPHistogramKeySchema.HOT_SHARDED_FLAG);

        // Verify they don't overlap
        int allFlags = APPHistogramKeySchema.NEEDS_SPLIT_FLAG |
                      APPHistogramKeySchema.NEEDS_MERGE_FLAG |
                      APPHistogramKeySchema.HOT_SHARDED_FLAG;
        assertEquals(7, allFlags); // 1 + 2 + 4 = 7

        // Verify individual flag checking works
        assertTrue((allFlags & APPHistogramKeySchema.NEEDS_SPLIT_FLAG) != 0);
        assertTrue((allFlags & APPHistogramKeySchema.NEEDS_MERGE_FLAG) != 0);
        assertTrue((allFlags & APPHistogramKeySchema.HOT_SHARDED_FLAG) != 0);

        // Test that flags can be combined and separated
        int combined = APPHistogramKeySchema.NEEDS_SPLIT_FLAG | APPHistogramKeySchema.HOT_SHARDED_FLAG;
        assertTrue((combined & APPHistogramKeySchema.NEEDS_SPLIT_FLAG) != 0);
        assertFalse((combined & APPHistogramKeySchema.NEEDS_MERGE_FLAG) != 0);
        assertTrue((combined & APPHistogramKeySchema.HOT_SHARDED_FLAG) != 0);
    }
}