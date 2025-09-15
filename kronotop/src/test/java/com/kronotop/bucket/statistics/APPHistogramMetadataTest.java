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

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("APPHistogramMetadata Tests")
class APPHistogramMetadataTest {

    @Test
    @DisplayName("Default metadata should have valid configuration")
    void testDefaultMetadata() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();

        assertEquals(3, metadata.maxDepth());
        assertEquals(4, metadata.fanout());
        assertEquals(4096, metadata.splitThreshold());
        assertEquals(1024, metadata.mergeThreshold());
        assertEquals(8, metadata.maxShardCount());
        assertEquals(1, metadata.version());

        // Verify hysteresis ratio
        double ratio = metadata.hysteresisRatio();
        assertEquals(4.0, ratio, 0.001);
        assertTrue(ratio >= 2.0);
    }

    @Test
    @DisplayName("Leaf width calculation should follow APP specification")
    void testLeafWidthCalculation() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();

        // For maxDepth = 3: S(d) = 256^(3 - d)
        assertEquals(256L * 256L, metadata.leafWidth(1)); // 256^2 = 65536
        assertEquals(256L, metadata.leafWidth(2));        // 256^1 = 256
        assertEquals(1L, metadata.leafWidth(3));          // 256^0 = 1
    }

    @Test
    @DisplayName("Parent width calculation should be fanout * leaf width")
    void testParentWidthCalculation() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();

        // Parent width = fanout * S(d) = 4 * S(d)
        assertEquals(4L * 256L * 256L, metadata.parentWidth(1)); // 4 * 65536
        assertEquals(4L * 256L, metadata.parentWidth(2));        // 4 * 256
        assertEquals(4L, metadata.parentWidth(3));               // 4 * 1
    }

    @Test
    @DisplayName("Max depth validation should work correctly")
    void testMaxDepthValidation() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();

        assertFalse(metadata.isMaxDepth(1));
        assertFalse(metadata.isMaxDepth(2));
        assertTrue(metadata.isMaxDepth(3));
        assertTrue(metadata.isMaxDepth(4)); // >= maxDepth
    }

    @Test
    @DisplayName("Constructor should validate parameters")
    void testParameterValidation() {
        // Valid parameters should work
        assertDoesNotThrow(() -> new APPHistogramMetadata(3, 4, 4096, 1024, 8, 1));

        // Invalid maxDepth
        assertThrows(IllegalArgumentException.class,
                () -> new APPHistogramMetadata(0, 4, 4096, 1024, 8, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new APPHistogramMetadata(7, 4, 4096, 1024, 8, 1));

        // Invalid fanout (must be 4 for APP)
        assertThrows(IllegalArgumentException.class,
                () -> new APPHistogramMetadata(3, 3, 4096, 1024, 8, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new APPHistogramMetadata(3, 8, 4096, 1024, 8, 1));

        // Invalid thresholds (split must be > merge)
        assertThrows(IllegalArgumentException.class,
                () -> new APPHistogramMetadata(3, 4, 1024, 1024, 8, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new APPHistogramMetadata(3, 4, 1000, 1024, 8, 1));

        // Invalid merge threshold
        assertThrows(IllegalArgumentException.class,
                () -> new APPHistogramMetadata(3, 4, 4096, 0, 8, 1));

        // Invalid shard count
        assertThrows(IllegalArgumentException.class,
                () -> new APPHistogramMetadata(3, 4, 4096, 1024, 0, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new APPHistogramMetadata(3, 4, 4096, 1024, 257, 1));

        // Invalid version
        assertThrows(IllegalArgumentException.class,
                () -> new APPHistogramMetadata(3, 4, 4096, 1024, 8, 0));

        // Low hysteresis ratio should be rejected
        assertThrows(IllegalArgumentException.class,
                () -> new APPHistogramMetadata(3, 4, 1500, 1000, 8, 1)); // ratio = 1.5 < 2.0
    }

    @Test
    @DisplayName("Hysteresis ratio calculation should be correct")
    void testHysteresisRatio() {
        APPHistogramMetadata metadata1 = new APPHistogramMetadata(3, 4, 4000, 1000, 8, 1);
        assertEquals(4.0, metadata1.hysteresisRatio(), 0.001);

        APPHistogramMetadata metadata2 = new APPHistogramMetadata(3, 4, 3000, 1000, 8, 1);
        assertEquals(3.0, metadata2.hysteresisRatio(), 0.001);

        APPHistogramMetadata metadata3 = new APPHistogramMetadata(3, 4, 2048, 1024, 8, 1);
        assertEquals(2.0, metadata3.hysteresisRatio(), 0.001);
    }

    @Test
    @DisplayName("Custom configurations should work with different depth limits")
    void testCustomConfigurations() {
        // Depth 4 configuration
        APPHistogramMetadata depth4 = new APPHistogramMetadata(4, 4, 8192, 2048, 16, 1);
        assertEquals(4, depth4.maxDepth());
        assertEquals(256L * 256L * 256L, depth4.leafWidth(1)); // 256^3
        assertEquals(256L * 256L, depth4.leafWidth(2));        // 256^2
        assertEquals(256L, depth4.leafWidth(3));               // 256^1
        assertEquals(1L, depth4.leafWidth(4));                 // 256^0

        // Depth 2 configuration (minimal)
        APPHistogramMetadata depth2 = new APPHistogramMetadata(2, 4, 2048, 512, 4, 1);
        assertEquals(2, depth2.maxDepth());
        assertEquals(256L, depth2.leafWidth(1));               // 256^1
        assertEquals(1L, depth2.leafWidth(2));                 // 256^0
        assertTrue(depth2.isMaxDepth(2));
        assertFalse(depth2.isMaxDepth(1));
    }
}