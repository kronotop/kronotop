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

import com.apple.foundationdb.Transaction;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class PrefixHistogramTest extends BasePrefixHistogramTest {

    @Test
    void testInitializeAndOpen() {
        assertNotNull(histogram.getMetadata());
        assertEquals(8, histogram.getMetadata().maxDepth());
        assertNotNull(histogram.getEstimator());
    }

    @Test
    void testAddAndDelete() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Add some keys
            byte[] key1 = "hello".getBytes(StandardCharsets.UTF_8);
            byte[] key2 = "world".getBytes(StandardCharsets.UTF_8);
            byte[] key3 = "test".getBytes(StandardCharsets.UTF_8);

            histogram.add(tr, key1);
            histogram.add(tr, key2);
            histogram.add(tr, key3);

            // Verify bins have been incremented
            PrefixHistogramEstimator estimator = histogram.getEstimator();
            long totalCount = estimator.estimateRange(tr, null, null);
            assertEquals(3, totalCount);

            // Delete one key
            histogram.delete(tr, key1);
            totalCount = estimator.estimateRange(tr, null, null);
            assertEquals(2, totalCount);

            tr.commit().join();
        }
    }

    @Test
    void testUpdate() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] oldKey = "old".getBytes(StandardCharsets.UTF_8);
            byte[] newKey = "new".getBytes(StandardCharsets.UTF_8);

            // Add original key
            histogram.add(tr, oldKey);
            long totalCount = histogram.getEstimator().estimateRange(tr, null, null);
            assertEquals(1, totalCount);

            // Update key
            histogram.update(tr, oldKey, newKey);
            totalCount = histogram.getEstimator().estimateRange(tr, null, null);
            assertEquals(1, totalCount);

            // Update with same key (no-op)
            histogram.update(tr, newKey, newKey);
            totalCount = histogram.getEstimator().estimateRange(tr, null, null);
            assertEquals(1, totalCount);

            tr.commit().join();
        }
    }

    @Test
    void testEmptyKeys() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Adding null or empty keys should be handled gracefully
            histogram.add(tr, null);
            histogram.add(tr, new byte[0]);
            histogram.delete(tr, null);
            histogram.delete(tr, new byte[0]);

            long totalCount = histogram.getEstimator().estimateRange(tr, null, null);
            assertEquals(0, totalCount);

            tr.commit().join();
        }
    }

    @Test
    void testMaxDepthHandling() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Create a key longer than max depth (8 bytes)
            byte[] longKey = "verylongkeyexceedingmaxdepth".getBytes(StandardCharsets.UTF_8);
            assertTrue(longKey.length > 8);

            histogram.add(tr, longKey);
            long totalCount = histogram.getEstimator().estimateRange(tr, null, null);
            assertEquals(1, totalCount);

            tr.commit().join();
        }
    }

    @Test
    void testCustomMetadata() {
        PrefixHistogramMetadata customMetadata = new PrefixHistogramMetadata(4, 1);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrefixHistogram.initialize(tr, Arrays.asList("test", "bucket", "field2"), customMetadata);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrefixHistogram customHistogram = new PrefixHistogram(tr, Arrays.asList("test", "bucket", "field2"));
            assertEquals(4, customHistogram.getMetadata().maxDepth());
            assertEquals(1, customHistogram.getMetadata().version());
            tr.commit().join();
        }
    }
}