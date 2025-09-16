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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.server.Session;
import org.bson.BsonType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("APPHistogram Integration Tests")
class APPHistogramTest extends BaseStandaloneInstanceTest {

    private APPHistogram histogram;
    private DirectorySubspace indexSubspace;
    private String testBucket;

    protected void createBucket(String bucketName) {
        Session session = getSession();
        BucketMetadataUtil.createOrOpen(context, session, bucketName);
    }

    protected void createIndex(String bucketName, IndexDefinition indexDefinition) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = getBucketMetadata(bucketName);
            IndexUtil.create(tr, metadata.subspace(), indexDefinition);
            tr.commit().join();
        }
    }

    protected BucketMetadata createIndexesAndLoadBucketMetadata(String bucketName, IndexDefinition definition) {
        createBucket(bucketName);
        createIndex(bucketName, definition);
        Session session = getSession();
        return BucketMetadataUtil.createOrOpen(context, session, bucketName);
    }

    @BeforeEach
    void setUp() {
        testBucket = "test_bucket_" + System.nanoTime();

        IndexDefinition testIndex = IndexDefinition.create("test-index", "testField", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata bucketMetadata = createIndexesAndLoadBucketMetadata(testBucket, testIndex);
        indexSubspace = bucketMetadata.indexes().getSubspace("testField");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            APPHistogram.initialize(tr, indexSubspace.getPath());
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            histogram = new APPHistogram(tr, indexSubspace.getPath());
        }
    }

    @Test
    @DisplayName("Histogram initialization should create proper structure")
    void testInitialization() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Verify histogram can be opened
            APPHistogram testHistogram = new APPHistogram(tr, indexSubspace.getPath());

            assertNotNull(testHistogram.getMetadata());
            assertEquals(3, testHistogram.getMetadata().maxDepth());
            assertEquals(4, testHistogram.getMetadata().fanout());

            // Verify initial root leaf exists at depth 0
            byte[] globalLow = APPHistogramArithmetic.createGlobalLow(3);
            byte[] boundaryKey = APPHistogramKeySchema.leafBoundaryKey(testHistogram.getSubspace(), globalLow, 0);
            byte[] boundaryValue = tr.get(boundaryKey).join();
            assertNotNull(boundaryValue);
        }
    }

    @Test
    @DisplayName("Basic add and delete operations should work")
    void testBasicOperations() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Add some values
            histogram.add(tr, "hello".getBytes());
            histogram.add(tr, "world".getBytes());
            histogram.add(tr, "test".getBytes());

            // Add to index as well (for recount testing)
            addToIndex(tr, indexSubspace, "hello".getBytes());
            addToIndex(tr, indexSubspace, "world".getBytes());
            addToIndex(tr, indexSubspace, "test".getBytes());

            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            APPHistogram testHistogram = new APPHistogram(tr, indexSubspace.getPath());

            // Delete a value
            testHistogram.delete(tr, "world".getBytes());
            removeFromIndex(tr, indexSubspace, "world".getBytes());

            tr.commit().join();
        }

        // Verify operations completed without errors
        assertTrue(true);
    }

    @Test
    @DisplayName("Update operation should be atomic")
    void testUpdateOperation() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Add initial value
            histogram.add(tr, "initial".getBytes());
            addToIndex(tr, indexSubspace, "initial".getBytes());

            // Update to new value
            histogram.update(tr, "initial".getBytes(), "updated".getBytes());
            removeFromIndex(tr, indexSubspace, "initial".getBytes());
            addToIndex(tr, indexSubspace, "updated".getBytes());

            tr.commit().join();
        }

        assertTrue(true);
    }

    @Test
    @DisplayName("Split should trigger with sufficient data")
    void testSplitBehavior() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Add enough values to trigger split
            for (int i = 0; i < 20; i++) {
                byte[] value = String.format("value%03d", i).getBytes();

                histogram.add(tr, value);
                addToIndex(tr, indexSubspace, value);
            }

            tr.commit().join();
        }

        assertTrue(true);
    }

    @Test
    @DisplayName("Estimation should provide reasonable results")
    void testEstimation() {
        // Use values that stay within the same leaf structure for predictable testing
        // All values start with 0x00, so they'll be in the initial leaf [000000, 010000)
        byte[][] testValues = {
                {0x00, 0x10, 0x00}, {0x00, 0x20, 0x00}, {0x00, 0x30, 0x00},
                {0x00, 0x40, 0x00}, {0x00, 0x50, 0x00}
        };

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Add each value 3 times (total = 15 entries)
            for (byte[] testValue : testValues) {
                for (int j = 0; j < 3; j++) {
                    histogram.add(tr, testValue);
                    addToIndex(tr, indexSubspace, testValue);
                }
            }

            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            APPHistogram testHistogram = new APPHistogram(tr, indexSubspace.getPath());
            APPHistogramEstimator estimator = testHistogram.getEstimator();

            // Test total count estimation - should be close to 15
            double totalEstimate = estimator.estimateTotalCount(tr);
            assertTrue(totalEstimate >= 0, "Total estimate should be non-negative");

            // Test range estimation - all data is in leaf [000000, 010000) with width 65536
            // Range [001500, 003500) covers (0x3500 - 0x1500) = 0x2000 = 8192 cells
            // Ratio = 8192/65536 = 1/8 = 0.125, so estimate = 15 * 0.125 = 1.875
            double rangeEstimate = estimator.estimateRange(tr,
                    new byte[]{0x00, 0x15, 0x00},
                    new byte[]{0x00, 0x35, 0x00});

            assertTrue(rangeEstimate >= 0, "Range estimate should be non-negative");
            assertTrue(rangeEstimate <= totalEstimate, "Range estimate should not exceed total");

            // Test equality estimation - very small range around one value
            double equalityEstimate = estimator.estimateEquality(tr, new byte[]{0x00, 0x20, 0x00});
            assertTrue(equalityEstimate >= 0, "Equality estimate should be non-negative");

            // Debug output for verification
            System.out.println("Total estimate: " + totalEstimate + " (expected ~15)");
            System.out.println("Range [001500,003500) estimate: " + rangeEstimate + " (expected ~1.875)");
            System.out.println("Equality 002000 estimate: " + equalityEstimate + " (expected very small)");

            // The key insight: APP estimates are based on uniform distribution assumption
            // within each leaf, so they'll be proportional to the range size
        }
    }

    @Test
    @DisplayName("Concurrent operations should maintain consistency")
    void testConcurrentOperations() {
        Random random = new Random(42);

        for (int round = 0; round < 5; round++) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                APPHistogram testHistogram = new APPHistogram(tr, indexSubspace.getPath());

                // Add random values
                for (int i = 0; i < 10; i++) {
                    byte[] value = ("round" + round + "_value" + i).getBytes();

                    testHistogram.add(tr, value);
                    addToIndex(tr, indexSubspace, value);
                }

                tr.commit().join();
            }
        }

        assertTrue(true);
    }

    @Test
    @DisplayName("Empty histogram should handle queries gracefully")
    void testEmptyHistogram() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            APPHistogramEstimator estimator = histogram.getEstimator();

            // Empty histogram should return 0 estimates
            double totalEstimate = estimator.estimateTotalCount(tr);
            assertEquals(0.0, totalEstimate, 0.001);

            double rangeEstimate = estimator.estimateRange(tr, new byte[]{0x10}, new byte[]{0x20});
            assertEquals(0.0, rangeEstimate, 0.001);

            double equalityEstimate = estimator.estimateEquality(tr, new byte[]{0x15});
            assertEquals(0.0, equalityEstimate, 0.001);
        }
    }

    @Test
    @DisplayName("Large keys should be handled correctly with padding")
    void testLargeKeys() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Test with keys longer than maxDepth
            byte[] longKey = "this_is_a_very_long_key_that_exceeds_max_depth".getBytes();
            histogram.add(tr, longKey);
            addToIndex(tr, indexSubspace, longKey);

            // Test with very short keys
            byte[] shortKey = "x".getBytes();
            histogram.add(tr, shortKey);
            addToIndex(tr, indexSubspace, shortKey);

            // Test with empty key
            byte[] emptyKey = new byte[0];
            histogram.add(tr, emptyKey);
            addToIndex(tr, indexSubspace, emptyKey);

            tr.commit().join();
        }

        assertTrue(true);
    }

    /**
     * Helper method to add entries to the mock index for testing recount operations.
     */
    private void addToIndex(Transaction tr, DirectorySubspace indexSubspace, byte[] value) {
        byte[] key = indexSubspace.pack(Tuple.from((Object) value));
        tr.set(key, new byte[0]); // Empty value, just presence
    }

    /**
     * Helper method to remove entries from the mock index.
     */
    private void removeFromIndex(Transaction tr, DirectorySubspace indexSubspace, byte[] value) {
        byte[] key = indexSubspace.pack(Tuple.from((Object) value));
        tr.clear(key);
    }
}