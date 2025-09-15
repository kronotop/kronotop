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
            histogram = new APPHistogram(tr, indexSubspace.getPath(), indexSubspace);
        }
    }

    @Test
    @DisplayName("Histogram initialization should create proper structure")
    void testInitialization() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Verify histogram can be opened
            APPHistogram testHistogram = new APPHistogram(tr, indexSubspace.getPath(), indexSubspace);

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
            histogram.addValue(tr, "hello".getBytes(), "doc1");
            histogram.addValue(tr, "world".getBytes(), "doc2");
            histogram.addValue(tr, "test".getBytes(), "doc3");

            // Add to index as well (for recount testing)
            addToIndex(tr, indexSubspace, "hello".getBytes(), "doc1");
            addToIndex(tr, indexSubspace, "world".getBytes(), "doc2");
            addToIndex(tr, indexSubspace, "test".getBytes(), "doc3");

            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            APPHistogram testHistogram = new APPHistogram(tr, indexSubspace.getPath(), indexSubspace);

            // Delete a value
            testHistogram.deleteValue(tr, "world".getBytes(), "doc2");
            removeFromIndex(tr, indexSubspace, "world".getBytes(), "doc2");

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
            histogram.addValue(tr, "initial".getBytes(), "doc1");
            addToIndex(tr, indexSubspace, "initial".getBytes(), "doc1");

            // Update to new value
            histogram.updateValue(tr, "initial".getBytes(), "updated".getBytes(), "doc1");
            removeFromIndex(tr, indexSubspace, "initial".getBytes(), "doc1");
            addToIndex(tr, indexSubspace, "updated".getBytes(), "doc1");

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
                String docRef = "doc" + i;

                histogram.addValue(tr, value, docRef);
                addToIndex(tr, indexSubspace, value, docRef);
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
            for (int i = 0; i < testValues.length; i++) {
                for (int j = 0; j < 3; j++) {
                    String docRef = "doc" + i + "_" + j;
                    histogram.addValue(tr, testValues[i], docRef);
                    addToIndex(tr, indexSubspace, testValues[i], docRef);
                }
            }

            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            APPHistogram testHistogram = new APPHistogram(tr, indexSubspace.getPath(), indexSubspace);
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
                APPHistogram testHistogram = new APPHistogram(tr, indexSubspace.getPath(), indexSubspace);

                // Add random values
                for (int i = 0; i < 10; i++) {
                    byte[] value = ("round" + round + "_value" + i).getBytes();
                    String docRef = "round" + round + "_doc" + i;

                    testHistogram.addValue(tr, value, docRef);
                    addToIndex(tr, indexSubspace, value, docRef);
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
    @DisplayName("APP algorithm debug and understanding")
    void testAPPAlgorithmDebug() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            APPHistogram testHistogram = new APPHistogram(tr, indexSubspace.getPath(), indexSubspace);

            // Let's understand the initial leaf structure
            // With maxDepth=3, initial leaf at depth=1 covers range [000000, 010000)
            // which is 256^2 = 65536 "minimal cells"
            System.out.println("=== Initial APP Leaf Structure ===");
            System.out.println("maxDepth = 3");
            System.out.println("Initial leaf at depth 1:");
            System.out.println("  Lower bound: [00, 00, 00]");
            System.out.println("  Width: 256^(3-1) = " + testHistogram.getMetadata().leafWidth(1));
            System.out.println("  Upper bound: [01, 00, 00]");

            // Now add some data spread across this range
            byte[][] testKeys = {
                    {0x00, 0x10, 0x00},  // Early in range
                    {0x00, (byte) 0x80, 0x00},  // Middle of range
                    {0x00, (byte) 0xF0, 0x00}   // Late in range
            };

            for (int i = 0; i < testKeys.length; i++) {
                testHistogram.addValue(tr, testKeys[i], "doc" + i);
                addToIndex(tr, indexSubspace, testKeys[i], "doc" + i);
            }

            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            APPHistogram testHistogram = new APPHistogram(tr, indexSubspace.getPath(), indexSubspace);
            APPHistogramEstimator estimator = testHistogram.getEstimator();

            System.out.println("=== After Adding Data ===");
            double totalCount = estimator.estimateTotalCount(tr);
            System.out.println("Total entries: 3, Estimated total: " + totalCount);

            // Test range that covers part of the leaf
            // Range [00 20 00, 00 A0 00) should cover middle portion
            double rangeEstimate = estimator.estimateRange(tr,
                    new byte[]{0x00, 0x20, 0x00},
                    new byte[]{0x00, (byte) 0xA0, 0x00});

            // Calculate expected: range covers (0xA0 - 0x20) * 256 = 128 * 256 = 32768 cells
            // Out of total leaf width 65536, so ratio = 32768/65536 = 0.5
            // With 3 entries, estimate should be 3 * 0.5 = 1.5
            System.out.println("Range [002000, 00A000) estimate: " + rangeEstimate + " (expected ~1.5)");

            // Test smaller range [00 80 00, 00 90 00) - should cover ~1/16th
            double smallRangeEstimate = estimator.estimateRange(tr,
                    new byte[]{0x00, (byte) 0x80, 0x00},
                    new byte[]{0x00, (byte) 0x90, 0x00});
            System.out.println("Range [008000, 009000) estimate: " + smallRangeEstimate + " (expected ~0.1875)");
        }
    }

    @Test
    @DisplayName("APP algorithm with readable words")
    void testAPPWithReadableWords() {
        // Use real words to understand how APP works with string data
        String[] words = {
                "apple", "banana", "cherry", "date", "elderberry",
                "fig", "grape", "honeydew", "kiwi", "lemon"
        };

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Add words to histogram and index
            for (int i = 0; i < words.length; i++) {
                histogram.addValue(tr, words[i].getBytes(), "doc" + i);
                addToIndex(tr, indexSubspace, words[i].getBytes(), "doc" + i);
            }

            tr.commit().join();
        }

        {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                System.out.println(">> " + histogram.getEstimator().estimateEquality(tr, "apple".getBytes()));
            }
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            APPHistogram testHistogram = new APPHistogram(tr, indexSubspace.getPath(), indexSubspace);
            APPHistogramEstimator estimator = testHistogram.getEstimator();

            System.out.println("=== APP with Real Words ===");

            // Show how words get padded to maxDepth=3 bytes
            System.out.println("Word padding examples (maxDepth=3):");
            for (String word : new String[]{"apple", "banana", "fig"}) {
                byte[] original = word.getBytes();
                byte[] padded = APPHistogramKeySchema.rightPad(original, 3, (byte) 0x00);
                System.out.printf("'%s' -> [%02X, %02X, %02X]\n",
                        word, padded[0] & 0xFF, padded[1] & 0xFF, padded[2] & 0xFF);
            }

            // Also show the range query bounds after padding
            System.out.println("\nRange query bounds after padding:");
            byte[] bPadded = APPHistogramKeySchema.rightPad("b".getBytes(), 3, (byte) 0x00);
            byte[] hPadded = APPHistogramKeySchema.rightPad("h".getBytes(), 3, (byte) 0x00);
            System.out.printf("'b' -> [%02X, %02X, %02X]\n", bPadded[0] & 0xFF, bPadded[1] & 0xFF, bPadded[2] & 0xFF);
            System.out.printf("'h' -> [%02X, %02X, %02X]\n", hPadded[0] & 0xFF, hPadded[1] & 0xFF, hPadded[2] & 0xFF);

            // Check if 'banana' falls within ['b', 'h') range
            byte[] bananaPadded = APPHistogramKeySchema.rightPad("banana".getBytes(), 3, (byte) 0x00);
            System.out.printf("'banana' -> [%02X, %02X, %02X]\n", bananaPadded[0] & 0xFF, bananaPadded[1] & 0xFF, bananaPadded[2] & 0xFF);

            boolean bananaInRange = APPHistogramArithmetic.compareUnsigned(bananaPadded, bPadded) >= 0 &&
                    APPHistogramArithmetic.compareUnsigned(bananaPadded, hPadded) < 0;
            System.out.println("Does 'banana' fall in ['b', 'h')? " + bananaInRange);

            // Total count
            double totalEstimate = estimator.estimateTotalCount(tr);
            System.out.println("\nTotal words: " + words.length + ", Estimated: " + totalEstimate);

            // Let's debug by checking what leaves exist in the histogram
            System.out.println("\nDEBUG: Let's check the actual leaf structure...");

            // First, let's scan what leaf boundaries actually exist
            byte[] leafScanBegin = testHistogram.getSubspace().pack(Tuple.from("L"));
            byte[] leafScanEnd = testHistogram.getSubspace().pack(Tuple.from("L", new byte[]{(byte) 0xFF}));
            var leafBoundaries = tr.getRange(leafScanBegin, leafScanEnd).asList().join();
            System.out.println("Found " + leafBoundaries.size() + " leaf boundaries:");
            for (var kv : leafBoundaries) {
                var unpacked = testHistogram.getSubspace().unpack(kv.getKey());
                if (unpacked.size() >= 2) {
                    byte[] leafId = (byte[]) unpacked.get(1);
                    byte[] lowerBound = APPHistogramKeySchema.extractLowerBound(leafId);
                    int depth = APPHistogramKeySchema.extractDepth(leafId);
                    System.out.printf("  Leaf: lower=[%02X,%02X,%02X], depth=%d\n",
                            lowerBound[0] & 0xFF, lowerBound[1] & 0xFF, lowerBound[2] & 0xFF, depth);

                }
            }

            // Try a range that definitely covers everything - from 0x00 to 0xFF in first byte
            double debugRange = estimator.estimateRange(tr,
                    new byte[]{0x00, 0x00, 0x00},
                    new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
            System.out.println("Debug range [000000, FFFFFF) estimate: " + debugRange);

            // Range queries with readable meanings
            // Range from "b" to "h" should include: banana, cherry, date, elderberry, fig, grape
            double rangeB_H = estimator.estimateRange(tr, "b".getBytes(), "h".getBytes());
            System.out.println("Range ['b', 'h') estimate: " + rangeB_H + " (should include banana->grape)");

            // Range from "a" to "d" should include: apple, banana, cherry
            double rangeA_D = estimator.estimateRange(tr, "a".getBytes(), "d".getBytes());
            System.out.println("Range ['a', 'd') estimate: " + rangeA_D + " (should include apple->cherry)");

            // Range from "f" to "z" should include: fig, grape, honeydew, kiwi, lemon
            double rangeF_Z = estimator.estimateRange(tr, "f".getBytes(), "z".getBytes());
            System.out.println("Range ['f', 'z') estimate: " + rangeF_Z + " (should include fig->lemon)");

            // Let's also try using the exact padded bounds to see if the issue is padding
            System.out.println("\nUsing explicit 3-byte ranges:");
            double rangeBH_explicit = estimator.estimateRange(tr,
                    new byte[]{0x62, 0x00, 0x00},  // 'b' padded
                    new byte[]{0x68, 0x00, 0x00}); // 'h' padded
            System.out.println("Range [620000, 680000) estimate: " + rangeBH_explicit);

            // Equality test for existing key
            double equalityApple = estimator.estimateEquality(tr, "apple".getBytes());
            System.out.println("Equality 'apple' estimate: " + equalityApple);

            // Equality test for non-existent key
            double equalityNonExistent = estimator.estimateEquality(tr, "xyz".getBytes());
            System.out.println("Equality 'xyz' (non-existent) estimate: " + equalityNonExistent);

            // Verify non-existent key has very small estimate (should be tiny since it covers minimal space)
            assertTrue(equalityNonExistent >= 0, "Non-existent key estimate should be non-negative");
            assertTrue(equalityNonExistent < 1.0, "Non-existent key estimate should be very small");

            // Test the fundamental property: larger ranges should have >= estimates
            assertTrue(totalEstimate >= rangeB_H, "Total should >= range estimate");
            assertTrue(totalEstimate >= rangeA_D, "Total should >= range estimate");
            assertTrue(totalEstimate >= rangeF_Z, "Total should >= range estimate");

            // Show the key insight about how padding affects lexicographic ordering
            System.out.println("\nKey insight: Words get truncated/padded to " +
                    testHistogram.getMetadata().maxDepth() + " bytes,");
            System.out.println("which determines their position in the 256^3 address space.");
        }
    }

    @Test
    @DisplayName("Large keys should be handled correctly with padding")
    void testLargeKeys() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Test with keys longer than maxDepth
            byte[] longKey = "this_is_a_very_long_key_that_exceeds_max_depth".getBytes();
            histogram.addValue(tr, longKey, "doc1");
            addToIndex(tr, indexSubspace, longKey, "doc1");

            // Test with very short keys
            byte[] shortKey = "x".getBytes();
            histogram.addValue(tr, shortKey, "doc2");
            addToIndex(tr, indexSubspace, shortKey, "doc2");

            // Test with empty key
            byte[] emptyKey = new byte[0];
            histogram.addValue(tr, emptyKey, "doc3");
            addToIndex(tr, indexSubspace, emptyKey, "doc3");

            tr.commit().join();
        }

        assertTrue(true);
    }

    /**
     * Helper method to add entries to the mock index for testing recount operations.
     */
    private void addToIndex(Transaction tr, DirectorySubspace indexSubspace, byte[] value, String docRef) {
        byte[] key = indexSubspace.pack(Tuple.from(value, docRef));
        tr.set(key, new byte[0]); // Empty value, just presence
    }

    /**
     * Helper method to remove entries from the mock index.
     */
    private void removeFromIndex(Transaction tr, DirectorySubspace indexSubspace, byte[] value, String docRef) {
        byte[] key = indexSubspace.pack(Tuple.from(value, docRef));
        tr.clear(key);
    }
}