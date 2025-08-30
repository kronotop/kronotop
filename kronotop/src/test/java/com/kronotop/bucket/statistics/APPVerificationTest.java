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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive verification tests for APP histogram implementation
 * following the verification plan from docs/algorithms/adaptive-prefix-partitioning.md
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class APPVerificationTest extends BaseStandaloneInstanceTest {

    private FDBAdaptivePrefixHistogram histogram;
    private final String bucketName = "verify_bucket";
    private final String fieldName = "verify_field";
    
    // Ground truth for validation
    private TreeMap<byte[], List<String>> groundTruth;

    @BeforeEach
    public void setUp() {
        histogram = new FDBAdaptivePrefixHistogram(instance.getContext().getFoundationDB());
        groundTruth = new TreeMap<>(Arrays::compareUnsigned);
    }

    // ===== 1. INVARIANTS & STRUCTURAL CORRECTNESS =====

    @Test
    public void testHelperFunctions() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        
        // Test S(d) = 256^(D_max - d)
        assertEquals(65536L, metadata.leafWidth(1)); // 256^(3-1) = 256^2
        assertEquals(256L, metadata.leafWidth(2));   // 256^(3-2) = 256^1
        assertEquals(1L, metadata.leafWidth(3));     // 256^(3-3) = 256^0
        
        // Test child width calculation
        assertEquals(16384L, metadata.childWidth(1)); // 65536 / 4
        assertEquals(64L, metadata.childWidth(2));    // 256 / 4
        
        // Test padding
        byte[] input = "abc".getBytes(StandardCharsets.UTF_8);
        byte[] padded = APPKeySchema.rightPad(input, 5, (byte) 0x00);
        assertArrayEquals(new byte[]{97, 98, 99, (byte) 0, (byte) 0}, padded);
    }

    @Test
    public void testLeafGeometry() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        
        // Create a leaf at depth 2
        byte[] lowerBound = new byte[]{1, (byte) 0, (byte) 0};
        APPLeaf leaf = APPLeaf.create(lowerBound, 2, metadata.maxDepth());
        
        // Test U_pad - L_pad == S(d)
        long expectedWidth = metadata.leafWidth(2);
        long actualWidth = APPKeySchema.bytesToLong(leaf.upperBound()) - APPKeySchema.bytesToLong(leaf.lowerBound());
        assertEquals(expectedWidth, actualWidth);
        
        // Test parent/child relationships
        if (leaf.depth() > 1) {
            APPLeaf parent = leaf.getParent(metadata);
            assertTrue(parent.depth() < leaf.depth());
            
            // Parent should contain this leaf
            assertTrue(Arrays.compareUnsigned(parent.lowerBound(), leaf.lowerBound()) <= 0);
            assertTrue(Arrays.compareUnsigned(leaf.upperBound(), parent.upperBound()) <= 0);
        }
        
        // Test sibling relationships
        if (leaf.depth() < metadata.maxDepth()) {
            APPLeaf[] children = leaf.getChildren(metadata);
            assertEquals(metadata.fanout(), children.length);
            
            // Children should be at depth d+1
            for (APPLeaf child : children) {
                assertEquals(leaf.depth() + 1, child.depth());
            }
            
            // Children should be in ascending order and non-overlapping
            for (int i = 1; i < children.length; i++) {
                assertTrue(Arrays.compareUnsigned(children[i-1].upperBound(), children[i].lowerBound()) <= 0);
            }
        }
    }

    @Test
    public void testMassConservation() {
        APPHistogramMetadata metadata = new APPHistogramMetadata(3, 4, 8, 4, 1); // Low split threshold
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Add some keys
        String[] keys = {"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"};
        
        for (String key : keys) {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            histogram.add(bucketName, fieldName, keyBytes);
            addToGroundTruth(keyBytes, key);
        }
        
        // Check mass conservation
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        long histogramTotal = estimator.getTotalCount();
        long groundTruthTotal = groundTruth.values().stream().mapToInt(List::size).sum();
        
        assertEquals(groundTruthTotal, histogramTotal, "Mass conservation failed");
        
        // Delete some keys and check again
        for (int i = 0; i < 3; i++) {
            byte[] keyBytes = keys[i].getBytes(StandardCharsets.UTF_8);
            try (var tr = histogram.getDatabase().createTransaction()) {
                histogram.deleteValue(tr, bucketName, fieldName, keyBytes, metadata);
                tr.commit().join();
            }
            removeFromGroundTruth(keyBytes, keys[i]);
        }
        
        histogramTotal = estimator.getTotalCount();
        groundTruthTotal = groundTruth.values().stream().mapToInt(List::size).sum();
        assertEquals(groundTruthTotal, histogramTotal, "Mass conservation failed after deletes");
    }

    @Test
    public void testLeafCoverageAndDisjointness() {
        APPHistogramMetadata metadata = new APPHistogramMetadata(3, 4, 4, 2, 1); // Very low thresholds
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Add keys to force some splits
        String[] keys = {"aaa", "aab", "abb", "baa", "bab", "bbb", "caa", "cab", "cbb"};
        for (String key : keys) {
            histogram.add(bucketName, fieldName, key.getBytes(StandardCharsets.UTF_8));
        }
        
        // Trigger maintenance
        histogram.performMaintenance(bucketName, fieldName);
        
        // Get all leaf intervals
        List<APPLeaf> leaves = getAllLeaves();
        
        // Check ordering and non-overlapping
        leaves.sort((a, b) -> Arrays.compareUnsigned(a.lowerBound(), b.lowerBound()));
        
        for (int i = 1; i < leaves.size(); i++) {
            APPLeaf prev = leaves.get(i - 1);
            APPLeaf curr = leaves.get(i);
            
            // Previous leaf's upper bound should be <= current leaf's lower bound
            assertTrue(Arrays.compareUnsigned(prev.upperBound(), curr.lowerBound()) <= 0,
                    "Leaves overlap or are out of order");
        }
        
        // Check that some key can be found in exactly one leaf
        for (String key : keys) {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            APPLeaf containingLeaf = histogram.findLeaf(bucketName, fieldName, keyBytes);
            assertNotNull(containingLeaf, "No leaf found for key: " + key);
            
            // Verify no other leaf contains this key
            int containingCount = 0;
            for (APPLeaf leaf : leaves) {
                byte[] keyPad = APPKeySchema.canonicalizeLowerBound(keyBytes, metadata.maxDepth());
                if (leaf.contains(keyPad)) {
                    containingCount++;
                }
            }
            assertEquals(1, containingCount, "Key should be contained in exactly one leaf: " + key);
        }
    }

    // ===== 2. SELECTIVITY ESTIMATION VALIDATION =====

    @Test
    public void testUniformDataEstimationAccuracy() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Generate uniform random data
        Random rng = new Random(12345); // Fixed seed for reproducibility
        int numKeys = 1000;
        
        for (int i = 0; i < numKeys; i++) {
            byte[] key = generateRandomKey(rng, 8);
            String docRef = "doc" + i;
            histogram.add(bucketName, fieldName, key);
            addToGroundTruth(key, docRef);
        }
        
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        
        // Test range queries with different sizes
        double[] relativeErrors = new double[100];
        
        for (int i = 0; i < 100; i++) {
            // Generate random range
            byte[] lower = generateRandomKey(rng, 8);
            byte[] upper = generateRandomKey(rng, 8);
            
            if (Arrays.compareUnsigned(lower, upper) > 0) {
                // Swap to ensure lower <= upper
                byte[] temp = lower;
                lower = upper;
                upper = temp;
            }
            
            double estimate = estimator.estimateRange(lower, upper);
            long truth = countRangeInGroundTruth(lower, upper);
            
            double relativeError = Math.abs(estimate - truth) / Math.max(1.0, truth);
            relativeErrors[i] = relativeError;
        }
        
        // Calculate percentiles
        Arrays.sort(relativeErrors);
        double p50 = relativeErrors[50];
        double p90 = relativeErrors[90];
        double p99 = relativeErrors[99];
        
        System.out.println("Uniform data estimation errors - P50: " + p50 + ", P90: " + p90 + ", P99: " + p99);
        
        // Acceptance criteria (relaxed for initial implementation)
        assertTrue(p90 <= 0.30, "P90 error should be <= 30% for uniform data, got: " + p90);
        assertTrue(p99 <= 0.50, "P99 error should be <= 50% for uniform data, got: " + p99);
    }

    @Test
    public void testMonotonicityProperty() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Add some data
        Random rng = new Random(54321);
        for (int i = 0; i < 500; i++) {
            byte[] key = generateRandomKey(rng, 6);
            histogram.add(bucketName, fieldName, key);
        }
        
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        
        // Test monotonicity: if [A,B) ⊆ [C,D), then est(A,B) ≤ est(C,D)
        for (int i = 0; i < 20; i++) {
            byte[] a = generateRandomKey(rng, 6);
            byte[] b = generateRandomKey(rng, 6);
            byte[] c = generateRandomKey(rng, 6);
            byte[] d = generateRandomKey(rng, 6);
            
            // Ensure proper ordering: c <= a < b <= d
            byte[][] sorted = {c, a, b, d};
            Arrays.sort(sorted, Arrays::compareUnsigned);
            c = sorted[0]; a = sorted[1]; b = sorted[2]; d = sorted[3];
            
            double estAB = estimator.estimateRange(a, b);
            double estCD = estimator.estimateRange(c, d);
            
            assertTrue(estAB <= estCD + 1, // Allow small rounding error
                    String.format("Monotonicity violated: est([%s,%s)) = %.2f > est([%s,%s)) = %.2f",
                            Arrays.toString(a), Arrays.toString(b), estAB,
                            Arrays.toString(c), Arrays.toString(d), estCD));
        }
    }

    // ===== HELPER METHODS =====

    private void addToGroundTruth(byte[] key, String docRef) {
        groundTruth.computeIfAbsent(key, k -> new ArrayList<>()).add(docRef);
    }

    private void removeFromGroundTruth(byte[] key, String docRef) {
        List<String> docs = groundTruth.get(key);
        if (docs != null) {
            docs.remove(docRef);
            if (docs.isEmpty()) {
                groundTruth.remove(key);
            }
        }
    }

    private long countRangeInGroundTruth(byte[] lower, byte[] upper) {
        return groundTruth.subMap(lower, true, upper, false)
                .values().stream()
                .mapToLong(List::size)
                .sum();
    }

    private byte[] generateRandomKey(Random rng, int length) {
        byte[] key = new byte[length];
        rng.nextBytes(key);
        // Ensure non-negative bytes for simplicity
        for (int i = 0; i < key.length; i++) {
            key[i] = (byte) (key[i] & 0x7F);
        }
        return key;
    }

    private List<APPLeaf> getAllLeaves() {
        APPHistogramMetadata metadata = histogram.getMetadata(bucketName, fieldName);
        List<APPLeaf> leaves = new ArrayList<>();
        
        try (var tr = histogram.getDatabase().createTransaction()) {
            var subspace = histogram.getHistogramSubspace(tr, bucketName, fieldName);
            
            byte[] beginKey = subspace.pack(Tuple.from(APPKeySchema.LEAF_PREFIX));
            byte[] endKey = subspace.pack(Tuple.from(APPKeySchema.LEAF_PREFIX + "\u0000"));
            
            Range range = new Range(beginKey, endKey);
            List<KeyValue> results = tr.getRange(range).asList().join();
            
            for (KeyValue kv : results) {
                Tuple tuple = subspace.unpack(kv.getKey());
                byte[] compoundKey = (byte[]) tuple.get(1);
                APPLeaf leaf = APPLeaf.fromCompoundKey(compoundKey, metadata.maxDepth());
                leaves.add(leaf);
            }
        }
        
        return leaves;
    }
}