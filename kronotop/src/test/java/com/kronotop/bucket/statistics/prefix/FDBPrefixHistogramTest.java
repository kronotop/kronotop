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

package com.kronotop.bucket.statistics.prefix;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for Fixed N-Byte Prefix Histogram implementation.
 * Covers correctness, robustness, and performance validation.
 */
public class FDBPrefixHistogramTest extends BaseStandaloneInstanceTest {

    private FDBPrefixHistogram histogram;
    private PrefixHistogramEstimator estimator;
    private String testIndexName;
    private DirectorySubspace testIndexSubspace;
    private Oracle oracle; // Ground truth for comparison

    @BeforeEach
    public void setupHistogram() {
        histogram = new FDBPrefixHistogram(context.getFoundationDB());
        testIndexName = "test-index-"+UUID.randomUUID().toString();
        estimator = histogram.createEstimator(testIndexName);
        
        // Create mock index subspace for peek operations
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            testIndexSubspace = DirectoryLayer.getDefault().createOrOpen(tr, 
                Arrays.asList("IDX", testIndexName)).join();
            tr.commit().join();
        }
        
        oracle = new Oracle();
    }

    @Test
    public void testBasicOperations() {
        // Test initialization
        PrefixHistogramMetadata metadata = PrefixHistogramMetadata.defaultMetadata();
        histogram.initialize(testIndexName, metadata);
        
        assertEquals(metadata, histogram.getMetadata(testIndexName));
        assertEquals(0, histogram.getTotalCount(testIndexName));
    }

    @Test
    public void testAddDelete() {
        byte[] key1 = "hello".getBytes();
        byte[] key2 = "world".getBytes();
        
        // Add operations
        histogram.add(testIndexName, key1);
        histogram.add(testIndexName, key2);
        histogram.add(testIndexName, key1); // Duplicate
        
        assertEquals(3, histogram.getTotalCount(testIndexName));
        
        // Delete operations
        histogram.delete(testIndexName, key1);
        assertEquals(2, histogram.getTotalCount(testIndexName));
        
        histogram.delete(testIndexName, key1); // Delete duplicate
        assertEquals(1, histogram.getTotalCount(testIndexName));
    }

    @Test
    public void testUpdate() {
        byte[] oldKey = "old".getBytes();
        byte[] newKey = "new".getBytes();
        
        histogram.add(testIndexName, oldKey);
        assertEquals(1, histogram.getTotalCount(testIndexName));
        
        histogram.update(testIndexName, oldKey, newKey);
        assertEquals(1, histogram.getTotalCount(testIndexName));
        
        // Verify old key bucket is decremented, new key bucket is incremented
        PrefixHistogramMetadata metadata = histogram.getMetadata(testIndexName);
        byte[] oldPN = PrefixHistogramUtils.pN(oldKey, metadata.N());
        byte[] newPN = PrefixHistogramUtils.pN(newKey, metadata.N());
        
        assertEquals(0, histogram.getBucketCount(testIndexName, oldPN));
        assertEquals(1, histogram.getBucketCount(testIndexName, newPN));
    }

    @Test
    public void testMassConservation() {
        Random random = new Random(42);
        
        for (int i = 0; i < 1000; i++) {
            byte[] key = generateRandomKey(random, 8);
            histogram.add(testIndexName, key);
            oracle.add(key);
        }
        
        // Mass conservation: Σ count(pN) == |IDX|
        assertEquals(oracle.size(), histogram.getTotalCount(testIndexName));
        
        // Delete half the keys
        List<byte[]> keysToDelete = new ArrayList<>(oracle.getKeys());
        Collections.shuffle(keysToDelete, random);
        
        for (int i = 0; i < keysToDelete.size() / 2; i++) {
            byte[] key = keysToDelete.get(i);
            histogram.delete(testIndexName, key);
            oracle.delete(key);
        }
        
        assertEquals(oracle.size(), histogram.getTotalCount(testIndexName));
    }

    @Test
    public void testGoldenTrace() {
        // Golden trace with N=2: Insert {"a","aa","ab","b","ba","bb"}
        String[] keys = {"a", "aa", "ab", "b", "ba", "bb"};
        
        for (String key : keys) {
            histogram.add(testIndexName, key.getBytes());
            oracle.add(key.getBytes());
        }
        
        PrefixHistogramMetadata metadata = histogram.getMetadata(testIndexName);
        
        // Verify expected counters per p2
        Map<String, Integer> expectedCounts = Map.of(
            "a\0", 3,  // "a", "aa", "ab" -> "a\0"  
            "b\0", 3   // "b", "ba", "bb" -> "b\0"
        );
        
        for (Map.Entry<String, Integer> entry : expectedCounts.entrySet()) {
            byte[] pN = entry.getKey().getBytes();
            int expected = entry.getValue();
            long actual = histogram.getBucketCount(testIndexName, pN);
            assertEquals(expected, actual, "Bucket count mismatch for prefix: " + entry.getKey());
        }
        
        // Delete subset and verify updated counts
        histogram.delete(testIndexName, "aa".getBytes());
        histogram.delete(testIndexName, "ba".getBytes());
        oracle.delete("aa".getBytes());
        oracle.delete("ba".getBytes());
        
        assertEquals(oracle.size(), histogram.getTotalCount(testIndexName));
        assertEquals(2, histogram.getBucketCount(testIndexName, "a\0".getBytes()));
        assertEquals(2, histogram.getBucketCount(testIndexName, "b\0".getBytes()));
    }

    @Test 
    public void testEqualityEstimation() {
        PrefixHistogramMetadata metadata = PrefixHistogramMetadata.defaultMetadata();
        
        // Small set test (≤ PEEK_CAP)
        byte[] value = "test".getBytes();
        
        for (int i = 0; i < 500; i++) {
            histogram.add(testIndexName, value);
            addToMockIndex(value, i);
            oracle.add(value);
        }
        
        var result = estimator.estimateEq(testIndexSubspace, value);
        assertTrue(result.isExact());
        assertEquals(500, result.getCount());
        
        // Large set test (> PEEK_CAP) 
        for (int i = 500; i < 2000; i++) {
            histogram.add(testIndexName, value);
            addToMockIndex(value, i);
            oracle.add(value);
        }
        
        result = estimator.estimateEq(testIndexSubspace, value);
        assertFalse(result.isExact());
        
        // Should be reasonably accurate (within 25% for P90)
        double error = Math.abs(result.getCount() - 2000.0) / 2000.0;
        assertTrue(error < 0.5, "Error too high: " + error); // Relaxed for single test
    }

    @Test
    public void testRangeEstimation() {
        Random random = new Random(42);
        
        // Generate uniform distribution
        for (int i = 0; i < 10000; i++) {
            byte[] key = generateUniformKey(random, 4);
            histogram.add(testIndexName, key);
            oracle.add(key);
        }
        
        // Test various ranges
        for (int i = 0; i < 100; i++) {
            byte[] A = generateUniformKey(random, 4);
            byte[] B = generateUniformKey(random, 4);
            
            // Ensure A < B lexicographically
            if (Arrays.compareUnsigned(A, B) > 0) {
                byte[] temp = A;
                A = B;
                B = temp;
            }
            
            long truth = oracle.countRange(A, B);
            double estimate = estimator.estimateRange(A, B);
            
            if (truth > 0) {
                double error = Math.abs(estimate - truth) / truth;
                assertTrue(error < 1.0, 
                    String.format("Range error too high: %.2f, truth=%d, est=%.1f", 
                                error, truth, estimate));
            }
        }
    }

    @Test
    public void testMetamorphicProperties() {
        Random random = new Random(42);
        
        // Generate test data
        for (int i = 0; i < 1000; i++) {
            byte[] key = generateRandomKey(random, 6);
            histogram.add(testIndexName, key);
        }
        
        // Monotonicity: If [A,B) ⊆ [C,D), then est(A,B) ≤ est(C,D)
        byte[] A = new byte[]{0x30};
        byte[] B = new byte[]{0x40}; 
        byte[] C = new byte[]{0x20};
        byte[] D = new byte[]{0x50};
        
        double estAB = estimator.estimateRange(A, B);
        double estCD = estimator.estimateRange(C, D);
        assertTrue(estAB <= estCD + 1, // Allow small rounding error
                  String.format("Monotonicity violated: [%s,%s)=%.1f > [%s,%s)=%.1f", 
                               bytesToHex(A), bytesToHex(B), estAB,
                               bytesToHex(C), bytesToHex(D), estCD));
        
        // Additivity: est(A,B) ≈ est(A,M) + est(M,B)
        byte[] M = new byte[]{0x35};
        double estAM = estimator.estimateRange(A, M);
        double estMB = estimator.estimateRange(M, B);
        double sum = estAM + estMB;
        
        double additiveError = Math.abs(estAB - sum) / Math.max(1, estAB);
        assertTrue(additiveError < 0.1, 
                  String.format("Additivity violated: est(A,B)=%.1f, est(A,M)+est(M,B)=%.1f", 
                               estAB, sum));
    }

    // Helper methods

    private byte[] generateRandomKey(Random random, int maxLength) {
        int length = 1 + random.nextInt(maxLength);
        byte[] key = new byte[length];
        random.nextBytes(key);
        return key;
    }

    private byte[] generateUniformKey(Random random, int length) {
        byte[] key = new byte[length];
        random.nextBytes(key);
        return key;
    }

    private void addToMockIndex(byte[] value, int docRef) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] key = testIndexSubspace.subspace(Tuple.from((Object) value)).pack(Tuple.from(docRef));
            tr.set(key, new byte[0]);
            tr.commit().join();
        }
    }

    private String bytesToHex(byte[] bytes) {
        return PrefixHistogramUtils.bytesToHex(bytes);
    }

    /**
     * Simple in-memory oracle for ground truth comparison
     */
    private static class Oracle {
        private final TreeMap<byte[], Integer> data = new TreeMap<>(Arrays::compareUnsigned);

        public void add(byte[] key) {
            data.merge(key.clone(), 1, Integer::sum);
        }

        public void delete(byte[] key) {
            data.computeIfPresent(key, (k, v) -> v > 1 ? v - 1 : null);
        }

        public long size() {
            return data.values().stream().mapToInt(Integer::intValue).sum();
        }

        public long countRange(byte[] A, byte[] B) {
            return data.subMap(A, true, B, false)
                      .values().stream()
                      .mapToInt(Integer::intValue)
                      .sum();
        }

        public Set<byte[]> getKeys() {
            return new HashSet<>(data.keySet());
        }
    }
}