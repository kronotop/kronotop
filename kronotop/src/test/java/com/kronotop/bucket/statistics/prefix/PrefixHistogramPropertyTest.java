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

import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based tests for Fixed N-Byte Prefix Histogram with different data distributions.
 * Tests accuracy targets: Uniform P90 ≤ 15%, Zipf/Clustered P90 ≤ 25%.
 */
public class PrefixHistogramPropertyTest extends BaseStandaloneInstanceTest {

    private FDBPrefixHistogram histogram;
    private PrefixHistogramEstimator estimator;
    private String testIndexName;
    private DirectorySubspace testIndexSubspace;

    @BeforeEach
    public void setupHistogram() {
        histogram = new FDBPrefixHistogram(context.getFoundationDB());
        testIndexName = "property-test-index";
        estimator = histogram.createEstimator(testIndexName);
        
        try (var tr = context.getFoundationDB().createTransaction()) {
            testIndexSubspace = DirectoryLayer.getDefault().createOrOpen(tr, 
                Arrays.asList("IDX", testIndexName)).join();
            tr.commit().join();
        }
    }

    @Test
    @Disabled
    public void testUniformDistribution() {
        DataGenerator generator = new UniformDataGenerator(42);
        Oracle oracle = generateData(generator, 50000);
        
        List<Double> errors = collectRangeErrors(oracle, 1000);
        
        double p50 = percentile(errors, 0.50);
        double p90 = percentile(errors, 0.90);
        double p99 = percentile(errors, 0.99);
        
        System.out.printf("Uniform Distribution - P50: %.3f, P90: %.3f, P99: %.3f%n", p50, p90, p99);
        
        // Acceptance criteria: P90 ≤ 15% for uniform
        assertTrue(p90 <= 0.15, String.format("P90 error %.3f exceeds 15%% threshold", p90));
        assertTrue(p99 <= 0.30, String.format("P99 error %.3f exceeds 30%% threshold", p99));
    }

    @Test
    @Disabled
    public void testZipfDistribution() {
        DataGenerator generator = new ZipfDataGenerator(42, 0.8); // 80% concentration
        Oracle oracle = generateData(generator, 50000);
        
        List<Double> errors = collectRangeErrors(oracle, 1000);
        
        double p50 = percentile(errors, 0.50);
        double p90 = percentile(errors, 0.90);
        double p99 = percentile(errors, 0.99);
        
        System.out.printf("Zipf Distribution - P50: %.3f, P90: %.3f, P99: %.3f%n", p50, p90, p99);
        
        // Acceptance criteria: P90 ≤ 25% for Zipf/skewed
        assertTrue(p90 <= 0.25, String.format("P90 error %.3f exceeds 25%% threshold", p90));
        assertTrue(p99 <= 0.50, String.format("P99 error %.3f exceeds 50%% threshold", p99));
    }

    @Test
    @Disabled
    public void testClusteredDistribution() {
        DataGenerator generator = new ClusteredDataGenerator(42, 5); // 5 clusters
        Oracle oracle = generateData(generator, 50000);
        
        List<Double> errors = collectRangeErrors(oracle, 1000);
        
        double p50 = percentile(errors, 0.50);
        double p90 = percentile(errors, 0.90);
        double p99 = percentile(errors, 0.99);
        
        System.out.printf("Clustered Distribution - P50: %.3f, P90: %.3f, P99: %.3f%n", p50, p90, p99);
        
        // Acceptance criteria: P90 ≤ 25% for clustered
        assertTrue(p90 <= 0.25, String.format("P90 error %.3f exceeds 25%% threshold", p90));
        assertTrue(p99 <= 0.50, String.format("P99 error %.3f exceeds 50%% threshold", p99));
    }

    @Test
    @Disabled
    public void testMixedDistribution() {
        DataGenerator generator = new MixedDataGenerator(42); // Uniform + hot clusters
        Oracle oracle = generateData(generator, 50000);
        
        List<Double> errors = collectRangeErrors(oracle, 1000);
        
        double p50 = percentile(errors, 0.50);
        double p90 = percentile(errors, 0.90);
        double p99 = percentile(errors, 0.99);
        
        System.out.printf("Mixed Distribution - P50: %.3f, P90: %.3f, P99: %.3f%n", p50, p90, p99);
        
        // Acceptance criteria: P90 ≤ 25% for mixed
        assertTrue(p90 <= 0.25, String.format("P90 error %.3f exceeds 25%% threshold", p90));
        assertTrue(p99 <= 0.50, String.format("P99 error %.3f exceeds 50%% threshold", p99));
    }

    @Test
    @Disabled
    public void testEdgeCases() {
        // Edge keys: 00...00, FF...FF, short/long keys
        byte[][] edgeKeys = {
            new byte[]{0x00, 0x00},
            new byte[]{(byte)0xFF, (byte)0xFF},
            new byte[]{0x00},           // Short key
            new byte[16],               // Long key (all zeros)
            new byte[]{(byte)0xFF},     // Short FF
            new byte[32]                // Very long key
        };
        
        Arrays.fill(edgeKeys[5], (byte)0xFF); // Fill long key with FF
        
        Oracle oracle = new Oracle();
        for (byte[] key : edgeKeys) {
            for (int i = 0; i < 100; i++) {
                histogram.add(testIndexName, key);
                oracle.add(key);
            }
        }
        
        // Test ranges involving edge keys
        for (int i = 0; i < edgeKeys.length - 1; i++) {
            byte[] A = edgeKeys[i];
            byte[] B = edgeKeys[i + 1];
            
            if (Arrays.compareUnsigned(A, B) > 0) {
                continue; // Skip invalid ranges
            }
            
            long truth = oracle.countRange(A, B);
            double estimate = estimator.estimateRange(A, B);
            
            if (truth > 0) {
                double error = Math.abs(estimate - truth) / truth;
                assertTrue(error < 0.5, 
                    String.format("Edge case error too high: %.3f for range [%s, %s)", 
                                error, bytesToHex(A), bytesToHex(B)));
            }
        }
    }

    private Oracle generateData(DataGenerator generator, int count) {
        Oracle oracle = new Oracle();
        
        for (int i = 0; i < count; i++) {
            byte[] key = generator.generate();
            histogram.add(testIndexName, key);
            oracle.add(key);
        }
        
        return oracle;
    }

    private List<Double> collectRangeErrors(Oracle oracle, int numRanges) {
        List<Double> errors = new ArrayList<>();
        Random random = new Random(42);
        
        for (int i = 0; i < numRanges; i++) {
            byte[] A = generateRandomKey(random, 8);
            byte[] B = generateRandomKey(random, 8);
            
            // Ensure A < B lexicographically
            if (Arrays.compareUnsigned(A, B) > 0) {
                byte[] temp = A;
                A = B;
                B = temp;
            }
            
            long truth = oracle.countRange(A, B);
            double estimate = estimator.estimateRange(A, B);
            
            // Skip very small ranges for error calculation
            if (truth >= 4) {
                double error = Math.abs(estimate - truth) / truth;
                errors.add(error);
            }
        }
        
        return errors;
    }

    private byte[] generateRandomKey(Random random, int maxLength) {
        int length = 1 + random.nextInt(maxLength);
        byte[] key = new byte[length];
        random.nextBytes(key);
        return key;
    }

    private double percentile(List<Double> values, double p) {
        if (values.isEmpty()) return 0.0;
        
        Collections.sort(values);
        int index = Math.min((int) Math.ceil(p * values.size()) - 1, values.size() - 1);
        index = Math.max(0, index);
        return values.get(index);
    }

    private String bytesToHex(byte[] bytes) {
        return PrefixHistogramUtils.bytesToHex(bytes);
    }

    // Data Generators

    interface DataGenerator {
        byte[] generate();
    }

    static class UniformDataGenerator implements DataGenerator {
        private final Random random;

        UniformDataGenerator(long seed) {
            this.random = new Random(seed);
        }

        @Override
        public byte[] generate() {
            int length = 1 + random.nextInt(16);
            byte[] key = new byte[length];
            random.nextBytes(key);
            return key;
        }
    }

    static class ZipfDataGenerator implements DataGenerator {
        private final Random random;
        private final double concentration;
        private final byte[][] hotPrefixes;

        ZipfDataGenerator(long seed, double concentration) {
            this.random = new Random(seed);
            this.concentration = concentration;
            this.hotPrefixes = new byte[][]{
                {0x41, 0x42}, // "AB"
                {0x43, 0x44}, // "CD" 
                {0x45, 0x46}  // "EF"
            };
        }

        @Override
        public byte[] generate() {
            if (random.nextDouble() < concentration) {
                // Generate hot key with shared prefix
                byte[] prefix = hotPrefixes[random.nextInt(hotPrefixes.length)];
                byte[] key = Arrays.copyOf(prefix, prefix.length + 2);
                random.nextBytes(key);
                System.arraycopy(prefix, 0, key, 0, prefix.length);
                return key;
            } else {
                // Generate uniform key
                return new UniformDataGenerator(random.nextLong()).generate();
            }
        }
    }

    static class ClusteredDataGenerator implements DataGenerator {
        private final Random random;
        private final byte[][] clusterPrefixes;

        ClusteredDataGenerator(long seed, int numClusters) {
            this.random = new Random(seed);
            this.clusterPrefixes = new byte[numClusters][];
            
            for (int i = 0; i < numClusters; i++) {
                clusterPrefixes[i] = new byte[4];
                random.nextBytes(clusterPrefixes[i]);
            }
        }

        @Override
        public byte[] generate() {
            byte[] prefix = clusterPrefixes[random.nextInt(clusterPrefixes.length)];
            byte[] key = Arrays.copyOf(prefix, prefix.length + 4);
            
            // Add some variation to the suffix
            for (int i = prefix.length; i < key.length; i++) {
                key[i] = (byte) random.nextInt(16); // Limited variation within cluster
            }
            
            return key;
        }
    }

    static class MixedDataGenerator implements DataGenerator {
        private final UniformDataGenerator uniform;
        private final ClusteredDataGenerator clustered;
        private final Random random;

        MixedDataGenerator(long seed) {
            this.random = new Random(seed);
            this.uniform = new UniformDataGenerator(seed);
            this.clustered = new ClusteredDataGenerator(seed + 1, 3);
        }

        @Override
        public byte[] generate() {
            return random.nextBoolean() ? uniform.generate() : clustered.generate();
        }
    }

    // Oracle implementation (same as main test)
    private static class Oracle {
        private final TreeMap<byte[], Integer> data = new TreeMap<>(Arrays::compareUnsigned);

        public void add(byte[] key) {
            data.merge(key.clone(), 1, Integer::sum);
        }

        public long countRange(byte[] A, byte[] B) {
            return data.subMap(A, true, B, false)
                      .values().stream()
                      .mapToInt(Integer::intValue)
                      .sum();
        }
    }
}