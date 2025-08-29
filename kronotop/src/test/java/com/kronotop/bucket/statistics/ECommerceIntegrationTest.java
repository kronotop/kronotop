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

import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test simulating an e-commerce system with 250k products
 * having prices following a Gaussian distribution between 10 and 2500.
 * Tests the LogHistogramDynamic2 implementation's ability to estimate
 * selectivities that match the expected Gaussian distribution characteristics.
 */
class ECommerceIntegrationTest extends BaseStandaloneInstanceTest {
    
    private FDBLogHistogram histogram;
    private String bucketName;
    private final String priceField = "price";
    private final int TOTAL_PRODUCTS = 250_000;
    
    // Gaussian distribution parameters (mean=1255, std=417 covers range [10, 2500] well)
    private final double MEAN_PRICE = 1255.0;
    private final double STD_DEV = 417.0;
    private final double MIN_PRICE = 10.0;
    private final double MAX_PRICE = 2500.0;
    
    @BeforeEach
    void setUp() {
        histogram = new FDBLogHistogram(instance.getContext().getFoundationDB());
        bucketName = "ecommerce_products_" + System.nanoTime();
    }
    
    @Test
    @Disabled
    void testECommerceGaussianPriceDistribution() {
        // Initialize histogram with larger parameters for 250k entries
        HistogramMetadata metadata = new HistogramMetadata(32, 8, 15, 64, 1);
        histogram.initialize(bucketName, priceField, metadata);
        
        System.out.println("🏪 E-Commerce Integration Test: Inserting 250k products with Gaussian price distribution");
        System.out.println("📊 Distribution: μ=" + MEAN_PRICE + ", σ=" + STD_DEV + ", range=[" + MIN_PRICE + ", " + MAX_PRICE + "]");
        
        // Generate and insert 250k prices with Gaussian distribution
        Random random = new Random(12345L); // Fixed seed for reproducible tests
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < TOTAL_PRODUCTS; i++) {
            double price = generateGaussianPrice(random);
            histogram.add(bucketName, priceField, price);
            
            // Progress logging
            if ((i + 1) % 50_000 == 0) {
                long elapsed = System.currentTimeMillis() - startTime;
                System.out.println("  📦 Inserted " + (i + 1) + " products (" + (elapsed / 1000.0) + "s)");
            }
        }
        
        long totalTime = System.currentTimeMillis() - startTime;
        System.out.println("✅ Completed insertion of " + TOTAL_PRODUCTS + " products in " + (totalTime / 1000.0) + "s");
        System.out.println("   ⚡ Rate: " + (TOTAL_PRODUCTS / (totalTime / 1000.0)) + " products/sec");
        
        // Create estimator and test Gaussian distribution properties
        HistogramEstimator estimator = histogram.createEstimator(bucketName, priceField);
        
        // Test 1: Central tendency - prices around the mean should have highest density
        System.out.println("\n📈 Testing Gaussian Distribution Properties:");
        
        // Test median region (around mean ± 0.5σ should contain ~38% of data)
        double medianLow = MEAN_PRICE - 0.5 * STD_DEV;
        double medianHigh = MEAN_PRICE + 0.5 * STD_DEV;
        double centralDensity = estimator.estimateRange(medianLow, medianHigh);
        System.out.println("  🎯 Central region [" + medianLow + ", " + medianHigh + "): " + 
                          String.format("%.1f%%", centralDensity * 100) + " (expected ~38%)");
        assertTrue(centralDensity >= 0.30 && centralDensity <= 0.46, 
                  "Central region should contain 30-46% of data, got " + (centralDensity * 100) + "%");
        
        // Test 2: Standard deviation bands (68-95-99.7 rule approximation)
        // Within 1σ should contain ~68% of data
        double oneStdLow = MEAN_PRICE - STD_DEV;
        double oneStdHigh = MEAN_PRICE + STD_DEV;
        double oneStdRange = estimator.estimateRange(oneStdLow, oneStdHigh);
        System.out.println("  📊 1σ range [" + oneStdLow + ", " + oneStdHigh + "): " + 
                          String.format("%.1f%%", oneStdRange * 100) + " (expected ~68%)");
        assertTrue(oneStdRange >= 0.60 && oneStdRange <= 0.76, 
                  "1σ range should contain 60-76% of data, got " + (oneStdRange * 100) + "%");
        
        // Within 2σ should contain ~95% of data
        double twoStdLow = Math.max(MIN_PRICE, MEAN_PRICE - 2 * STD_DEV);
        double twoStdHigh = Math.min(MAX_PRICE, MEAN_PRICE + 2 * STD_DEV);
        double twoStdRange = estimator.estimateRange(twoStdLow, twoStdHigh);
        System.out.println("  📊 2σ range [" + twoStdLow + ", " + twoStdHigh + "): " + 
                          String.format("%.1f%%", twoStdRange * 100) + " (expected ~95%)");
        assertTrue(twoStdRange >= 0.88 && twoStdRange <= 0.98, 
                  "2σ range should contain 88-98% of data, got " + (twoStdRange * 100) + "%");
        
        // Test 3: Tail behavior - extreme values should be rare
        double expensiveThreshold = MEAN_PRICE + 1.5 * STD_DEV; // ~1883
        double expensiveRate = estimator.estimateGreaterThan(expensiveThreshold);
        System.out.println("  💰 Expensive products (>" + expensiveThreshold + "): " + 
                          String.format("%.1f%%", expensiveRate * 100) + " (expected ~7%)");
        assertTrue(expensiveRate >= 0.04 && expensiveRate <= 0.12, 
                  "Expensive products should be 4-12%, got " + (expensiveRate * 100) + "%");
        
        double cheapThreshold = MEAN_PRICE - 1.5 * STD_DEV; // ~627
        double cheapRate = estimator.estimateGreaterThan(cheapThreshold);
        double budgetRate = 1.0 - cheapRate;
        System.out.println("  💵 Budget products (<" + cheapThreshold + "): " + 
                          String.format("%.1f%%", budgetRate * 100) + " (expected ~7%)");
        assertTrue(budgetRate >= 0.04 && budgetRate <= 0.12, 
                  "Budget products should be 4-12%, got " + (budgetRate * 100) + "%");
        
        // Test 4: Price band analysis (typical e-commerce categories)
        testPriceBands(estimator);
        
        // Test 5: Query performance validation
        testQueryPerformance(estimator);
        
        System.out.println("\n🎉 E-Commerce Integration Test Passed!");
        System.out.println("   ✅ LogHistogramDynamic2 successfully models Gaussian price distribution");
        System.out.println("   ✅ Selectivity estimates match expected statistical properties");
        System.out.println("   ✅ Ready for production e-commerce query optimization");
    }
    
    /**
     * Generates a price following Gaussian distribution, clamped to [MIN_PRICE, MAX_PRICE]
     */
    private double generateGaussianPrice(Random random) {
        double price;
        do {
            price = random.nextGaussian() * STD_DEV + MEAN_PRICE;
        } while (price < MIN_PRICE || price > MAX_PRICE);
        
        // Round to 2 decimal places (cents)
        return Math.round(price * 100.0) / 100.0;
    }
    
    /**
     * Test typical e-commerce price bands
     */
    private void testPriceBands(HistogramEstimator estimator) {
        System.out.println("\n🏷️  E-Commerce Price Band Analysis:");
        
        // Budget segment: $10-$50
        double budgetSegment = estimator.estimateRange(10, 50);
        System.out.println("  💰 Budget ($10-$50): " + String.format("%.1f%%", budgetSegment * 100));
        
        // Economy segment: $50-$200  
        double economySegment = estimator.estimateRange(50, 200);
        System.out.println("  🛒 Economy ($50-$200): " + String.format("%.1f%%", economySegment * 100));
        
        // Mid-range segment: $200-$800
        double midRangeSegment = estimator.estimateRange(200, 800);
        System.out.println("  🎯 Mid-range ($200-$800): " + String.format("%.1f%%", midRangeSegment * 100));
        
        // Premium segment: $800-$1500
        double premiumSegment = estimator.estimateRange(800, 1500);
        System.out.println("  ⭐ Premium ($800-$1500): " + String.format("%.1f%%", premiumSegment * 100));
        
        // Luxury segment: $1500+
        double luxurySegment = estimator.estimateGreaterThan(1500);
        System.out.println("  💎 Luxury ($1500+): " + String.format("%.1f%%", luxurySegment * 100));
        
        // Verify segments sum to ~100%
        double totalCoverage = budgetSegment + economySegment + midRangeSegment + premiumSegment + luxurySegment;
        System.out.println("  📊 Total coverage: " + String.format("%.1f%%", totalCoverage * 100));
        assertTrue(totalCoverage >= 0.95 && totalCoverage <= 1.05, 
                  "Price band coverage should be 95-105%, got " + (totalCoverage * 100) + "%");
    }
    
    /**
     * Test query performance with various selectivity patterns
     */
    private void testQueryPerformance(HistogramEstimator estimator) {
        System.out.println("\n⚡ Query Performance Validation:");
        
        // Test common e-commerce queries
        long startTime = System.nanoTime();
        
        // High selectivity query (rare expensive items)
        double luxuryQuery = estimator.estimateGreaterThan(2000);
        
        // Medium selectivity query (mid-range products)
        double midRangeQuery = estimator.estimateRange(500, 1000);
        
        // Low selectivity query (most products)
        double broadQuery = estimator.estimateRange(100, 2000);
        
        // Range scan simulation
        double[] thresholds = {100, 200, 500, 800, 1200, 1500, 1800, 2000};
        for (double threshold : thresholds) {
            estimator.estimateGreaterThan(threshold);
        }
        
        long queryTime = System.nanoTime() - startTime;
        double queryTimeMs = queryTime / 1_000_000.0;
        
        System.out.println("  🚀 Query execution time: " + String.format("%.2fms", queryTimeMs) + " for 11 queries");
        System.out.println("  📈 Average per query: " + String.format("%.3fms", queryTimeMs / 11));
        
        // Performance should be excellent (sub-millisecond per query)
        assertTrue(queryTimeMs < 100, "Query performance should be <100ms total, got " + queryTimeMs + "ms");
        
        // Validate query results are reasonable
        assertTrue(luxuryQuery >= 0.0 && luxuryQuery <= 0.1, "Luxury query selectivity should be 0-10%");
        assertTrue(midRangeQuery >= 0.1 && midRangeQuery <= 0.4, "Mid-range query selectivity should be 10-40%");
        assertTrue(broadQuery >= 0.7 && broadQuery <= 1.0, "Broad query selectivity should be 70-100%");
        
        System.out.println("  ✅ All queries returned reasonable selectivity estimates");
    }
}