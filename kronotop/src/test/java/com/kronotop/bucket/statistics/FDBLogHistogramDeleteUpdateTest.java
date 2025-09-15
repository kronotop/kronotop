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
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.server.Session;
import org.bson.BsonType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for FDBLogHistogram delete and update operations.
 * Tests the LogHistogramDynamic2 delete/update semantics with deterministic sharding.
 */
class FDBLogHistogramDeleteUpdateTest extends BaseStandaloneInstanceTest {

    private final String testField = "price";
    private FDBLogHistogram histogram;
    private String testBucket; // Will be unique per test

    protected void createBucket(String bucketName) {
        // Bucket is created implicitly through BucketMetadataUtil.createOrOpen()
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
        // Create the bucket first
        createBucket(bucketName);

        createIndex(bucketName, definition);

        // Load and return metadata
        Session session = getSession();
        return BucketMetadataUtil.createOrOpen(context, session, bucketName);
    }

    @BeforeEach
    void setUp() {
        testBucket = "test_bucket_" + System.nanoTime(); // Unique bucket per test

        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING);
        BucketMetadata bucketMetadata = createIndexesAndLoadBucketMetadata(testBucket, ageIndex);
        DirectorySubspace indexSubspace = bucketMetadata.indexes().getSubspace("age");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            FDBLogHistogram.initialize(tr, indexSubspace.getPath());
            tr.commit().join();
        }
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            histogram = new FDBLogHistogram(tr, indexSubspace.getPath());
        }
    }
    
    @Test
    void testBasicDeleteOperation() {
        double value = 100.0;
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Insert value
            histogram.addValue(tr, value);
            tr.commit().join();
        }

        HistogramEstimator estimator = histogram.getEstimator();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Verify insertion
            double beforeDelete = estimator.estimateGreaterThan(tr, 50);
            assertTrue(beforeDelete > 0, "Should have positive selectivity after insert");
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Delete the same value
            histogram.deleteValue(tr, value);
            
            // Verify deletion
            double afterDelete = estimator.estimateGreaterThan(tr, 50);
            assertEquals(0.0, afterDelete, 0.01, "Selectivity should be zero after delete");
            
            tr.commit().join();
        }
    }
    
    @Test
    void testDeleteZeroValue() {
        double value = 0.0;
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Insert zero
            histogram.addValue(tr, value);
            tr.commit().join();
        }
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Delete zero
            histogram.deleteValue(tr, value);
            
            // Verify zero count is back to zero
            HistogramEstimator estimator = histogram.getEstimator();
            double afterDelete = estimator.estimateGreaterThan(tr, -1);
            assertEquals(0.0, afterDelete, 0.01, "Should have no values after deleting zero");
            
            tr.commit().join();
        }
    }
    
    @Test
    void testDeleteNegativeValue() {
        double value = -50.0;
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Insert negative value
            histogram.addValue(tr, value);
            tr.commit().join();
        }
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Delete negative value
            histogram.deleteValue(tr, value);
            
            // Verify deletion
            HistogramEstimator estimator = histogram.getEstimator();
            double afterDelete = estimator.estimateGreaterThan(tr, -100);
            assertEquals(0.0, afterDelete, 0.01, "Should have no values after delete");
            
            tr.commit().join();
        }
    }
    
    @Test
    void testMultipleIdenticalValues() {
        double value = 100.0;
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Insert same value multiple times
            histogram.addValue(tr, value);
            histogram.addValue(tr, value);
            histogram.addValue(tr, value);
            tr.commit().join();
        }
        
        HistogramEstimator estimator = histogram.getEstimator();
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            double beforeDelete = estimator.estimateGreaterThan(tr, 50);
            assertTrue(beforeDelete > 0, "Should have positive selectivity with 3 values");
            
            // Delete one instance
            histogram.deleteValue(tr, value);
            
            // Should still have values (but fewer)
            double afterOneDelete = estimator.estimateGreaterThan(tr, 50);
            assertTrue(afterOneDelete > 0, "Should still have positive selectivity after one delete");
            
            // Delete remaining instances
            histogram.deleteValue(tr, value);
            histogram.deleteValue(tr, value);
            
            // Now should be zero
            double afterAllDeletes = estimator.estimateGreaterThan(tr, 50);
            assertEquals(0.0, afterAllDeletes, 0.01, "Should have zero selectivity after all deletes");
            
            tr.commit().join();
        }
    }
    
    @Test
    void testBasicUpdateOperation() {
        double oldValue = 50.0;
        double newValue = 150.0;
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Insert initial value
            histogram.addValue(tr, oldValue);
            tr.commit().join();
        }
        
        HistogramEstimator estimator = histogram.getEstimator();
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Verify initial state
            double beforeUpdate = estimator.estimateGreaterThan(tr, 100);
            assertEquals(0.0, beforeUpdate, 0.01, "Should not have values > 100 initially");
            
            double initialCount = estimator.estimateGreaterThan(tr, 0);
            assertTrue(initialCount > 0, "Should have positive values initially");
            
            // Update the value
            histogram.updateValue(tr, oldValue, newValue);
            
            // Verify update results
            double afterUpdate = estimator.estimateGreaterThan(tr, 100);
            assertTrue(afterUpdate > 0, "Should have values > 100 after update");
            
            double lowValues = estimator.estimateGreaterThan(tr, 25);
            assertTrue(lowValues > 0, "Should still have some values after update");
            
            tr.commit().join();
        }
    }
    
    @Test
    void testUpdateToZero() {
        double oldValue = 100.0;
        double newValue = 0.0;
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Insert positive value
            histogram.addValue(tr, oldValue);
            tr.commit().join();
        }
        
        HistogramEstimator estimator = histogram.getEstimator();
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Update to zero
            histogram.updateValue(tr, oldValue, newValue);
            
            // Should have zero positive values
            double positiveValues = estimator.estimateGreaterThan(tr, 0);
            assertEquals(0.0, positiveValues, 0.01, "Should have no positive values");
            
            // Should have some values >= -1 (including zero)
            double allValues = estimator.estimateGreaterThan(tr, -1);
            assertTrue(allValues > 0, "Should have values including zero");
            
            tr.commit().join();
        }
    }
    
    @Test
    void testUpdateSignFlip() {
        double oldValue = -100.0;
        double newValue = 200.0;
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Insert negative value
            histogram.addValue(tr, oldValue);
            tr.commit().join();
        }
        
        HistogramEstimator estimator = histogram.getEstimator();
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Verify initial negative value
            double initialPositive = estimator.estimateGreaterThan(tr, 0);
            assertEquals(0.0, initialPositive, 0.01, "Should have no positive values initially");
            
            double initialNegatives = estimator.estimateGreaterThan(tr, -200);
            assertTrue(initialNegatives > 0, "Should have negative values initially");
            
            // Update from negative to positive (sign flip)
            histogram.updateValue(tr, oldValue, newValue);
            
            // Verify sign flip results
            double finalPositive = estimator.estimateGreaterThan(tr, 0);
            assertTrue(finalPositive > 0, "Should have positive values after sign flip");
            
            double finalLowNegatives = estimator.estimateGreaterThan(tr, -50);
            assertTrue(finalLowNegatives > 0, "Should have positive values in this range");
            
            tr.commit().join();
        }
    }
    
    @Test
    void testNoOpUpdate() {
        double value = 100.0;
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Insert value
            histogram.addValue(tr, value);
            tr.commit().join();
        }
        
        HistogramEstimator estimator = histogram.getEstimator();
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            double beforeUpdate = estimator.estimateGreaterThan(tr, 50);
            
            // Update to same value (no-op)
            histogram.updateValue(tr, value, value);
            
            // Should be unchanged
            double afterUpdate = estimator.estimateGreaterThan(tr, 50);
            assertEquals(beforeUpdate, afterUpdate, 0.01, "No-op update should not change selectivity");
            
            tr.commit().join();
        }
    }
    
    @Test
    void testDeterministicSharding() {
        double value1 = 100.0;
        double value2 = 200.0;
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Insert value
            histogram.addValue(tr, value1);
            tr.commit().join();
        }
        
        HistogramEstimator estimator = histogram.getEstimator();
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Update should work correctly
            histogram.updateValue(tr, value1, value2);
            
            // Should have the new value, not the old
            double lowValues = estimator.estimateGreaterThan(tr, 50);
            assertTrue(lowValues > 0, "Should have values after update");
            
            double highValues = estimator.estimateGreaterThan(tr, 150);
            assertTrue(highValues > 0, "Should have high values after update");
            
            tr.commit().join();
        }
    }
    
    @Test
    void testMixedOperations() {
        // Test complex scenario with inserts, deletes, and updates
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Insert various values
            histogram.addValue(tr, 100.0);
            histogram.addValue(tr, -50.0);
            histogram.addValue(tr, 0.0);
            tr.commit().join();
        }
        
        HistogramEstimator estimator = histogram.getEstimator();
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            double initialTotal = estimator.estimateGreaterThan(tr, -100);
            assertTrue(initialTotal > 0, "Should have values after initial inserts");
            
            // Update one value
            histogram.updateValue(tr, 100.0, 200.0);
            
            // Delete one value  
            histogram.deleteValue(tr, -50.0);
            
            // Should still have some values (zero and 200)
            double finalTotal = estimator.estimateGreaterThan(tr, -100);
            assertTrue(finalTotal > 0, "Should still have values after mixed operations");
            
            // Should have high value
            double highValues = estimator.estimateGreaterThan(tr, 150);
            assertTrue(highValues > 0, "Should have high value after update");
            
            tr.commit().join();
        }
    }
    
    @Test
    void testPublicDeleteMethod() {
        double value = 250.0;
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Insert value
            histogram.addValue(tr, value);
            tr.commit().join();
        }
        
        HistogramEstimator estimator = histogram.getEstimator();
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Verify insertion
            double beforeDelete = estimator.estimateGreaterThan(tr, 200);
            assertTrue(beforeDelete > 0, "Should have positive selectivity after insert");
        }
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Delete value
            histogram.deleteValue(tr, value);
            
            // Verify deletion
            double afterDelete = estimator.estimateGreaterThan(tr, 200);
            assertEquals(0.0, afterDelete, 0.01, "Selectivity should be zero after delete");
            
            tr.commit().join();
        }
    }
    
    @Test
    void testPublicUpdateMethod() {
        double oldValue = 75.0;
        double newValue = 175.0;
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Insert value
            histogram.addValue(tr, oldValue);
            tr.commit().join();
        }
        
        HistogramEstimator estimator = histogram.getEstimator();
        
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Verify initial state
            double beforeUpdate = estimator.estimateGreaterThan(tr, 150);
            assertEquals(0.0, beforeUpdate, 0.01, "Should not have values > 150 initially");
            
            double lowValues = estimator.estimateGreaterThan(tr, 50);
            assertTrue(lowValues > 0, "Should have values > 50 initially");
            
            // Update value
            histogram.updateValue(tr, oldValue, newValue);
            
            // Verify update results
            double afterUpdate = estimator.estimateGreaterThan(tr, 150);
            assertTrue(afterUpdate > 0, "Should have values > 150 after update");
            
            double stillLowValues = estimator.estimateGreaterThan(tr, 50);
            assertTrue(stillLowValues > 0, "Should still have values > 50 after update");
            
            tr.commit().join();
        }
    }
    
    /*@Test
    void testPublicDeleteNonExistentHistogram() {
        String nonExistentBucket = "non_existent_" + System.nanoTime();
        
        // Should not throw exception when deleting from non-existent histogram
        assertDoesNotThrow(() -> {
            histogram.delete(nonExistentBucket, testField, 100.0);
        }, "Delete from non-existent histogram should not throw exception");
    }
    
    @Test
    void testPublicUpdateCreatesHistogramIfNeeded() {
        String newBucket = "new_bucket_" + System.nanoTime();
        
        // First add a value so we have something to update
        histogram.add(newBucket, testField, 50.0);
        
        // Update should work on the newly created histogram
        assertDoesNotThrow(() -> {
            histogram.update(newBucket, testField, 50.0, 150.0);
        }, "Update should work on histogram");
        
        // Verify the histogram has the updated value
        HistogramEstimator estimator = histogram.createEstimator(newBucket, testField);
        double result = estimator.estimateGreaterThan(100);
        assertTrue(result > 0, "Should have values > 100 after update");
        
        // Should have some values in the range that includes 150
        double highValues = estimator.estimateGreaterThan(125);
        assertTrue(highValues > 0, "Should have values > 125 (the new value)");
        
        // Should not have the old value
        double lowValues = estimator.estimateGreaterThan(25);
        double veryLowValues = estimator.estimateGreaterThan(75);
        // The estimate should show we have some values but they're in the higher range
        assertTrue(lowValues > 0, "Should have some values > 25");
    }
    
    @Test
    void testPublicDeleteMultipleValues() {
        double value1 = 100.0;
        double value2 = 200.0;
        double value3 = 300.0;
        
        // Insert multiple values
        histogram.add(testBucket, testField, value1);
        histogram.add(testBucket, testField, value2);
        histogram.add(testBucket, testField, value3);
        
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        double initialCount = estimator.estimateGreaterThan(50);
        assertTrue(initialCount > 0, "Should have values after inserts");
        
        // Delete values one by one
        histogram.delete(testBucket, testField, value2);
        double afterFirstDelete = estimator.estimateGreaterThan(50);
        assertTrue(afterFirstDelete > 0, "Should still have values after first delete");
        
        histogram.delete(testBucket, testField, value1);
        histogram.delete(testBucket, testField, value3);
        
        double afterAllDeletes = estimator.estimateGreaterThan(50);
        assertEquals(0.0, afterAllDeletes, 0.01, "Should have no values after all deletes");
    }
    
    @Test
    void testPublicUpdateChain() {
        double value1 = 50.0;
        double value2 = 100.0;
        double value3 = 200.0;
        
        // Insert initial value
        histogram.add(testBucket, testField, value1);
        
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        
        // Chain of updates
        histogram.update(testBucket, testField, value1, value2);
        double afterFirstUpdate = estimator.estimateGreaterThan(75);
        assertTrue(afterFirstUpdate > 0, "Should have values > 75 after first update");
        
        histogram.update(testBucket, testField, value2, value3);
        double afterSecondUpdate = estimator.estimateGreaterThan(150);
        assertTrue(afterSecondUpdate > 0, "Should have values > 150 after second update");
        
        // Should not have the intermediate values
        double lowValues = estimator.estimateGreaterThan(25);
        assertTrue(lowValues > 0, "Should have some values > 25");
        
        double midValues = estimator.estimateGreaterThan(125);
        assertTrue(midValues > 0, "Should have values > 125 (the final value)");
    }
    
    @Test
    void testPublicUpdateNoOpSameValue() {
        double value = 150.0;
        
        // Insert value
        histogram.add(testBucket, testField, value);
        
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        double beforeUpdate = estimator.estimateGreaterThan(100);
        
        // Update to same value (should be no-op)
        histogram.update(testBucket, testField, value, value);
        
        double afterUpdate = estimator.estimateGreaterThan(100);
        assertEquals(beforeUpdate, afterUpdate, 0.01, "No-op update should not change selectivity");
    }
    
    @Test
    void testPublicMethodsWithZeroValues() {
        double zeroValue = 0.0;
        double positiveValue = 100.0;
        
        // Test with zero values
        histogram.add(testBucket, testField, zeroValue);
        histogram.add(testBucket, testField, positiveValue);
        
        HistogramEstimator estimator = histogram.createEstimator(testBucket, testField);
        double beforeDelete = estimator.estimateGreaterThan(-1);
        assertTrue(beforeDelete > 0, "Should have values after inserts including zero");
        
        // Delete zero value
        histogram.delete(testBucket, testField, zeroValue);
        double afterDeleteZero = estimator.estimateGreaterThan(-1);
        assertTrue(afterDeleteZero > 0, "Should still have positive value after deleting zero");
        
        // Update positive to zero
        histogram.update(testBucket, testField, positiveValue, zeroValue);
        double afterUpdateToZero = estimator.estimateGreaterThan(0);
        assertEquals(0.0, afterUpdateToZero, 0.01, "Should have no positive values after update to zero");
        
        double afterUpdateIncludingZero = estimator.estimateGreaterThan(-1);
        assertTrue(afterUpdateIncludingZero > 0, "Should have zero value after update");
    }
    
    @Test
    void testPublicUpdateNonExistentValue() {
        String newBucket = "test_nonexistent_" + System.nanoTime();
        
        // Try to update a value that doesn't exist in a new histogram
        // This creates the histogram and effectively does: delete(nonexistent) + add(new)
        // The delete of nonexistent creates negative counts, so the result may be unpredictable
        histogram.update(newBucket, testField, 100.0, 200.0);
        
        HistogramEstimator estimator = histogram.createEstimator(newBucket, testField);
        
        // The behavior is that delete(-1) + add(+1) may not result in a clean state
        // This test documents the current behavior rather than asserting what should happen
        double result = estimator.estimateGreaterThan(150);
        
        // Since we're deleting a value that doesn't exist and then adding a new one,
        // the histogram may end up in an inconsistent state with negative counts
        // This is expected behavior - you shouldn't update values that don't exist
        System.out.println("Update nonexistent value result: " + result);
        
        // The test passes if no exception is thrown - the actual result depends on
        // how the histogram handles negative counts from deleting nonexistent values
        assertDoesNotThrow(() -> {
            estimator.estimateGreaterThan(150);
        }, "Estimator should handle inconsistent state gracefully");
    }*/
}