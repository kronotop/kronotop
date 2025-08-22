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

package com.kronotop.bucket.optimizer;

import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.planner.physical.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for RangeScanFallbackRule to ensure it works properly
 * within the full optimizer pipeline and handles cases where no suitable
 * index is available for range queries.
 */
class RangeScanFallbackIntegrationTest extends BaseOptimizerTest {

    @Test
    void testRangeScanFallbackInOptimizerPipeline() {
        // Create a PhysicalRangeScan with null index (simulating no available index)
        PlannerContext context = new PlannerContext();
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                context.generateId(), "nonIndexedField", 10, 50, true, false, null
        );

        // Run through the optimizer
        PhysicalNode optimized = optimizer.optimize(metadata, rangeScan, context);

        // Should be converted to PhysicalFullScan
        assertTrue(optimized instanceof PhysicalFullScan);
        PhysicalFullScan fullScan = (PhysicalFullScan) optimized;
        
        // Should contain the composite filter
        assertTrue(fullScan.node() instanceof PhysicalAnd);
        PhysicalAnd and = (PhysicalAnd) fullScan.node();
        assertEquals(2, and.children().size());
    }

    @Test
    void testRangeScanWithIndexRemainsUnchanged() {
        // Create an index for this test
        IndexDefinition ageIndex = IndexDefinition.create(
                "age-index", "age", org.bson.BsonType.INT32, com.kronotop.bucket.index.SortOrder.ASCENDING
        );
        createIndex(ageIndex);

        // Create a PhysicalRangeScan with a valid index
        PlannerContext context = new PlannerContext();
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                context.generateId(), "age", 18, 65, true, false, ageIndex
        );

        // Run through the optimizer
        PhysicalNode optimized = optimizer.optimize(metadata, rangeScan, context);

        // Should remain as PhysicalRangeScan (not converted to full scan)
        assertTrue(optimized instanceof PhysicalRangeScan);
        PhysicalRangeScan optimizedScan = (PhysicalRangeScan) optimized;
        assertEquals("age", optimizedScan.selector());
        assertNotNull(optimizedScan.index());
    }

    @Test
    void testComplexPlanWithMixedRangeScans() {
        // Create an index for one field but not another
        IndexDefinition ageIndex = IndexDefinition.create(
                "age-index", "age", org.bson.BsonType.INT32, com.kronotop.bucket.index.SortOrder.ASCENDING
        );
        createIndex(ageIndex);

        PlannerContext context = new PlannerContext();
        
        // One range scan with index, one without
        PhysicalRangeScan indexedScan = new PhysicalRangeScan(
                context.generateId(), "age", 18, 65, true, false, ageIndex
        );
        PhysicalRangeScan nonIndexedScan = new PhysicalRangeScan(
                context.generateId(), "score", 80, 100, true, true, null
        );
        
        PhysicalAnd andPlan = new PhysicalAnd(
                context.generateId(), 
                java.util.List.of(indexedScan, nonIndexedScan)
        );

        // Run through the optimizer
        PhysicalNode optimized = optimizer.optimize(metadata, andPlan, context);

        // Should still be an AND
        assertTrue(optimized instanceof PhysicalAnd);
        PhysicalAnd optimizedAnd = (PhysicalAnd) optimized;
        assertEquals(2, optimizedAnd.children().size());

        // Find the indexed scan (should remain as range scan)
        PhysicalNode indexedChild = optimizedAnd.children().stream()
                .filter(child -> child instanceof PhysicalRangeScan)
                .findFirst()
                .orElse(null);
        assertNotNull(indexedChild);
        PhysicalRangeScan indexedResult = (PhysicalRangeScan) indexedChild;
        assertEquals("age", indexedResult.selector());
        assertNotNull(indexedResult.index());

        // Find the non-indexed scan (should be converted to full scan)
        PhysicalNode fullScanChild = optimizedAnd.children().stream()
                .filter(child -> child instanceof PhysicalFullScan)
                .findFirst()
                .orElse(null);
        assertNotNull(fullScanChild);
        PhysicalFullScan fullScanResult = (PhysicalFullScan) fullScanChild;
        assertTrue(fullScanResult.node() instanceof PhysicalAnd);
    }

    @Test
    void testNestedStructuresWithRangeScanFallback() {
        PlannerContext context = new PlannerContext();
        
        // Create a nested structure with range scan that needs fallback
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                context.generateId(), "nonIndexedField", 1, 10, true, true, null
        );
        
        PhysicalOr orPlan = new PhysicalOr(
                context.generateId(),
                java.util.List.of(
                        rangeScan,
                        new PhysicalFilter(context.generateId(), "name", com.kronotop.bucket.planner.Operator.EQ, "test")
                )
        );

        // Run through the optimizer
        PhysicalNode optimized = optimizer.optimize(metadata, orPlan, context);

        // Should still be an OR
        assertTrue(optimized instanceof PhysicalOr);
        PhysicalOr optimizedOr = (PhysicalOr) optimized;
        assertEquals(2, optimizedOr.children().size());

        // First child should be converted to full scan
        assertTrue(optimizedOr.children().get(0) instanceof PhysicalFullScan);
        
        // Second child should remain as filter
        assertTrue(optimizedOr.children().get(1) instanceof PhysicalFilter);
    }

    @Test
    void testOptimizerRuleOrderingWithRangeScanFallback() {
        // Test that the range scan fallback rule works in conjunction with other rules
        PlannerContext context = new PlannerContext();
        
        // Create a plan that could be optimized by multiple rules
        PhysicalRangeScan rangeScan1 = new PhysicalRangeScan(
                context.generateId(), "field1", 1, 10, true, false, null
        );
        PhysicalRangeScan rangeScan2 = new PhysicalRangeScan(
                context.generateId(), "field1", 5, 15, false, true, null
        );
        
        PhysicalAnd andPlan = new PhysicalAnd(
                context.generateId(),
                java.util.List.of(rangeScan1, rangeScan2)
        );

        // Run through the optimizer (multiple rules should apply)
        PhysicalNode optimized = optimizer.optimize(metadata, andPlan, context);

        // Both range scans should be converted to full scans
        // Note: The exact result depends on other optimization rules, 
        // but we verify that the fallback rule has been applied
        assertNotNull(optimized);
        int fullScanCount = countNodeType(optimized, PhysicalFullScan.class);
        assertTrue(fullScanCount > 0, "At least one PhysicalFullScan should be present after optimization");
    }

    @Test
    void testSingleBoundRangeScanFallback() {
        PlannerContext context = new PlannerContext();
        
        // Test with only lower bound
        PhysicalRangeScan lowerBoundOnly = new PhysicalRangeScan(
                context.generateId(), "temperature", 20, null, true, false, null
        );

        PhysicalNode optimized = optimizer.optimize(metadata, lowerBoundOnly, context);

        assertTrue(optimized instanceof PhysicalFullScan);
        PhysicalFullScan fullScan = (PhysicalFullScan) optimized;
        assertTrue(fullScan.node() instanceof PhysicalFilter);

        // Test with only upper bound
        PhysicalRangeScan upperBoundOnly = new PhysicalRangeScan(
                context.generateId(), "humidity", null, 80, false, true, null
        );

        PhysicalNode optimized2 = optimizer.optimize(metadata, upperBoundOnly, context);

        assertTrue(optimized2 instanceof PhysicalFullScan);
        PhysicalFullScan fullScan2 = (PhysicalFullScan) optimized2;
        assertTrue(fullScan2.node() instanceof PhysicalFilter);
    }
}