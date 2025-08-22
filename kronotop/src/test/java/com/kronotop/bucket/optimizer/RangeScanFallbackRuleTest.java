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
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for RangeScanFallbackRule optimization.
 * Verifies that PhysicalRangeScan nodes with null indexes are correctly
 * converted to PhysicalFullScan nodes with composite filters.
 */
class RangeScanFallbackRuleTest extends BaseOptimizerTest {
    private RangeScanFallbackRule rule;
    private PlannerContext context;

    @BeforeEach
    void setUpRule() {
        rule = new RangeScanFallbackRule();
        context = new PlannerContext();
    }

    @Test
    void testRangeScanWithBothBoundsConverted() {
        // Create a range scan with both bounds but no index (null)
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "age", 18, 65, true, false, null
        );

        // Apply the rule
        PhysicalNode result = rule.apply(rangeScan, metadata, context);

        // Should be converted to PhysicalFullScan with PhysicalAnd
        assertTrue(result instanceof PhysicalFullScan);
        PhysicalFullScan fullScan = (PhysicalFullScan) result;
        
        assertTrue(fullScan.node() instanceof PhysicalAnd);
        PhysicalAnd and = (PhysicalAnd) fullScan.node();
        
        assertEquals(2, and.children().size());
        
        // Check first condition: age >= 18
        PhysicalFilter filter1 = (PhysicalFilter) and.children().get(0);
        assertEquals("age", filter1.selector());
        assertEquals(Operator.GTE, filter1.op());
        assertEquals(18, filter1.operand());
        
        // Check second condition: age < 65
        PhysicalFilter filter2 = (PhysicalFilter) and.children().get(1);
        assertEquals("age", filter2.selector());
        assertEquals(Operator.LT, filter2.op());
        assertEquals(65, filter2.operand());
    }

    @Test
    void testRangeScanWithLowerBoundOnlyConverted() {
        // Create a range scan with only lower bound
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "score", 50, null, false, false, null
        );

        PhysicalNode result = rule.apply(rangeScan, metadata, context);

        // Should be converted to PhysicalFullScan with single PhysicalFilter
        assertTrue(result instanceof PhysicalFullScan);
        PhysicalFullScan fullScan = (PhysicalFullScan) result;
        
        assertTrue(fullScan.node() instanceof PhysicalFilter);
        PhysicalFilter filter = (PhysicalFilter) fullScan.node();
        
        assertEquals("score", filter.selector());
        assertEquals(Operator.GT, filter.op());
        assertEquals(50, filter.operand());
    }

    @Test
    void testRangeScanWithUpperBoundOnlyConverted() {
        // Create a range scan with only upper bound
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "price", null, 100.0, true, true, null
        );

        PhysicalNode result = rule.apply(rangeScan, metadata, context);

        // Should be converted to PhysicalFullScan with single PhysicalFilter
        assertTrue(result instanceof PhysicalFullScan);
        PhysicalFullScan fullScan = (PhysicalFullScan) result;
        
        assertTrue(fullScan.node() instanceof PhysicalFilter);
        PhysicalFilter filter = (PhysicalFilter) fullScan.node();
        
        assertEquals("price", filter.selector());
        assertEquals(Operator.LTE, filter.op());
        assertEquals(100.0, filter.operand());
    }

    @Test
    void testRangeScanWithInclusiveBounds() {
        // Test inclusive bounds conversion
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "rating", 1, 5, true, true, null
        );

        PhysicalNode result = rule.apply(rangeScan, metadata, context);

        PhysicalFullScan fullScan = (PhysicalFullScan) result;
        PhysicalAnd and = (PhysicalAnd) fullScan.node();
        
        // Check inclusive operators
        PhysicalFilter filter1 = (PhysicalFilter) and.children().get(0);
        assertEquals(Operator.GTE, filter1.op());
        
        PhysicalFilter filter2 = (PhysicalFilter) and.children().get(1);
        assertEquals(Operator.LTE, filter2.op());
    }

    @Test
    void testRangeScanWithValidIndexNotConverted() {
        // Create a mock index for this test
        IndexDefinition mockIndex = IndexDefinition.create(
                "age-index", "age", org.bson.BsonType.INT32, com.kronotop.bucket.index.SortOrder.ASCENDING
        );
        
        // Create a range scan with a valid index (should not be converted)
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "age", 18, 65, true, false, mockIndex
        );

        PhysicalNode result = rule.apply(rangeScan, metadata, context);

        // Should remain unchanged
        assertEquals(rangeScan, result);
    }

    @Test
    void testPhysicalAndWithRangeScanFallback() {
        // Create an AND with a range scan that needs fallback
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "age", 18, 65, true, false, null
        );
        PhysicalFilter regularFilter = new PhysicalFilter(2, "name", Operator.EQ, "John");
        PhysicalAnd and = new PhysicalAnd(3, List.of(rangeScan, regularFilter));

        PhysicalNode result = rule.apply(and, metadata, context);

        // Should convert the range scan within the AND
        assertTrue(result instanceof PhysicalAnd);
        PhysicalAnd resultAnd = (PhysicalAnd) result;
        
        assertEquals(2, resultAnd.children().size());
        
        // First child should be the converted full scan
        assertTrue(resultAnd.children().get(0) instanceof PhysicalFullScan);
        
        // Second child should be the regular filter
        assertTrue(resultAnd.children().get(1) instanceof PhysicalFilter);
        PhysicalFilter filter = (PhysicalFilter) resultAnd.children().get(1);
        assertEquals("name", filter.selector());
    }

    @Test
    void testNestedStructureWithRangeScanFallback() {
        // Test nested structure with range scan that needs fallback
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "score", 50, null, true, false, null
        );
        PhysicalNot not = new PhysicalNot(2, rangeScan);

        PhysicalNode result = rule.apply(not, metadata, context);

        // Should convert the nested range scan
        assertTrue(result instanceof PhysicalNot);
        PhysicalNot resultNot = (PhysicalNot) result;
        
        assertTrue(resultNot.child() instanceof PhysicalFullScan);
    }

    @Test
    void testCanApplyWithRangeScanNullIndex() {
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "age", 18, 65, true, false, null
        );

        assertTrue(rule.canApply(rangeScan));
    }

    @Test
    void testCanApplyWithRangeScanValidIndex() {
        // Create a mock index for this test
        IndexDefinition mockIndex = IndexDefinition.create(
                "age-index", "age", org.bson.BsonType.INT32, com.kronotop.bucket.index.SortOrder.ASCENDING
        );
        
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "age", 18, 65, true, false, mockIndex
        );

        assertFalse(rule.canApply(rangeScan));
    }

    @Test
    void testCanApplyWithAndContainingNullIndexRangeScan() {
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "age", 18, 65, true, false, null
        );
        PhysicalFilter filter = new PhysicalFilter(2, "name", Operator.EQ, "John");
        PhysicalAnd and = new PhysicalAnd(3, List.of(rangeScan, filter));

        assertTrue(rule.canApply(and));
    }

    @Test
    void testCanApplyWithAndNotContainingNullIndexRangeScan() {
        PhysicalFilter filter1 = new PhysicalFilter(1, "name", Operator.EQ, "John");
        PhysicalFilter filter2 = new PhysicalFilter(2, "age", Operator.GT, 18);
        PhysicalAnd and = new PhysicalAnd(3, List.of(filter1, filter2));

        assertFalse(rule.canApply(and));
    }

    @Test
    void testRulePriority() {
        assertEquals(80, rule.getPriority());
    }

    @Test
    void testRuleName() {
        assertEquals("RangeScanFallback", rule.getName());
    }

    @Test
    void testIllegalStateForNoBounds() {
        // This should not happen in practice, but test error handling
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "field", null, null, false, false, null
        );

        assertThrows(IllegalStateException.class, () -> {
            rule.apply(rangeScan, metadata, context);
        });
    }
}