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
    void shouldConvertRangeScanWithBothBounds() {
        // Create a range scan with both bounds but no index (null)
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "age", 18, 65, true, false, null
        );

        // Apply the rule
        PhysicalNode result = rule.apply(rangeScan, metadata, context);

        assertInstanceOf(PhysicalAnd.class, result);
        PhysicalAnd and = (PhysicalAnd) result;

        assertEquals(2, and.children().size());

        // First child: PhysicalFullScan with age >= 18 filter
        PhysicalFullScan fullScan1 = (PhysicalFullScan) and.children().get(0);
        PhysicalFilter filter1 = (PhysicalFilter) fullScan1.node();
        assertEquals("age", filter1.selector());
        assertEquals(Operator.GTE, filter1.op());
        assertEquals(18, filter1.operand());

        // Second child: PhysicalFullScan with age < 65 filter
        PhysicalFullScan fullScan2 = (PhysicalFullScan) and.children().get(1);
        PhysicalFilter filter2 = (PhysicalFilter) fullScan2.node();
        assertEquals("age", filter2.selector());
        assertEquals(Operator.LT, filter2.op());
        assertEquals(65, filter2.operand());
    }

    @Test
    void shouldConvertRangeScanWithLowerBoundOnly() {
        // Create a range scan with only lower bound
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "score", 50, null, false, false, null
        );

        PhysicalNode result = rule.apply(rangeScan, metadata, context);

        // Should be converted to PhysicalFullScan with single PhysicalFilter
        assertInstanceOf(PhysicalFullScan.class, result);
        PhysicalFullScan fullScan = (PhysicalFullScan) result;

        assertInstanceOf(PhysicalFilter.class, fullScan.node());
        PhysicalFilter filter = (PhysicalFilter) fullScan.node();

        assertEquals("score", filter.selector());
        assertEquals(Operator.GT, filter.op());
        assertEquals(50, filter.operand());
    }

    @Test
    void shouldConvertRangeScanWithUpperBoundOnly() {
        // Create a range scan with only upper bound
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "price", null, 100.0, true, true, null
        );

        PhysicalNode result = rule.apply(rangeScan, metadata, context);

        // Should be converted to PhysicalFullScan with single PhysicalFilter
        assertInstanceOf(PhysicalFullScan.class, result);
        PhysicalFullScan fullScan = (PhysicalFullScan) result;

        assertInstanceOf(PhysicalFilter.class, fullScan.node());
        PhysicalFilter filter = (PhysicalFilter) fullScan.node();

        assertEquals("price", filter.selector());
        assertEquals(Operator.LTE, filter.op());
        assertEquals(100.0, filter.operand());
    }

    @Test
    void shouldConvertRangeScanWithInclusiveBounds() {
        // Test inclusive bounds conversion
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "rating", 1, 5, true, true, null
        );

        PhysicalNode result = rule.apply(rangeScan, metadata, context);

        // Result should be PhysicalAnd with two PhysicalFullScan children
        assertInstanceOf(PhysicalAnd.class, result);
        PhysicalAnd and = (PhysicalAnd) result;
        assertEquals(2, and.children().size());

        // First child: PhysicalFullScan with rating >= 1 filter
        PhysicalFullScan fullScan1 = (PhysicalFullScan) and.children().get(0);
        PhysicalFilter filter1 = (PhysicalFilter) fullScan1.node();
        assertEquals("rating", filter1.selector());
        assertEquals(Operator.GTE, filter1.op());
        assertEquals(1, filter1.operand());

        // Second child: PhysicalFullScan with rating <= 5 filter
        PhysicalFullScan fullScan2 = (PhysicalFullScan) and.children().get(1);
        PhysicalFilter filter2 = (PhysicalFilter) fullScan2.node();
        assertEquals("rating", filter2.selector());
        assertEquals(Operator.LTE, filter2.op());
        assertEquals(5, filter2.operand());
    }

    @Test
    void shouldNotConvertRangeScanWithValidIndex() {
        // Create a mock index for this test
        IndexDefinition mockIndex = IndexDefinition.create(
                "age-index", "age", org.bson.BsonType.INT32
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
    void shouldApplyFallbackToPhysicalAndWithRangeScan() {
        // Create an AND with a range scan that needs fallback
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "age", 18, 65, true, false, null
        );
        PhysicalFilter regularFilter = new PhysicalFilter(2, "name", Operator.EQ, "John");
        PhysicalAnd and = new PhysicalAnd(3, List.of(rangeScan, regularFilter));

        PhysicalNode result = rule.apply(and, metadata, context);

        // Should convert to nested structure: PhysicalAnd[PhysicalAnd[2 PhysicalFullScans], PhysicalFilter]
        assertInstanceOf(PhysicalAnd.class, result);
        PhysicalAnd resultAnd = (PhysicalAnd) result;
        assertEquals(2, resultAnd.children().size());

        // First child: PhysicalAnd containing two PhysicalFullScan children (from range scan conversion)
        PhysicalAnd nestedAnd = (PhysicalAnd) resultAnd.children().get(0);
        assertEquals(2, nestedAnd.children().size());

        // First nested child: PhysicalFullScan with age >= 18 filter
        PhysicalFullScan fullScan1 = (PhysicalFullScan) nestedAnd.children().get(0);
        PhysicalFilter filter1 = (PhysicalFilter) fullScan1.node();
        assertEquals("age", filter1.selector());
        assertEquals(Operator.GTE, filter1.op());
        assertEquals(18, filter1.operand());

        // Second nested child: PhysicalFullScan with age < 65 filter
        PhysicalFullScan fullScan2 = (PhysicalFullScan) nestedAnd.children().get(1);
        PhysicalFilter filter2 = (PhysicalFilter) fullScan2.node();
        assertEquals("age", filter2.selector());
        assertEquals(Operator.LT, filter2.op());
        assertEquals(65, filter2.operand());

        // Second child: Original PhysicalFilter for name
        PhysicalFilter nameFilter = (PhysicalFilter) resultAnd.children().get(1);
        assertEquals("name", nameFilter.selector());
        assertEquals(Operator.EQ, nameFilter.op());
        assertEquals("John", nameFilter.operand());
    }

    @Test
    void shouldApplyFallbackToNestedStructureWithRangeScan() {
        // Test nested structure with range scan that needs fallback
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "score", 50, null, true, false, null
        );
        PhysicalNot not = new PhysicalNot(2, rangeScan);

        PhysicalNode result = rule.apply(not, metadata, context);

        // Should convert the nested range scan
        assertInstanceOf(PhysicalNot.class, result);
        PhysicalNot resultNot = (PhysicalNot) result;

        assertInstanceOf(PhysicalFullScan.class, resultNot.child());
    }

    @Test
    void shouldReturnTrueForCanApplyWithRangeScanNullIndex() {
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "age", 18, 65, true, false, null
        );

        assertTrue(rule.canApply(rangeScan));
    }

    @Test
    void shouldReturnFalseForCanApplyWithRangeScanValidIndex() {
        // Create a mock index for this test
        IndexDefinition mockIndex = IndexDefinition.create(
                "age-index", "age", org.bson.BsonType.INT32
        );

        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "age", 18, 65, true, false, mockIndex
        );

        assertFalse(rule.canApply(rangeScan));
    }

    @Test
    void shouldReturnTrueForCanApplyWithAndContainingNullIndexRangeScan() {
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "age", 18, 65, true, false, null
        );
        PhysicalFilter filter = new PhysicalFilter(2, "name", Operator.EQ, "John");
        PhysicalAnd and = new PhysicalAnd(3, List.of(rangeScan, filter));

        assertTrue(rule.canApply(and));
    }

    @Test
    void shouldReturnFalseForCanApplyWithAndNotContainingNullIndexRangeScan() {
        PhysicalFilter filter1 = new PhysicalFilter(1, "name", Operator.EQ, "John");
        PhysicalFilter filter2 = new PhysicalFilter(2, "age", Operator.GT, 18);
        PhysicalAnd and = new PhysicalAnd(3, List.of(filter1, filter2));

        assertFalse(rule.canApply(and));
    }

    @Test
    void shouldReturnCorrectRulePriority() {
        assertEquals(80, rule.getPriority());
    }

    @Test
    void shouldReturnCorrectRuleName() {
        assertEquals("RangeScanFallback", rule.getName());
    }

    @Test
    void shouldThrowIllegalStateForNoBounds() {
        // This should not happen in practice, but test error handling
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(
                1, "field", null, null, false, false, null
        );

        assertThrows(IllegalStateException.class, () -> {
            rule.apply(rangeScan, metadata, context);
        });
    }
}