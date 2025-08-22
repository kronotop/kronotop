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

import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.PhysicalAnd;
import com.kronotop.bucket.planner.physical.PhysicalFilter;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.bucket.planner.physical.PhysicalOr;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Simple flat tests for RedundantScanEliminationRule without nested classes.
 */
class RedundantScanEliminationTest extends BaseOptimizerTest {

    @Test
    void shouldEliminateDuplicateFiltersInAnd() {
        // Create: AND(name="john", name="john")
        PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
        PhysicalFilter filter2 = createFilter("name", Operator.EQ, "john");
        PhysicalAnd originalPlan = createAnd(filter1, filter2);

        PhysicalNode optimized = optimize(originalPlan);

        // Should be reduced to single filter
        assertInstanceOf(PhysicalFilter.class, optimized);
        PhysicalFilter result = (PhysicalFilter) optimized;
        assertEquals("name", result.selector());
        assertEquals(Operator.EQ, result.op());
        assertEquals("john", result.operand());
    }

    @Test
    void shouldEliminateDuplicateFiltersInOr() {
        // Create: OR(name="john", name="john")
        PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
        PhysicalFilter filter2 = createFilter("name", Operator.EQ, "john");
        PhysicalOr originalPlan = createOr(filter1, filter2);

        PhysicalNode optimized = optimize(originalPlan);

        // Should be reduced to single filter
        assertInstanceOf(PhysicalFilter.class, optimized);
        PhysicalFilter result = (PhysicalFilter) optimized;
        assertEquals("name", result.selector());
        assertEquals(Operator.EQ, result.op());
        assertEquals("john", result.operand());
    }

    @Test
    void shouldPreserveUniqueConditionsInAnd() {
        // Create: AND(name="john", age=25, status="active")
        PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
        PhysicalFilter filter2 = createFilter("age", Operator.EQ, 25);
        PhysicalFilter filter3 = createFilter("status", Operator.EQ, "active");
        PhysicalAnd originalPlan = createAnd(filter1, filter2, filter3);

        PhysicalNode optimized = optimize(originalPlan);

        // Should remain as AND with 3 children
        assertInstanceOf(PhysicalAnd.class, optimized);
        PhysicalAnd result = (PhysicalAnd) optimized;
        assertEquals(3, result.children().size());
    }

    @Test
    void shouldNotModifyPlansWithNoRedundancy() {
        // Create: AND(name="john", age=25)
        PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
        PhysicalFilter filter2 = createFilter("age", Operator.EQ, 25);
        PhysicalAnd originalPlan = createAnd(filter1, filter2);

        PhysicalNode optimized = optimize(originalPlan);

        // Should remain unchanged (PhysicalNode.equals() ignores IDs)
        assertEquals(originalPlan, optimized);
    }
}