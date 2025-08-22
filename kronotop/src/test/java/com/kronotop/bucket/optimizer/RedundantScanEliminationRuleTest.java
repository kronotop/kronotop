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
import com.kronotop.bucket.planner.physical.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for RedundantScanEliminationRule.
 * Tests all scenarios where redundant scans should be eliminated.
 */
class RedundantScanEliminationRuleTest extends BaseOptimizerTest {

    @Nested
    @DisplayName("AND Operation Redundancy Elimination")
    class AndRedundancyTests {

        @Test
        @DisplayName("Should eliminate duplicate filters in AND operation")
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
        @DisplayName("Should eliminate duplicate index scans in AND operation")
        void shouldEliminateDuplicateIndexScansInAnd() {
            // Create: AND(IndexScan(age=25), IndexScan(age=25))
            PhysicalFilter filter = createFilter("age", Operator.EQ, 25);
            PhysicalIndexScan scan1 = createIndexScan(filter);
            PhysicalIndexScan scan2 = createIndexScan(filter);
            PhysicalAnd originalPlan = createAnd(scan1, scan2);

            PhysicalNode optimized = optimize(originalPlan);

            // Should be reduced to single index scan
            assertInstanceOf(PhysicalIndexScan.class, optimized);
            PhysicalIndexScan result = (PhysicalIndexScan) optimized;
            PhysicalFilter resultFilter = (PhysicalFilter) result.node();
            assertEquals("age", resultFilter.selector());
            assertEquals(Operator.EQ, resultFilter.op());
            assertEquals(25, resultFilter.operand());
        }

        @Test
        @DisplayName("Should eliminate duplicate full scans in AND operation")
        void shouldEliminateDuplicateFullScansInAnd() {
            // Create: AND(FullScan(status="active"), FullScan(status="active"))
            PhysicalFilter filter1 = createFilter("status", Operator.EQ, "active");
            PhysicalFilter filter2 = createFilter("status", Operator.EQ, "active");
            PhysicalFullScan scan1 = new PhysicalFullScan(1, filter1);
            PhysicalFullScan scan2 = new PhysicalFullScan(2, filter2);
            PhysicalAnd originalPlan = createAnd(scan1, scan2);

            PhysicalNode optimized = optimize(originalPlan);

            // Should be reduced to single full scan
            assertInstanceOf(PhysicalFullScan.class, optimized);
            PhysicalFullScan result = (PhysicalFullScan) optimized;
            PhysicalFilter resultFilter = (PhysicalFilter) result.node();
            assertEquals("status", resultFilter.selector());
            assertEquals(Operator.EQ, resultFilter.op());
            assertEquals("active", resultFilter.operand());
        }

        @Test
        @DisplayName("Should preserve unique conditions in AND operation")
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
        @DisplayName("Should eliminate some duplicates while preserving unique conditions")
        void shouldEliminateSomeDuplicatesWhilePreservingUnique() {
            // Create: AND(name="john", age=25, name="john", status="active")
            PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
            PhysicalFilter filter2 = createFilter("age", Operator.EQ, 25);
            PhysicalFilter filter3 = createFilter("name", Operator.EQ, "john"); // Duplicate
            PhysicalFilter filter4 = createFilter("status", Operator.EQ, "active");
            PhysicalAnd originalPlan = createAnd(filter1, filter2, filter3, filter4);

            PhysicalNode optimized = optimize(originalPlan);

            // Should be reduced to AND with 3 unique children
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd result = (PhysicalAnd) optimized;
            assertEquals(3, result.children().size());
        }
    }

    @Nested
    @DisplayName("OR Operation Redundancy Elimination")
    class OrRedundancyTests {

        @Test
        @DisplayName("Should eliminate duplicate filters in OR operation")
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
        @DisplayName("Should eliminate duplicate index scans in OR operation")
        void shouldEliminateDuplicateIndexScansInOr() {
            // Create: OR(IndexScan(status="active"), IndexScan(status="active"))
            PhysicalFilter filter = createFilter("status", Operator.EQ, "active");
            PhysicalIndexScan scan1 = createIndexScan(filter);
            PhysicalIndexScan scan2 = createIndexScan(filter);
            PhysicalOr originalPlan = createOr(scan1, scan2);

            PhysicalNode optimized = optimize(originalPlan);

            // Should be reduced to single index scan
            assertInstanceOf(PhysicalIndexScan.class, optimized);
            PhysicalIndexScan result = (PhysicalIndexScan) optimized;
            PhysicalFilter resultFilter = (PhysicalFilter) result.node();
            assertEquals("status", resultFilter.selector());
            assertEquals(Operator.EQ, resultFilter.op());
            assertEquals("active", resultFilter.operand());
        }

        @Test
        @DisplayName("Should preserve unique conditions in OR operation")
        void shouldPreserveUniqueConditionsInOr() {
            // Create: OR(name="john", name="jane", name="bob")
            PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
            PhysicalFilter filter2 = createFilter("name", Operator.EQ, "jane");
            PhysicalFilter filter3 = createFilter("name", Operator.EQ, "bob");
            PhysicalOr originalPlan = createOr(filter1, filter2, filter3);

            PhysicalNode optimized = optimize(originalPlan);

            // Should remain as OR with 3 children
            assertInstanceOf(PhysicalOr.class, optimized);
            PhysicalOr result = (PhysicalOr) optimized;
            assertEquals(3, result.children().size());
        }
    }

    @Nested
    @DisplayName("Nested Structure Redundancy Elimination")
    class NestedRedundancyTests {

        @Test
        @DisplayName("Should eliminate redundancy in nested AND/OR structures")
        void shouldEliminateRedundancyInNestedStructures() {
            // Create: AND(OR(name="john", name="john"), age=25)
            PhysicalFilter nameFilter1 = createFilter("name", Operator.EQ, "john");
            PhysicalFilter nameFilter2 = createFilter("name", Operator.EQ, "john");
            PhysicalOr nestedOr = createOr(nameFilter1, nameFilter2);
            PhysicalFilter ageFilter = createFilter("age", Operator.EQ, 25);
            PhysicalAnd originalPlan = createAnd(nestedOr, ageFilter);

            PhysicalNode optimized = optimize(originalPlan);

            // Should be optimized to: AND(name="john", age=25)
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd result = (PhysicalAnd) optimized;
            assertEquals(2, result.children().size());

            // First child should be the simplified name filter
            PhysicalNode firstChild = result.children().get(0);
            assertInstanceOf(PhysicalFilter.class, firstChild);
            PhysicalFilter nameResult = (PhysicalFilter) firstChild;
            assertEquals("name", nameResult.selector());
            assertEquals("john", nameResult.operand());
        }

        @Test
        @DisplayName("Should handle deeply nested redundant structures")
        void shouldHandleDeeplyNestedRedundantStructures() {
            // Create: AND(OR(name="john", name="john"), OR(age=25, age=25))
            PhysicalFilter nameFilter1 = createFilter("name", Operator.EQ, "john");
            PhysicalFilter nameFilter2 = createFilter("name", Operator.EQ, "john");
            PhysicalOr nameOr = createOr(nameFilter1, nameFilter2);

            PhysicalFilter ageFilter1 = createFilter("age", Operator.EQ, 25);
            PhysicalFilter ageFilter2 = createFilter("age", Operator.EQ, 25);
            PhysicalOr ageOr = createOr(ageFilter1, ageFilter2);

            PhysicalAnd originalPlan = createAnd(nameOr, ageOr);

            PhysicalNode optimized = optimize(originalPlan);

            // Should be optimized to: AND(name="john", age=25)
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd result = (PhysicalAnd) optimized;
            assertEquals(2, result.children().size());

            // Both children should be simplified filters
            assertTrue(result.children().stream().allMatch(child -> child instanceof PhysicalFilter));
        }

        @Test
        @DisplayName("Should eliminate redundancy with NOT operations")
        void shouldEliminateRedundancyWithNotOperations() {
            // Create: NOT(AND(name="john", name="john"))
            PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
            PhysicalFilter filter2 = createFilter("name", Operator.EQ, "john");
            PhysicalAnd innerAnd = createAnd(filter1, filter2);
            PhysicalNot originalPlan = new PhysicalNot(1, innerAnd);

            PhysicalNode optimized = optimize(originalPlan);

            // Should be optimized to: NOT(name="john")
            assertInstanceOf(PhysicalNot.class, optimized);
            PhysicalNot result = (PhysicalNot) optimized;
            assertInstanceOf(PhysicalFilter.class, result.child());
            PhysicalFilter childFilter = (PhysicalFilter) result.child();
            assertEquals("name", childFilter.selector());
            assertEquals("john", childFilter.operand());
        }
    }

    @Nested
    @DisplayName("Edge Cases and No-Op Scenarios")
    class EdgeCaseTests {

        @Test
        @DisplayName("Should not modify plans with no redundancy")
        void shouldNotModifyPlansWithNoRedundancy() {
            // Create: AND(name="john", age=25)
            PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
            PhysicalFilter filter2 = createFilter("age", Operator.EQ, 25);
            PhysicalAnd originalPlan = createAnd(filter1, filter2);

            PhysicalNode optimized = optimize(originalPlan);

            // Should remain unchanged (PhysicalNode.equals() ignores IDs)
            assertEquals(originalPlan, optimized);
        }

        @Test
        @DisplayName("Should handle single child gracefully")
        void shouldHandleSingleChildGracefully() {
            // Create: AND(name="john") - should not happen in practice but test anyway
            PhysicalFilter filter = createFilter("name", Operator.EQ, "john");
            PhysicalAnd originalPlan = createAnd(filter);

            PhysicalNode optimized = optimize(originalPlan);

            // Should be simplified to just the filter
            assertInstanceOf(PhysicalFilter.class, optimized);
            assertEquals(filter, optimized);
        }

        @Test
        @DisplayName("Should handle leaf nodes without modification")
        void shouldHandleLeafNodesWithoutModification() {
            PhysicalFilter filter = createFilter("name", Operator.EQ, "john");

            PhysicalNode optimized = optimize(filter);

            // Should remain unchanged (PhysicalNode.equals() ignores IDs)
            assertEquals(filter, optimized);
        }

        @Test
        @DisplayName("Should handle complex mixed redundancy scenarios")
        void shouldHandleComplexMixedRedundancyScenarios() {
            // Create: AND(name="john", OR(age=25, age=25), name="john", status="active")
            PhysicalFilter nameFilter1 = createFilter("name", Operator.EQ, "john");
            PhysicalFilter ageFilter1 = createFilter("age", Operator.EQ, 25);
            PhysicalFilter ageFilter2 = createFilter("age", Operator.EQ, 25);
            PhysicalOr ageOr = createOr(ageFilter1, ageFilter2);
            PhysicalFilter nameFilter2 = createFilter("name", Operator.EQ, "john"); // Duplicate
            PhysicalFilter statusFilter = createFilter("status", Operator.EQ, "active");

            PhysicalAnd originalPlan = createAnd(nameFilter1, ageOr, nameFilter2, statusFilter);

            PhysicalNode optimized = optimize(originalPlan);

            // Should be optimized to: AND(name="john", age=25, status="active")
            assertInstanceOf(PhysicalAnd.class, optimized);
            PhysicalAnd result = (PhysicalAnd) optimized;
            assertEquals(3, result.children().size());

            // All children should be filters (no nested OR should remain)
            assertTrue(result.children().stream().allMatch(child -> child instanceof PhysicalFilter));
        }
    }
}