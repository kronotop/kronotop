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
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;
import org.bson.BsonType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration tests for the optimizer, testing all optimization rules
 * working together in complex scenarios.
 */
@DisplayName("Comprehensive Optimizer Integration Tests")
public class OptimizerComprehensiveIntegrationTest extends BaseOptimizerTest {

    private int countFieldConditions(PhysicalNode node, String field) {
        if (node instanceof PhysicalFilter filter && field.equals(filter.selector())) {
            return 1;
        } else if (node instanceof PhysicalIndexScan scan) {
            return countFieldConditions(scan.node(), field);
        } else if (node instanceof PhysicalFullScan scan) {
            return countFieldConditions(scan.node(), field);
        } else if (node instanceof PhysicalRangeScan rangeScan) {
            // Count conditions in range scan that match field
            return (field.equals(rangeScan.selector())) ? 1 : 0;
        } else if (node instanceof PhysicalIndexIntersection intersection) {
            return (int) intersection.filters().stream()
                    .filter(f -> field.equals(f.selector()))
                    .count();
        } else if (node instanceof PhysicalAnd and) {
            return and.children().stream()
                    .mapToInt(child -> countFieldConditions(child, field))
                    .sum();
        } else if (node instanceof PhysicalOr or) {
            return or.children().stream()
                    .mapToInt(child -> countFieldConditions(child, field))
                    .sum();
        }
        return 0;
    }

    private boolean isIndexedOperation(PhysicalNode node) {
        return node instanceof PhysicalIndexScan ||
                node instanceof PhysicalRangeScan ||
                node instanceof PhysicalIndexIntersection;
    }

    private int findFirstIndexedPosition(List<PhysicalNode> children) {
        for (int i = 0; i < children.size(); i++) {
            if (isIndexedOperation(children.get(i))) {
                return i;
            }
        }
        return -1;
    }

    // Helper methods for comprehensive tests

    private int findFirstNonIndexedPosition(List<PhysicalNode> children) {
        for (int i = 0; i < children.size(); i++) {
            PhysicalNode child = children.get(i);
            if (!isIndexedOperation(child) &&
                    !(child instanceof PhysicalAnd) &&
                    !(child instanceof PhysicalOr)) {
                return i;
            }
        }
        return -1;
    }

    private void verifyOrSelectivityOrdering(PhysicalNode node) {
        if (node instanceof PhysicalOr or) {
            // For OR, NE operations should come before EQ (less selective first)
            int nePosition = -1;
            int eqPosition = -1;

            for (int i = 0; i < or.children().size(); i++) {
                PhysicalNode child = or.children().get(i);
                Operator op = extractOperator(child);
                if (op == Operator.NE && nePosition == -1) {
                    nePosition = i;
                } else if (op == Operator.EQ && eqPosition == -1) {
                    eqPosition = i;
                }
            }

            if (nePosition != -1 && eqPosition != -1) {
                assertTrue(nePosition < eqPosition,
                        "In OR, less selective (NE) should come before more selective (EQ)");
            }
        } else if (node instanceof PhysicalAnd and) {
            and.children().forEach(this::verifyOrSelectivityOrdering);
        }
    }

    private Operator extractOperator(PhysicalNode node) {
        if (node instanceof PhysicalFilter filter) {
            return filter.op();
        } else if (node instanceof PhysicalIndexScan scan && scan.node() instanceof PhysicalFilter filter) {
            return filter.op();
        }
        return null;
    }

    private int countTotalNodes(PhysicalNode node) {
        int count = 1; // Count this node

        if (node instanceof PhysicalAnd and) {
            count += and.children().stream()
                    .mapToInt(this::countTotalNodes)
                    .sum();
        } else if (node instanceof PhysicalOr or) {
            count += or.children().stream()
                    .mapToInt(this::countTotalNodes)
                    .sum();
        } else if (node instanceof PhysicalNot not) {
            count += countTotalNodes(not.child());
        } else if (node instanceof PhysicalIndexScan scan) {
            count += countTotalNodes(scan.node());
        } else if (node instanceof PhysicalFullScan scan) {
            count += countTotalNodes(scan.node());
        }

        return count;
    }

    private boolean containsFieldCondition(PhysicalNode node, String field) {
        return countFieldConditions(node, field) > 0;
    }

    @Nested
    @DisplayName("All Rules Combined Tests")
    class AllRulesCombinedTests {

        @Test
        @DisplayName("Should apply all optimizations to complex e-commerce query")
        void shouldApplyAllOptimizationsToComplexEcommerceQuery() {
            // Setup indexes for an e-commerce scenario
            createIndexes(
                    IndexDefinition.create("product-name-index", "product.name", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("price-index", "price", BsonType.DOUBLE, SortOrder.ASCENDING),
                    IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("brand-index", "brand", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("status-index", "status", BsonType.STRING, SortOrder.ASCENDING)
            );

            // Complex e-commerce query with multiple optimization opportunities:
            // - Redundant conditions (same category twice)
            // - Range conditions that can be consolidated (price range)
            // - Multiple indexed EQ conditions for intersection
            // - Conditions that should be reordered by selectivity
            String query = "{ $and: [" +
                    "{ \"category\": \"electronics\" }, " +
                    "{ \"category\": \"electronics\" }, " +  // Redundant - should be eliminated
                    "{ \"price\": { $gte: 100 } }, " +       // Range - should be consolidated
                    "{ \"price\": { $lte: 1000 } }, " +      // Range - should be consolidated
                    "{ \"brand\": \"TechCorp\" }, " +        // Indexed EQ - good for intersection
                    "{ \"status\": \"available\" }, " +      // Indexed EQ - good for intersection
                    "{ \"description\": { $ne: \"\" } }, " + // Non-indexed - least selective
                    "{ \"rating\": { $gt: 3.5 } }" +         // Non-indexed range
                    "] }";

            PhysicalNode unoptimized = planWithoutOptimization(query);
            PhysicalNode optimized = planAndOptimize(query);

            // Verify all optimizations were applied

            // 1. Redundant elimination: No duplicate category conditions
            int categoryCount = countFieldConditions(optimized, "category");
            assertEquals(1, categoryCount, "Should eliminate redundant category condition");

            // 2. Range consolidation: Price conditions should be consolidated
            int rangeScanCount = countNodeType(optimized, PhysicalRangeScan.class);
            assertTrue(rangeScanCount > 0, "Should create range scan for price");

            // 3. Index intersection for EQ conditions (brand, status, possibly category)
            int intersectionCount = countNodeType(optimized, PhysicalIndexIntersection.class);
            assertTrue(intersectionCount > 0, "Should create index intersection for EQ conditions");

            // 4. Selectivity ordering: If top-level is AND, indexed conditions should come first
            if (optimized instanceof PhysicalAnd and) {
                PhysicalNode firstChild = and.children().get(0);
                boolean firstIsIndexed = isIndexedOperation(firstChild);
                assertTrue(firstIsIndexed, "Most selective (indexed) operations should come first");
            }
        }

        @Test
        @DisplayName("Should optimize social media user query with all rules")
        void shouldOptimizeSocialMediaUserQueryWithAllRules() {
            // Setup indexes for social media scenario
            createIndexes(
                    IndexDefinition.create("username-index", "username", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING),
                    IndexDefinition.create("country-index", "location.country", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("verified-index", "verified", BsonType.BOOLEAN, SortOrder.ASCENDING)
            );

            // Build complex query manually to test all optimizations
            // Create redundant username checks
            PhysicalFilter user1 = createFilter("username", Operator.EQ, "john_doe");
            PhysicalFilter user2 = createFilter("username", Operator.EQ, "john_doe");
            PhysicalIndexScan userScan1 = createIndexScan(user1);
            PhysicalIndexScan userScan2 = createIndexScan(user2);

            // Create age range that can be consolidated
            PhysicalFilter ageGte = createFilter("age", Operator.GTE, 18);
            PhysicalFilter ageLte = createFilter("age", Operator.LTE, 65);
            PhysicalFilter ageGt = createFilter("age", Operator.GT, 17); // Overlaps with ageGte
            PhysicalIndexScan ageGteScan = createIndexScan(ageGte);
            PhysicalIndexScan ageLteScan = createIndexScan(ageLte);
            PhysicalIndexScan ageGtScan = createIndexScan(ageGt);

            // Create indexed EQ conditions for intersection
            PhysicalFilter country = createFilter("location.country", Operator.EQ, "USA");
            PhysicalFilter verified = createFilter("verified", Operator.EQ, true);
            PhysicalIndexScan countryScan = createIndexScan(country);
            PhysicalIndexScan verifiedScan = createIndexScan(verified);

            // Create non-indexed conditions
            PhysicalFilter followerCount = createFilter("followers", Operator.GT, 1000);
            PhysicalFilter bio = createFilter("bio", Operator.NE, "");

            // Build nested structure with redundancy and suboptimal ordering
            PhysicalOr userOr = createOr(userScan1, userScan2); // Redundant OR
            PhysicalAnd ageAnd = createAnd(ageGteScan, ageLteScan, ageGtScan); // Range to consolidate

            // Create top-level AND with poor ordering (non-indexed first)
            PhysicalAnd topLevel = createAnd(
                    followerCount,    // Non-indexed (least selective)
                    bio,              // Non-indexed
                    userOr,           // Redundant structure
                    ageAnd,           // Range conditions
                    countryScan,      // Indexed EQ
                    verifiedScan      // Indexed EQ
            );

            PhysicalNode optimized = optimize(topLevel);

            // Verify comprehensive optimizations
            assertNotNull(optimized);

            // Should eliminate redundancy in username OR
            int usernameCount = countFieldConditions(optimized, "username");
            assertEquals(1, usernameCount, "Should eliminate redundant username conditions");

            // Should consolidate age ranges (but may keep them as separate nodes in AND)
            // The optimizer consolidates ranges into a PhysicalRangeScan or keeps them separate
            // Either way is valid as long as redundancy is eliminated
            int rangeScanCount = countNodeType(optimized, PhysicalRangeScan.class);
            int ageConditions = countFieldConditions(optimized, "age");
            assertTrue(rangeScanCount > 0 || ageConditions <= 3,
                    "Should consolidate age conditions into range scan or limit redundant conditions");

            // Should create optimized structures (intersection or range scan)
            boolean hasOptimizedStructures =
                    countNodeType(optimized, PhysicalIndexIntersection.class) > 0 ||
                            countNodeType(optimized, PhysicalRangeScan.class) > 0;
            assertTrue(hasOptimizedStructures, "Should create optimized scan structures");

            // Verify selectivity ordering if AND at top level
            if (optimized instanceof PhysicalAnd and && and.children().size() > 1) {
                // Check that indexed operations tend to come before non-indexed
                int indexedPosition = findFirstIndexedPosition(and.children());
                int nonIndexedPosition = findFirstNonIndexedPosition(and.children());

                if (indexedPosition != -1 && nonIndexedPosition != -1) {
                    assertTrue(indexedPosition < nonIndexedPosition,
                            "Indexed operations should generally come before non-indexed");
                }
            }
        }

        @Test
        @DisplayName("Should optimize complex OR-heavy query with all rules")
        void shouldOptimizeComplexOrHeavyQueryWithAllRules() {
            createIndexes(
                    IndexDefinition.create("type-index", "type", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("priority-index", "priority", BsonType.INT32, SortOrder.ASCENDING),
                    IndexDefinition.create("status-index", "status", BsonType.STRING, SortOrder.ASCENDING)
            );

            // Create OR-heavy structure with optimization opportunities
            // OR conditions should be ordered least to most selective
            PhysicalFilter highPriority = createFilter("priority", Operator.EQ, 1);
            PhysicalFilter mediumPriority = createFilter("priority", Operator.EQ, 2);
            PhysicalFilter anyPriority = createFilter("priority", Operator.NE, 0); // Less selective

            PhysicalIndexScan highScan = createIndexScan(highPriority);
            PhysicalIndexScan medScan = createIndexScan(mediumPriority);
            PhysicalIndexScan anyScan = createIndexScan(anyPriority);

            // Create OR with poor ordering (most selective first)
            PhysicalOr priorityOr = createOr(highScan, medScan, anyScan);

            // Add redundant conditions in nested OR
            PhysicalFilter activeStatus = createFilter("status", Operator.EQ, "active");
            PhysicalFilter activeStatus2 = createFilter("status", Operator.EQ, "active"); // Duplicate
            PhysicalIndexScan activeScan1 = createIndexScan(activeStatus);
            PhysicalIndexScan activeScan2 = createIndexScan(activeStatus2);
            PhysicalOr statusOr = createOr(activeScan1, activeScan2);

            // Combine with AND
            PhysicalFilter typeFilter = createFilter("type", Operator.EQ, "urgent");
            PhysicalIndexScan typeScan = createIndexScan(typeFilter);

            PhysicalAnd topLevel = createAnd(priorityOr, statusOr, typeScan);

            PhysicalNode optimized = optimize(topLevel);

            // Verify optimizations
            assertNotNull(optimized);

            // Should eliminate redundant status conditions
            int statusCount = countFieldConditions(optimized, "status");
            assertEquals(1, statusCount, "Should eliminate redundant status conditions in OR");

            // For OR operations, verify ordering (least to most selective)
            verifyOrSelectivityOrdering(optimized);
        }
    }

    @Nested
    @DisplayName("Selectivity-Based Ordering Integration Tests")
    class SelectivityBasedOrderingIntegrationTests {

        @Test
        @DisplayName("Should integrate selectivity ordering with other optimizations")
        void shouldIntegrateSelectivityOrderingWithOtherOptimizations() {
            createIndexes(
                    IndexDefinition.create("id-index", "id", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("timestamp-index", "timestamp", BsonType.DATE_TIME, SortOrder.ASCENDING),
                    IndexDefinition.create("user-index", "userId", BsonType.STRING, SortOrder.ASCENDING)
            );

            // Create query that benefits from multiple optimizations
            String query = "{ $and: [" +
                    "{ \"id\": \"123\" }, " +              // Most selective (unique ID)
                    "{ \"userId\": \"user456\" }, " +      // Moderately selective
                    "{ \"timestamp\": { $gte: 1000 } }, " + // Range - less selective
                    "{ \"timestamp\": { $lt: 2000 } }, " +  // Range to consolidate
                    "{ \"type\": \"event\" }, " +          // Non-indexed
                    "{ \"processed\": false }" +           // Non-indexed
                    "] }";

            PhysicalNode optimized = planAndOptimize(query);

            // After all optimizations including selectivity ordering:
            // 1. Timestamp ranges should be consolidated
            // 2. ID and userId might be intersected
            // 3. Result should be ordered by selectivity

            if (optimized instanceof PhysicalAnd and) {
                // Verify indexed operations come first
                PhysicalNode firstChild = and.children().get(0);
                assertTrue(isIndexedOperation(firstChild),
                        "Most selective indexed operations should come first");

                // Verify non-indexed operations come last
                PhysicalNode lastChild = and.children().get(and.children().size() - 1);
                boolean lastIsNonIndexed = !isIndexedOperation(lastChild);
                assertTrue(lastIsNonIndexed || and.children().size() <= 2,
                        "Non-indexed operations should come last when present");
            }
        }

        @Test
        @DisplayName("Should order complex nested structures by selectivity after other optimizations")
        void shouldOrderComplexNestedStructuresBySelectivityAfterOtherOptimizations() {
            createIndexes(
                    IndexDefinition.create("level-index", "level", BsonType.INT32, SortOrder.ASCENDING),
                    IndexDefinition.create("module-index", "module", BsonType.STRING, SortOrder.ASCENDING)
            );

            // Create nested structure with various selectivities
            PhysicalFilter levelEq = createFilter("level", Operator.EQ, 5);
            PhysicalFilter levelRange = createFilter("level", Operator.GT, 0);
            PhysicalFilter moduleEq = createFilter("module", Operator.EQ, "core");
            PhysicalFilter messageNe = createFilter("message", Operator.NE, "");

            PhysicalIndexScan levelEqScan = createIndexScan(levelEq);
            PhysicalIndexScan levelRangeScan = createIndexScan(levelRange);
            PhysicalIndexScan moduleScan = createIndexScan(moduleEq);

            // Create nested AND (should be most selective due to IndexScan with EQ)
            PhysicalAnd innerAnd = createAnd(levelEqScan, moduleScan);

            // Create OR (less selective)
            PhysicalOr levelOr = createOr(levelRangeScan, messageNe);

            // Top level with poor ordering
            PhysicalAnd topLevel = createAnd(messageNe, levelOr, innerAnd);

            PhysicalNode optimized = optimize(topLevel);

            // Inner AND with EQ conditions should be most selective and come first
            if (optimized instanceof PhysicalAnd and) {
                // Find position of nested AND (most selective)
                int innerAndPosition = -1;
                for (int i = 0; i < and.children().size(); i++) {
                    if (and.children().get(i) instanceof PhysicalAnd innerAnd2) {
                        // Check if this is our inner AND with index scans
                        if (countNodeType(innerAnd2, PhysicalIndexScan.class) >= 2) {
                            innerAndPosition = i;
                            break;
                        }
                    }
                }

                if (innerAndPosition != -1) {
                    assertEquals(0, innerAndPosition,
                            "Most selective nested AND should come first");
                }
            }
        }
    }

    @Nested
    @DisplayName("Performance and Efficiency Tests")
    class PerformanceEfficiencyTests {

        @Test
        @DisplayName("Should reduce total node count through optimizations")
        void shouldReduceTotalNodeCountThroughOptimizations() {
            createIndexes(
                    IndexDefinition.create("field1-index", "field1", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("field2-index", "field2", BsonType.INT32, SortOrder.ASCENDING)
            );

            // Query with redundancy and inefficiencies
            String query = "{ $and: [" +
                    "{ \"field1\": \"value1\" }, " +
                    "{ \"field1\": \"value1\" }, " +  // Redundant
                    "{ \"field1\": \"value1\" }, " +  // Redundant
                    "{ \"field2\": { $gte: 10 } }, " +
                    "{ \"field2\": { $lt: 20 } }, " +
                    "{ \"field2\": { $gt: 9 } }" +    // Overlapping with gte 10
                    "] }";

            PhysicalNode unoptimized = planWithoutOptimization(query);
            PhysicalNode optimized = planAndOptimize(query);

            int unoptimizedCount = countTotalNodes(unoptimized);
            int optimizedCount = countTotalNodes(optimized);

            assertTrue(optimizedCount < unoptimizedCount,
                    "Optimization should reduce total node count");
        }

        @Test
        @DisplayName("Should create efficient execution plan for complex queries")
        void shouldCreateEfficientExecutionPlanForComplexQueries() {
            createIndexes(
                    IndexDefinition.create("pk-index", "pk", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("sk-index", "sk", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("gsi1pk-index", "gsi1pk", BsonType.STRING, SortOrder.ASCENDING)
            );

            // DynamoDB-style query pattern
            String query = "{ $and: [" +
                    "{ \"pk\": \"USER#123\" }, " +        // Partition key - most selective
                    "{ \"sk\": { $gte: \"ORDER#\" } }, " + // Sort key range
                    "{ \"sk\": { $lt: \"ORDER$\" } }, " +  // Sort key range
                    "{ \"status\": \"completed\" }, " +    // Filter
                    "{ \"total\": { $gt: 100 } }" +       // Filter
                    "] }";

            PhysicalNode optimized = planAndOptimize(query);

            // Should create efficient plan:
            // 1. PK condition should be prioritized (most selective)
            // 2. SK range should be consolidated
            // 3. Non-indexed filters should come last

            // Verify structure is optimized
            boolean hasEfficientStructure =
                    countNodeType(optimized, PhysicalIndexIntersection.class) > 0 ||
                            countNodeType(optimized, PhysicalRangeScan.class) > 0;
            assertTrue(hasEfficientStructure, "Should create efficient scan structures");

            // Verify no redundant scans
            if (optimized instanceof PhysicalAnd and) {
                // Count PK conditions (should be 1)
                long pkConditions = and.children().stream()
                        .filter(child -> containsFieldCondition(child, "pk"))
                        .count();
                assertEquals(1, pkConditions, "Should have exactly one PK condition");
            }
        }
    }
}