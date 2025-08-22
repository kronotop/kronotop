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

package com.kronotop.bucket.planner.physical;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.bucket.planner.Operator;
import org.bson.BsonType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for PhysicalPlanner with index scenarios.
 * Tests PhysicalIndexScan usage with all operators and combinations.
 */
class PhysicalPlannerWithIndexTest extends BasePhysicalPlannerTest {
    /**
     * Helper method to create an index
     */
    void createIndex(IndexDefinition definition) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace indexSubspace = IndexUtil.create(tr, metadata.subspace(), definition);
            assertNotNull(indexSubspace);
            tr.commit().join();
        }
        // Refresh the index registry
        metadata = getBucketMetadata(TEST_BUCKET_NAME);
    }

    /**
     * Helper method to create multiple indexes at once
     */
    void createIndexes(IndexDefinition... definitions) {
        for (IndexDefinition definition : definitions) {
            createIndex(definition);
        }
    }

    // ============================================================================
    // Single Index Tests - All Operators
    // ============================================================================

    @Nested
    @DisplayName("Single Index Operator Tests")
    class SingleIndexOperatorTests {

        @Test
        @DisplayName("Numeric index with GTE operator should use PhysicalIndexScan")
        void testWithNumericIndex() {
            createIndex(IndexDefinition.create(
                    "numeric-index", "int32-field", BsonType.INT32, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("{ 'int32-field': { $gte: 20 } }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            assertInstanceOf(PhysicalFilter.class, indexScan.node());

            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("int32-field", filter.selector());
            assertEquals(Operator.GTE, filter.op());
            assertEquals(20, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("String index with EQ operator should use PhysicalIndexScan")
        void testWithStringIndexEQ() {
            createIndex(IndexDefinition.create(
                    "string-index", "name", BsonType.STRING, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("{ \"name\": \"john\" }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            assertInstanceOf(PhysicalFilter.class, indexScan.node());

            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("name", filter.selector());
            assertEquals(Operator.EQ, filter.op());
            assertEquals("john", extractValue(filter.operand()));
        }

        @Test
        @DisplayName("Numeric index with GT operator should use PhysicalIndexScan")
        void testWithNumericIndexGT() {
            createIndex(IndexDefinition.create(
                    "price-index", "price", BsonType.DOUBLE, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("{ \"price\": { \"$gt\": 99.99 } }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("price", filter.selector());
            assertEquals(Operator.GT, filter.op());
            assertEquals(99.99, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("Numeric index with LT operator should use PhysicalIndexScan")
        void testWithNumericIndexLT() {
            createIndex(IndexDefinition.create(
                    "age-index", "age", BsonType.INT32, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("{ \"age\": { \"$lt\": 65 } }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("age", filter.selector());
            assertEquals(Operator.LT, filter.op());
            assertEquals(65, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("Numeric index with LTE operator should use PhysicalIndexScan")
        void testWithNumericIndexLTE() {
            createIndex(IndexDefinition.create(
                    "score-index", "score", BsonType.INT32, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("{ \"score\": { \"$lte\": 100 } }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("score", filter.selector());
            assertEquals(Operator.LTE, filter.op());
            assertEquals(100, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("String index with NE operator should use PhysicalIndexScan")
        void testWithStringIndexNE() {
            createIndex(IndexDefinition.create(
                    "status-index", "status", BsonType.STRING, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("{ \"status\": { \"$ne\": \"deleted\" } }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("status", filter.selector());
            assertEquals(Operator.NE, filter.op());
            assertEquals("deleted", extractValue(filter.operand()));
        }

        @Test
        @DisplayName("String index with IN operator should use PhysicalIndexScan")
        void testWithStringIndexIN() {
            createIndex(IndexDefinition.create(
                    "category-index", "category", BsonType.STRING, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("{ \"category\": { \"$in\": [\"electronics\", \"books\", \"clothing\"] } }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("category", filter.selector());
            assertEquals(Operator.IN, filter.op());

            @SuppressWarnings("unchecked")
            List<Object> operand = (List<Object>) filter.operand();
            assertEquals(Arrays.asList("electronics", "books", "clothing"), extractValue(operand));
        }

        @Test
        @DisplayName("String index with NIN operator should use PhysicalIndexScan")
        void testWithStringIndexNIN() {
            createIndex(IndexDefinition.create(
                    "type-index", "type", BsonType.STRING, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("{ \"type\": { \"$nin\": [\"spam\", \"deleted\"] } }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("type", filter.selector());
            assertEquals(Operator.NIN, filter.op());

            @SuppressWarnings("unchecked")
            List<Object> operand = (List<Object>) filter.operand();
            assertEquals(Arrays.asList("spam", "deleted"), extractValue(operand));
        }

        @Test
        @DisplayName("Array index with ALL operator should use PhysicalIndexScan")
        void testWithArrayIndexALL() {
            createIndex(IndexDefinition.create(
                    "tags-index", "tags", BsonType.STRING, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("{ \"tags\": { \"$all\": [\"urgent\", \"important\"] } }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("tags", filter.selector());
            assertEquals(Operator.ALL, filter.op());

            @SuppressWarnings("unchecked")
            List<Object> operand = (List<Object>) filter.operand();
            assertEquals(Arrays.asList("urgent", "important"), extractValue(operand));
        }

        @Test
        @DisplayName("Array index with SIZE operator should use PhysicalIndexScan")
        void testWithArrayIndexSIZE() {
            createIndex(IndexDefinition.create(
                    "items-index", "items", BsonType.ARRAY, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("{ \"items\": { \"$size\": 3 } }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("items", filter.selector());
            assertEquals(Operator.SIZE, filter.op());
            assertEquals(3, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("Field index with EXISTS operator should use PhysicalIndexScan")
        void testWithFieldIndexEXISTS() {
            createIndex(IndexDefinition.create(
                    "metadata-index", "metadata", BsonType.DOCUMENT, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("{ \"metadata\": { \"$exists\": true } }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("metadata", filter.selector());
            assertEquals(Operator.EXISTS, filter.op());
            assertEquals(true, extractValue(filter.operand()));
        }
    }

    // ============================================================================
    // AND Combinations: Index + Index, Index + FullScan
    // ============================================================================

    @Nested
    @DisplayName("AND Combination Tests")
    class AndCombinationTests {

        @Test
        @DisplayName("AND with two indexed fields should use PhysicalAnd with two PhysicalIndexScans")
        void testAndWithTwoIndexedFields() {
            createIndexes(
                    IndexDefinition.create("age-index", "age", BsonType.INT32, SortOrder.ASCENDING),
                    IndexDefinition.create("status-index", "status", BsonType.STRING, SortOrder.ASCENDING)
            );

            PhysicalNode result = planQuery("""
                    {
                      "$and": [
                        { "age": { "$gte": 18 } },
                        { "status": "active" }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd and = (PhysicalAnd) result;
            assertEquals(2, and.children().size());

            // Both children should be index scans
            assertInstanceOf(PhysicalIndexScan.class, and.children().get(0));
            assertInstanceOf(PhysicalIndexScan.class, and.children().get(1));
        }

        @Test
        @DisplayName("AND with indexed and non-indexed fields should mix PhysicalIndexScan and PhysicalFullScan")
        void testAndWithMixedIndexedFields() {
            createIndex(IndexDefinition.create(
                    "price-index", "price", BsonType.DOUBLE, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("""
                    {
                      "$and": [
                        { "price": { "$gt": 50.0 } },
                        { "description": { "$ne": "outdated" } }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd and = (PhysicalAnd) result;
            assertEquals(2, and.children().size());

            // Should have one index scan and one full scan
            boolean hasIndexScan = false;
            boolean hasFullScan = false;

            for (PhysicalNode child : and.children()) {
                if (child instanceof PhysicalIndexScan) hasIndexScan = true;
                else if (child instanceof PhysicalFullScan) hasFullScan = true;
            }

            assertTrue(hasIndexScan, "Should have PhysicalIndexScan for indexed field");
            assertTrue(hasFullScan, "Should have PhysicalFullScan for non-indexed field");
        }

        @Test
        @DisplayName("AND with multiple conditions on same indexed field should use single PhysicalIndexScan")
        void testAndWithMultipleConditionsOnSameIndexedField() {
            createIndex(IndexDefinition.create(
                    "age-index", "age", BsonType.INT32, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("""
                    {
                      "$and": [
                        { "age": { "$gte": 18 } },
                        { "age": { "$lt": 65 } }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd and = (PhysicalAnd) result;
            assertEquals(2, and.children().size());

            // Both should be index scans on the same field
            assertInstanceOf(PhysicalIndexScan.class, and.children().get(0));
            assertInstanceOf(PhysicalIndexScan.class, and.children().get(1));

            PhysicalIndexScan scan1 = (PhysicalIndexScan) and.children().get(0);
            PhysicalIndexScan scan2 = (PhysicalIndexScan) and.children().get(1);

            PhysicalFilter filter1 = (PhysicalFilter) scan1.node();
            PhysicalFilter filter2 = (PhysicalFilter) scan2.node();

            assertEquals("age", filter1.selector());
            assertEquals("age", filter2.selector());
        }

        @Test
        @DisplayName("Complex AND with nested operations and mixed indexes")
        void testComplexAndWithMixedIndexes() {
            createIndexes(
                    IndexDefinition.create("category-index", "category", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("price-index", "price", BsonType.DOUBLE, SortOrder.ASCENDING)
            );

            PhysicalNode result = planQuery("""
                    {
                      "$and": [
                        { "category": { "$in": ["electronics", "books"] } },
                        { "price": { "$gte": 10.0 } },
                        { "available": true },
                        { "rating": { "$gt": 3.0 } }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd and = (PhysicalAnd) result;
            assertEquals(4, and.children().size());

            // Count index scans vs full scans
            long indexScans = and.children().stream()
                    .filter(child -> child instanceof PhysicalIndexScan)
                    .count();
            long fullScans = and.children().stream()
                    .filter(child -> child instanceof PhysicalFullScan)
                    .count();

            assertEquals(2, indexScans, "Should have 2 index scans for category and price");
            assertEquals(2, fullScans, "Should have 2 full scans for available and rating");
        }
    }

    // ============================================================================
    // OR Combinations: Index + Index, Index + FullScan
    // ============================================================================

    @Nested
    @DisplayName("OR Combination Tests")
    class OrCombinationTests {

        @Test
        @DisplayName("OR with two indexed fields should use PhysicalOr with two PhysicalIndexScans")
        void testOrWithTwoIndexedFields() {
            createIndexes(
                    IndexDefinition.create("name-index", "name", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("email-index", "email", BsonType.STRING, SortOrder.ASCENDING)
            );

            PhysicalNode result = planQuery("""
                    {
                      "$or": [
                        { "name": "john" },
                        { "email": "john@example.com" }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalOr.class, result);
            PhysicalOr or = (PhysicalOr) result;
            assertEquals(2, or.children().size());

            // Both children should be index scans
            assertInstanceOf(PhysicalIndexScan.class, or.children().get(0));
            assertInstanceOf(PhysicalIndexScan.class, or.children().get(1));
        }

        @Test
        @DisplayName("OR with indexed and non-indexed fields should mix PhysicalIndexScan and PhysicalFullScan")
        void testOrWithMixedIndexedFields() {
            createIndex(IndexDefinition.create(
                    "status-index", "status", BsonType.STRING, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("""
                    {
                      "$or": [
                        { "status": "premium" },
                        { "legacy": true }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalOr.class, result);
            PhysicalOr or = (PhysicalOr) result;
            assertEquals(2, or.children().size());

            // Should have one index scan and one full scan
            boolean hasIndexScan = false;
            boolean hasFullScan = false;

            for (PhysicalNode child : or.children()) {
                if (child instanceof PhysicalIndexScan) hasIndexScan = true;
                else if (child instanceof PhysicalFullScan) hasFullScan = true;
            }

            assertTrue(hasIndexScan, "Should have PhysicalIndexScan for indexed field");
            assertTrue(hasFullScan, "Should have PhysicalFullScan for non-indexed field");
        }

        @Test
        @DisplayName("OR with multiple conditions on same indexed field should use multiple PhysicalIndexScans")
        void testOrWithMultipleConditionsOnSameIndexedField() {
            createIndex(IndexDefinition.create(
                    "priority-index", "priority", BsonType.INT32, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("""
                    {
                      "$or": [
                        { "priority": { "$eq": 1 } },
                        { "priority": { "$eq": 5 } }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalOr.class, result);
            PhysicalOr or = (PhysicalOr) result;
            assertEquals(2, or.children().size());

            // Both should be index scans on the same field
            assertInstanceOf(PhysicalIndexScan.class, or.children().get(0));
            assertInstanceOf(PhysicalIndexScan.class, or.children().get(1));

            PhysicalIndexScan scan1 = (PhysicalIndexScan) or.children().get(0);
            PhysicalIndexScan scan2 = (PhysicalIndexScan) or.children().get(1);

            PhysicalFilter filter1 = (PhysicalFilter) scan1.node();
            PhysicalFilter filter2 = (PhysicalFilter) scan2.node();

            assertEquals("priority", filter1.selector());
            assertEquals("priority", filter2.selector());
        }

        @Test
        @DisplayName("Complex OR with nested operations and mixed indexes")
        void testComplexOrWithMixedIndexes() {
            createIndexes(
                    IndexDefinition.create("type-index", "type", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("score-index", "score", BsonType.INT32, SortOrder.ASCENDING)
            );

            PhysicalNode result = planQuery("""
                    {
                      "$or": [
                        { "type": { "$in": ["premium", "gold"] } },
                        { "score": { "$gte": 95 } },
                        { "featured": true },
                        { "recommended": true }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalOr.class, result);
            PhysicalOr or = (PhysicalOr) result;
            assertEquals(4, or.children().size());

            // Count index scans vs full scans
            long indexScans = or.children().stream()
                    .filter(child -> child instanceof PhysicalIndexScan)
                    .count();
            long fullScans = or.children().stream()
                    .filter(child -> child instanceof PhysicalFullScan)
                    .count();

            assertEquals(2, indexScans, "Should have 2 index scans for type and score");
            assertEquals(2, fullScans, "Should have 2 full scans for featured and recommended");
        }
    }

    // ============================================================================
    // Complex Nested Queries with Mixed Index Usage
    // ============================================================================

    @Nested
    @DisplayName("Complex Nested Query Tests")
    class ComplexNestedQueryTests {

        @Test
        @DisplayName("Nested AND/OR with mixed index usage")
        void testNestedAndOrWithMixedIndexes() {
            createIndexes(
                    IndexDefinition.create("user-id-index", "user_id", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("role-index", "role", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("active-index", "active", BsonType.BOOLEAN, SortOrder.ASCENDING)
            );

            PhysicalNode result = planQuery("""
                    {
                      "$and": [
                        { "user_id": "user123" },
                        {
                          "$or": [
                            { "role": "admin" },
                            { "role": "moderator" },
                            { "permissions": { "$in": ["read", "write"] } }
                          ]
                        },
                        { "active": true }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd rootAnd = (PhysicalAnd) result;
            assertEquals(3, rootAnd.children().size());

            // Should have: IndexScan, OR(IndexScan, IndexScan, FullScan), IndexScan
            PhysicalIndexScan userIdScan = null;
            PhysicalOr orNode = null;
            PhysicalIndexScan activeScan = null;

            for (PhysicalNode child : rootAnd.children()) {
                if (child instanceof PhysicalIndexScan scan) {
                    PhysicalFilter filter = (PhysicalFilter) scan.node();
                    if ("user_id".equals(filter.selector())) {
                        userIdScan = scan;
                    } else if ("active".equals(filter.selector())) {
                        activeScan = scan;
                    }
                } else if (child instanceof PhysicalOr or) {
                    orNode = or;
                }
            }

            assertNotNull(userIdScan, "Should have index scan for user_id");
            assertNotNull(activeScan, "Should have index scan for active");
            assertNotNull(orNode, "Should have OR node");

            // Check OR children: 2 index scans + 1 full scan
            assertEquals(3, orNode.children().size());
            long indexScansInOr = orNode.children().stream()
                    .filter(child -> child instanceof PhysicalIndexScan)
                    .count();
            long fullScansInOr = orNode.children().stream()
                    .filter(child -> child instanceof PhysicalFullScan)
                    .count();

            assertEquals(2, indexScansInOr, "Should have 2 index scans in OR for role fields");
            assertEquals(1, fullScansInOr, "Should have 1 full scan in OR for permissions");
        }

        @Test
        @DisplayName("ElemMatch with indexed field in subplan")
        void testElemMatchWithIndexedFieldInSubplan() {
            createIndex(IndexDefinition.create(
                    "items-price-index", "items.price", BsonType.DOUBLE, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("""
                    {
                      "orders": {
                        "$elemMatch": {
                          "$and": [
                            { "items.price": { "$gte": 100.0 } },
                            { "items.category": "electronics" }
                          ]
                        }
                      }
                    }
                    """);

            assertInstanceOf(PhysicalElemMatch.class, result);
            PhysicalElemMatch elemMatch = (PhysicalElemMatch) result;
            assertEquals("orders", elemMatch.selector());

            // Sub-plan should be PhysicalAnd with IndexScan + FullScan
            assertInstanceOf(PhysicalAnd.class, elemMatch.subPlan());
            PhysicalAnd subAnd = (PhysicalAnd) elemMatch.subPlan();
            assertEquals(2, subAnd.children().size());

            // Should have one index scan and one full scan
            boolean hasIndexScan = false;
            boolean hasFullScan = false;

            for (PhysicalNode child : subAnd.children()) {
                if (child instanceof PhysicalIndexScan) hasIndexScan = true;
                else if (child instanceof PhysicalFullScan) hasFullScan = true;
            }

            assertTrue(hasIndexScan, "Should have index scan for items.price");
            assertTrue(hasFullScan, "Should have full scan for items.category");
        }

        @Test
        @DisplayName("NOT operator with indexed field should use PhysicalNot with PhysicalIndexScan")
        void testNotWithIndexedField() {
            createIndex(IndexDefinition.create(
                    "status-index", "status", BsonType.STRING, SortOrder.ASCENDING
            ));

            PhysicalNode result = planQuery("""
                    {
                      "$not": { "status": "deleted" }
                    }
                    """);

            assertInstanceOf(PhysicalNot.class, result);
            PhysicalNot not = (PhysicalNot) result;

            // Child should be index scan
            assertInstanceOf(PhysicalIndexScan.class, not.child());
            PhysicalIndexScan indexScan = (PhysicalIndexScan) not.child();
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("status", filter.selector());
            assertEquals("deleted", extractValue(filter.operand()));
        }
    }

    // ============================================================================
    // Performance and Edge Cases
    // ============================================================================

    @Nested
    @DisplayName("Performance and Edge Cases")
    class PerformanceAndEdgeCases {

        @Test
        @DisplayName("Multiple indexes should all be utilized in complex query")
        void testMultipleIndexUtilization() {
            createIndexes(
                    IndexDefinition.create("user-index", "user_id", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("timestamp-index", "timestamp", BsonType.INT64, SortOrder.DESCENDING),
                    IndexDefinition.create("priority-index", "priority", BsonType.INT32, SortOrder.ASCENDING),
                    IndexDefinition.create("type-index", "type", BsonType.STRING, SortOrder.ASCENDING)
            );

            PhysicalNode result = planQuery("""
                    {
                      "$and": [
                        { "user_id": "user123" },
                        { "timestamp": { "$gte": 1640995200 } },
                        { "priority": { "$in": [1, 2, 3] } },
                        { "type": { "$ne": "system" } },
                        { "processed": false }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd and = (PhysicalAnd) result;
            assertEquals(5, and.children().size());

            // Count index scans vs full scans
            long indexScans = and.children().stream()
                    .filter(child -> child instanceof PhysicalIndexScan)
                    .count();
            long fullScans = and.children().stream()
                    .filter(child -> child instanceof PhysicalFullScan)
                    .count();

            assertEquals(4, indexScans, "Should have 4 index scans for indexed fields");
            assertEquals(1, fullScans, "Should have 1 full scan for non-indexed field");
        }

        @Test
        @DisplayName("Large IN clause with indexed field should use PhysicalIndexScan")
        void testLargeInClauseWithIndex() {
            createIndex(IndexDefinition.create(
                    "category-index", "category", BsonType.STRING, SortOrder.ASCENDING
            ));

            // Create large IN clause with 20 values
            StringBuilder queryBuilder = new StringBuilder("{ \"category\": { \"$in\": [");
            for (int i = 0; i < 20; i++) {
                if (i > 0) queryBuilder.append(", ");
                queryBuilder.append("\"category").append(i).append("\"");
            }
            queryBuilder.append("] } }");

            PhysicalNode result = planQuery(queryBuilder.toString());

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("category", filter.selector());
            assertEquals(Operator.IN, filter.op());

            @SuppressWarnings("unchecked")
            List<Object> operand = (List<Object>) filter.operand();
            assertEquals(20, operand.size());
        }

        @Test
        @DisplayName("Range query with indexed field should use single PhysicalIndexScan")
        void testRangeQueryWithIndex() {
            createIndex(IndexDefinition.create(
                    "date-index", "created_date", BsonType.INT64, SortOrder.ASCENDING
            ));

            // Range query: date >= start AND date <= end
            PhysicalNode result = planQuery("""
                    {
                      "$and": [
                        { "created_date": { "$gte": 1640995200 } },
                        { "created_date": { "$lte": 1672531200 } }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd and = (PhysicalAnd) result;
            assertEquals(2, and.children().size());

            // Both should be index scans on the same field
            assertInstanceOf(PhysicalIndexScan.class, and.children().get(0));
            assertInstanceOf(PhysicalIndexScan.class, and.children().get(1));

            PhysicalIndexScan scan1 = (PhysicalIndexScan) and.children().get(0);
            PhysicalIndexScan scan2 = (PhysicalIndexScan) and.children().get(1);

            PhysicalFilter filter1 = (PhysicalFilter) scan1.node();
            PhysicalFilter filter2 = (PhysicalFilter) scan2.node();

            assertEquals("created_date", filter1.selector());
            assertEquals("created_date", filter2.selector());
            assertEquals(Operator.GTE, filter1.op());
            assertEquals(Operator.LTE, filter2.op());
        }

        @Test
        @DisplayName("Index on different data types should work correctly")
        void testDifferentDataTypeIndexes() {
            createIndexes(
                    IndexDefinition.create("string-index", "name", BsonType.STRING, SortOrder.ASCENDING),
                    IndexDefinition.create("int32-index", "age", BsonType.INT32, SortOrder.ASCENDING),
                    IndexDefinition.create("double-index", "score", BsonType.DOUBLE, SortOrder.ASCENDING),
                    IndexDefinition.create("boolean-index", "active", BsonType.BOOLEAN, SortOrder.ASCENDING)
            );

            PhysicalNode result = planQuery("""
                    {
                      "$and": [
                        { "name": "test" },
                        { "age": 25 },
                        { "score": 95.5 },
                        { "active": true }
                      ]
                    }
                    """);

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd and = (PhysicalAnd) result;
            assertEquals(4, and.children().size());

            // All should be index scans
            for (PhysicalNode child : and.children()) {
                assertInstanceOf(PhysicalIndexScan.class, child);
            }
        }
    }
}
