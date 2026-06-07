/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
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
    void createIndex(SingleFieldIndexDefinition definition) {
        createIndexThenWaitForReadiness(definition);
        // Refresh the index registry
        metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
    }

    /**
     * Helper method to create multiple indexes at once
     */
    void createIndexes(SingleFieldIndexDefinition... definitions) {
        for (SingleFieldIndexDefinition definition : definitions) {
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
        void shouldUsePhysicalIndexScanForNumericIndexWithGTEOperator() {
            createIndex(SingleFieldIndexDefinition.create(
                    "numeric-index", "int32-field", BsonType.INT32
                    , false, IndexStatus.WAITING));

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
        void shouldUsePhysicalIndexScanForStringIndexWithEQOperator() {
            createIndex(SingleFieldIndexDefinition.create(
                    "string-index", "name", BsonType.STRING
                    , false, IndexStatus.WAITING));

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
        void shouldUsePhysicalIndexScanForNumericIndexWithGTOperator() {
            createIndex(SingleFieldIndexDefinition.create(
                    "price-index", "price", BsonType.DOUBLE
                    , false, IndexStatus.WAITING));

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
        void shouldUsePhysicalIndexScanForNumericIndexWithLTOperator() {
            createIndex(SingleFieldIndexDefinition.create(
                    "age-index", "age", BsonType.INT32
                    , false, IndexStatus.WAITING));

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
        void shouldUsePhysicalIndexScanForNumericIndexWithLTEOperator() {
            createIndex(SingleFieldIndexDefinition.create(
                    "score-index", "score", BsonType.INT32
                    , false, IndexStatus.WAITING));

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
        void shouldUsePhysicalIndexScanForStringIndexWithNEOperator() {
            createIndex(SingleFieldIndexDefinition.create(
                    "status-index", "status", BsonType.STRING
                    , false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ \"status\": { \"$ne\": \"deleted\" } }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("status", filter.selector());
            assertEquals(Operator.NE, filter.op());
            assertEquals("deleted", extractValue(filter.operand()));
        }

        @Test
        @DisplayName("String index with IN operator should use PhysicalOr with multiple EQ index scans")
        void shouldUsePhysicalOrForStringIndexWithINOperator() {
            createIndex(SingleFieldIndexDefinition.create(
                    "category-index", "category", BsonType.STRING
                    , false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ \"category\": { \"$in\": [\"electronics\", \"books\", \"clothing\"] } }");

            // IN with index is transformed to OR with multiple EQ index scans
            assertInstanceOf(PhysicalOr.class, result);
            PhysicalOr or = (PhysicalOr) result;
            assertEquals(3, or.children().size());

            List<String> expectedValues = Arrays.asList("electronics", "books", "clothing");
            for (int i = 0; i < or.children().size(); i++) {
                assertInstanceOf(PhysicalIndexScan.class, or.children().get(i));
                PhysicalIndexScan indexScan = (PhysicalIndexScan) or.children().get(i);
                PhysicalFilter filter = (PhysicalFilter) indexScan.node();
                assertEquals("category", filter.selector());
                assertEquals(Operator.EQ, filter.op());
                assertEquals(expectedValues.get(i), extractValue(filter.operand()));
            }
        }

        @Test
        @DisplayName("String index with NIN operator should use PhysicalAnd with NE index scans")
        void shouldUsePhysicalAndForStringIndexWithNINOperator() {
            createIndex(SingleFieldIndexDefinition.create(
                    "type-index", "type", BsonType.STRING
                    , false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ \"type\": { \"$nin\": [\"spam\", \"deleted\"] } }");

            // NIN with index is transformed to AND with multiple NE index scans
            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd and = (PhysicalAnd) result;
            assertEquals(2, and.children().size());

            List<String> expectedValues = Arrays.asList("spam", "deleted");
            for (int i = 0; i < and.children().size(); i++) {
                assertInstanceOf(PhysicalIndexScan.class, and.children().get(i));
                PhysicalIndexScan indexScan = (PhysicalIndexScan) and.children().get(i);
                PhysicalFilter filter = (PhysicalFilter) indexScan.node();
                assertEquals("type", filter.selector());
                assertEquals(Operator.NE, filter.op());
                assertEquals(expectedValues.get(i), extractValue(filter.operand()));
            }
        }

        @Test
        @DisplayName("Array index with ALL operator should use PhysicalFullScan (not indexable)")
        void shouldUsePhysicalFullScanForArrayIndexWithALLOperator() {
            createIndex(SingleFieldIndexDefinition.create(
                    "tags-index", "tags", BsonType.STRING
                    , false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ \"tags\": { \"$all\": [\"urgent\", \"important\"] } }");

            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("tags", filter.selector());
            assertEquals(Operator.ALL, filter.op());

            @SuppressWarnings("unchecked")
            List<Object> operand = (List<Object>) filter.operand();
            assertEquals(Arrays.asList("urgent", "important"), extractValue(operand));
        }

        @Test
        @DisplayName("EXISTS operator should use PhysicalFullScan (not indexable)")
        void shouldUsePhysicalFullScanForEXISTSOperator() {
            PhysicalNode result = planQuery("{ \"metadata\": { \"$exists\": true } }");

            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("metadata", filter.selector());
            assertEquals(Operator.EXISTS, filter.op());
            assertEquals(true, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("SIZE operator should use PhysicalFullScan (not indexable)")
        void shouldUsePhysicalFullScanForSIZEOperator() {
            PhysicalNode result = planQuery("{ \"tags\": { \"$size\": 3 } }");

            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("tags", filter.selector());
            assertEquals(Operator.SIZE, filter.op());
        }
    }

    // ============================================================================
    // Fallback to FullScan Tests
    // ============================================================================

    @Nested
    @DisplayName("Fallback to FullScan Tests")
    class FallbackToFullScanTests {

        @Test
        @DisplayName("Type mismatch should fall back to full scan")
        void shouldFallbackToFullScanOnTypeMismatch() {
            // Create INT32 index but query with string value
            createIndex(SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ \"age\": \"twenty\" }");

            // Should fall back to full scan due to type mismatch
            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("age", filter.selector());
            assertEquals(Operator.EQ, filter.op());
        }

        @Test
        @DisplayName("$in without index should use full scan with IN operator preserved")
        void shouldUseFullScanWithInOperatorPreservedWhenNoIndex() {
            // No index created for 'category' field
            PhysicalNode result = planQuery("{ \"category\": { \"$in\": [\"electronics\", \"books\"] } }");

            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("category", filter.selector());
            assertEquals(Operator.IN, filter.op());

            @SuppressWarnings("unchecked")
            List<Object> operand = (List<Object>) filter.operand();
            assertEquals(Arrays.asList("electronics", "books"), extractValue(operand));
        }

        @Test
        @DisplayName("$in with type mismatch should fall back to full scan")
        void shouldFallbackToFullScanForInWithTypeMismatch() {
            // Create STRING index but query with integer values
            createIndex(SingleFieldIndexDefinition.create("priority-index", "priority", BsonType.STRING, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ \"priority\": { \"$in\": [1, 2, 3] } }");

            // Should fall back to full scan due to type mismatch
            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("priority", filter.selector());
            assertEquals(Operator.IN, filter.op());
        }

        @Test
        @DisplayName("$in with mixed types where some match index should fall back to full scan")
        void shouldFallbackToFullScanForInWithPartialTypeMismatch() {
            // Create STRING index but query with mixed string and integer values
            createIndex(SingleFieldIndexDefinition.create("status-index", "status", BsonType.STRING, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ \"status\": { \"$in\": [\"active\", 1, \"inactive\"] } }");

            // Should fall back to full scan due to partial type mismatch
            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("status", filter.selector());
            assertEquals(Operator.IN, filter.op());
        }

        @Test
        @DisplayName("Comparison operator with type mismatch should fall back to full scan")
        void shouldFallbackToFullScanForComparisonWithTypeMismatch() {
            // Create INT64 index but query with string value
            createIndex(SingleFieldIndexDefinition.create("timestamp-index", "timestamp", BsonType.INT64, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ \"timestamp\": { \"$gte\": \"2024-01-01\" } }");

            // Should fall back to full scan due to type mismatch
            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("timestamp", filter.selector());
            assertEquals(Operator.GTE, filter.op());
        }

        @Test
        @DisplayName("$nin without index should use full scan with NIN operator preserved")
        void shouldUseFullScanWithNinOperatorPreservedWhenNoIndex() {
            // No index created for 'type' field
            PhysicalNode result = planQuery("{ \"type\": { \"$nin\": [\"spam\", \"deleted\"] } }");

            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("type", filter.selector());
            assertEquals(Operator.NIN, filter.op());
        }

        @Test
        @DisplayName("$ne on multikey index should fall back to full scan")
        void shouldFallbackToFullScanForNeOnMultikeyIndex() {
            // Behavior: $ne on multikey indexes must use FullScan because index scan finds
            // "any element != value" but $ne requires "no element == value".
            createIndex(SingleFieldIndexDefinition.create(
                    "grades-index", "grades", BsonType.INT32, true, IndexStatus.WAITING // multiKey = true
            ));

            PhysicalNode result = planQuery("{ \"grades\": { \"$ne\": 100 } }");

            // Should fall back to full scan due to multikey index
            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("grades", filter.selector());
            assertEquals(Operator.NE, filter.op());
            assertEquals(100, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("$ne on non-multikey index should use index scan")
        void shouldUseIndexScanForNeOnNonMultikeyIndex() {
            // Behavior: $ne on regular (non-multikey) indexes can safely use index scan.
            createIndex(SingleFieldIndexDefinition.create(
                    "age-index", "age", BsonType.INT32, false, IndexStatus.WAITING // multiKey = false
            ));

            PhysicalNode result = planQuery("{ \"age\": { \"$ne\": 30 } }");

            // Should use index scan for non-multikey index
            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("age", filter.selector());
            assertEquals(Operator.NE, filter.op());
            assertEquals(30, extractValue(filter.operand()));
        }

        @Test
        @DisplayName("$nin with type mismatch should fall back to full scan")
        void shouldFallbackToFullScanForNinWithTypeMismatch() {
            // Create STRING index but query with integer values
            createIndex(SingleFieldIndexDefinition.create("priority-index", "priority", BsonType.STRING, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ \"priority\": { \"$nin\": [1, 2, 3] } }");

            // Should fall back to full scan due to type mismatch
            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("priority", filter.selector());
            assertEquals(Operator.NIN, filter.op());
        }

        @Test
        @DisplayName("$nin with mixed types where some match index should fall back to full scan")
        void shouldFallbackToFullScanForNinWithPartialTypeMismatch() {
            // Create STRING index but query with mixed string and integer values
            createIndex(SingleFieldIndexDefinition.create("status-index", "status", BsonType.STRING, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ \"status\": { \"$nin\": [\"active\", 1, \"inactive\"] } }");

            // Should fall back to full scan due to partial type mismatch
            assertInstanceOf(PhysicalFullScan.class, result);
            PhysicalFullScan fullScan = (PhysicalFullScan) result;
            PhysicalFilter filter = (PhysicalFilter) fullScan.node();
            assertEquals("status", filter.selector());
            assertEquals(Operator.NIN, filter.op());
        }

        @Test
        @DisplayName("Empty $nin should return PhysicalTrue (matches everything)")
        void shouldReturnPhysicalTrueForEmptyNin() {
            createIndex(SingleFieldIndexDefinition.create("type-index", "type", BsonType.STRING, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ \"type\": { \"$nin\": [] } }");

            // Empty $nin matches everything
            assertInstanceOf(PhysicalTrue.class, result);
        }

        @Test
        @DisplayName("Single value $nin with index should optimize to single PhysicalIndexScan(NE)")
        void shouldOptimizeSingleValueNinToSingleIndexScan() {
            createIndex(SingleFieldIndexDefinition.create("type-index", "type", BsonType.STRING, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ \"type\": { \"$nin\": [\"spam\"] } }");

            // Single value $nin is optimized to single NE index scan
            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("type", filter.selector());
            assertEquals(Operator.NE, filter.op());
            assertEquals("spam", extractValue(filter.operand()));
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
        void shouldUsePhysicalAndWithTwoPhysicalIndexScansForTwoIndexedFields() {
            createIndexes(
                    SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING),
                    SingleFieldIndexDefinition.create("status-index", "status", BsonType.STRING, false, IndexStatus.WAITING)
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
        void shouldMixPhysicalIndexScanAndPhysicalFullScanForMixedIndexedFields() {
            createIndex(SingleFieldIndexDefinition.create(
                    "price-index", "price", BsonType.DOUBLE
                    , false, IndexStatus.WAITING));

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
        void shouldUseSinglePhysicalIndexScanForMultipleConditionsOnSameField() {
            createIndex(SingleFieldIndexDefinition.create(
                    "age-index", "age", BsonType.INT32
                    , false, IndexStatus.WAITING));

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
        void shouldHandleComplexAndWithNestedOperationsAndMixedIndexes() {
            createIndexes(
                    SingleFieldIndexDefinition.create("category-index", "category", BsonType.STRING, false, IndexStatus.WAITING),
                    SingleFieldIndexDefinition.create("price-index", "price", BsonType.DOUBLE, false, IndexStatus.WAITING)
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
            // Note: $in with index becomes PhysicalOr containing PhysicalIndexScans
            long indexScans = and.children().stream()
                    .filter(child -> child instanceof PhysicalIndexScan)
                    .count();
            long orWithIndexScans = and.children().stream()
                    .filter(child -> child instanceof PhysicalOr)
                    .count();
            long fullScans = and.children().stream()
                    .filter(child -> child instanceof PhysicalFullScan)
                    .count();

            assertEquals(1, indexScans, "Should have 1 index scan for price");
            assertEquals(1, orWithIndexScans, "Should have 1 PhysicalOr for category $in");
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
        void shouldUsePhysicalOrWithTwoPhysicalIndexScansForTwoIndexedFields() {
            createIndexes(
                    SingleFieldIndexDefinition.create("name-index", "name", BsonType.STRING, false, IndexStatus.WAITING),
                    SingleFieldIndexDefinition.create("email-index", "email", BsonType.STRING, false, IndexStatus.WAITING)
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
        void shouldMixPhysicalIndexScanAndPhysicalFullScanForOrWithMixedFields() {
            createIndex(SingleFieldIndexDefinition.create(
                    "status-index", "status", BsonType.STRING
                    , false, IndexStatus.WAITING));

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
        void shouldUseMultiplePhysicalIndexScansForOrWithMultipleConditionsOnSameField() {
            createIndex(SingleFieldIndexDefinition.create(
                    "priority-index", "priority", BsonType.INT32
                    , false, IndexStatus.WAITING));

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
        void shouldHandleComplexOrWithNestedOperationsAndMixedIndexes() {
            createIndexes(
                    SingleFieldIndexDefinition.create("type-index", "type", BsonType.STRING, false, IndexStatus.WAITING),
                    SingleFieldIndexDefinition.create("score-index", "score", BsonType.INT32, false, IndexStatus.WAITING)
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
            // $in with index becomes nested PhysicalOr, so we have 5 children after flattening
            // or 4 if the nested OR is not flattened
            assertTrue(or.children().size() >= 4);

            // Count index scans vs full scans
            // Note: $in with index becomes PhysicalOr containing PhysicalIndexScans
            long indexScans = or.children().stream()
                    .filter(child -> child instanceof PhysicalIndexScan)
                    .count();
            long nestedOrs = or.children().stream()
                    .filter(child -> child instanceof PhysicalOr)
                    .count();
            long fullScans = or.children().stream()
                    .filter(child -> child instanceof PhysicalFullScan)
                    .count();

            // score uses 1 index scan, type $in becomes nested PhysicalOr with 2 index scans
            assertEquals(1, indexScans, "Should have 1 index scan for score");
            assertEquals(1, nestedOrs, "Should have 1 nested PhysicalOr for type $in");
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
        void shouldHandleNestedAndOrWithMixedIndexUsage() {
            createIndexes(
                    SingleFieldIndexDefinition.create("user-id-index", "user_id", BsonType.STRING, false, IndexStatus.WAITING),
                    SingleFieldIndexDefinition.create("role-index", "role", BsonType.STRING, false, IndexStatus.WAITING),
                    SingleFieldIndexDefinition.create("active-index", "active", BsonType.BOOLEAN, false, IndexStatus.WAITING)
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
        void shouldHandleElemMatchWithIndexedFieldInSubplan() {
            createIndex(SingleFieldIndexDefinition.create(
                    "items-price-index", "items.price", BsonType.DOUBLE
                    , false, IndexStatus.WAITING));

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
        void shouldUsePhysicalNotWithPhysicalIndexScanForIndexedField() {
            createIndex(SingleFieldIndexDefinition.create(
                    "status-index", "status", BsonType.STRING
                    , false, IndexStatus.WAITING));

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
        void shouldUtilizeAllMultipleIndexesInComplexQuery() {
            createIndexes(
                    SingleFieldIndexDefinition.create("user-index", "user_id", BsonType.STRING, false, IndexStatus.WAITING),
                    SingleFieldIndexDefinition.create("timestamp-index", "timestamp", BsonType.INT64, false, IndexStatus.WAITING),
                    SingleFieldIndexDefinition.create("priority-index", "priority", BsonType.INT32, false, IndexStatus.WAITING),
                    SingleFieldIndexDefinition.create("type-index", "type", BsonType.STRING, false, IndexStatus.WAITING)
            );

            PhysicalNode result = planQuery("""
                    {
                      "$and": [
                        { "user_id": "user123" },
                        { "timestamp": { "$gte": { "$numberLong": "1640995200" } } },
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
            // Note: $in with index becomes PhysicalOr containing PhysicalIndexScans
            long indexScans = and.children().stream()
                    .filter(child -> child instanceof PhysicalIndexScan)
                    .count();
            long orWithIndexScans = and.children().stream()
                    .filter(child -> child instanceof PhysicalOr)
                    .count();
            long fullScans = and.children().stream()
                    .filter(child -> child instanceof PhysicalFullScan)
                    .count();

            assertEquals(3, indexScans, "Should have 3 index scans for user_id, timestamp, type");
            assertEquals(1, orWithIndexScans, "Should have 1 PhysicalOr for priority $in");
            assertEquals(1, fullScans, "Should have 1 full scan for non-indexed field");
        }

        @Test
        @DisplayName("Large IN clause with indexed field should use PhysicalOr with EQ index scans")
        void shouldUsePhysicalOrForLargeINClauseWithIndexedField() {
            createIndex(SingleFieldIndexDefinition.create(
                    "category-index", "category", BsonType.STRING
                    , false, IndexStatus.WAITING));

            // Create large IN clause with 20 values
            StringBuilder queryBuilder = new StringBuilder("{ \"category\": { \"$in\": [");
            for (int i = 0; i < 20; i++) {
                if (i > 0) queryBuilder.append(", ");
                queryBuilder.append("\"category").append(i).append("\"");
            }
            queryBuilder.append("] } }");

            PhysicalNode result = planQuery(queryBuilder.toString());

            // IN with index is transformed to OR with multiple EQ index scans
            assertInstanceOf(PhysicalOr.class, result);
            PhysicalOr or = (PhysicalOr) result;
            assertEquals(20, or.children().size());

            // Verify all children are index scans with EQ operator
            for (PhysicalNode child : or.children()) {
                assertInstanceOf(PhysicalIndexScan.class, child);
                PhysicalIndexScan indexScan = (PhysicalIndexScan) child;
                PhysicalFilter filter = (PhysicalFilter) indexScan.node();
                assertEquals("category", filter.selector());
                assertEquals(Operator.EQ, filter.op());
            }
        }

        @Test
        @DisplayName("Range query with indexed field should use single PhysicalIndexScan")
        void shouldUseSinglePhysicalIndexScanForRangeQueryWithIndexedField() {
            createIndex(SingleFieldIndexDefinition.create(
                    "date-index", "created_date", BsonType.INT64
                    , false, IndexStatus.WAITING));

            // Range query: date >= start AND date <= end
            PhysicalNode result = planQuery("""
                    {
                      "$and": [
                        { "created_date": { "$gte": { "$numberLong": "1640995200" } } },
                        { "created_date": { "$lte": { "$numberLong": "1672531200" } } }
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
        void shouldHandleIndexesOnDifferentDataTypesCorrectly() {
            createIndexes(
                    SingleFieldIndexDefinition.create("string-index", "name", BsonType.STRING, false, IndexStatus.WAITING),
                    SingleFieldIndexDefinition.create("int32-index", "age", BsonType.INT32, false, IndexStatus.WAITING),
                    SingleFieldIndexDefinition.create("double-index", "score", BsonType.DOUBLE, false, IndexStatus.WAITING),
                    SingleFieldIndexDefinition.create("boolean-index", "active", BsonType.BOOLEAN, false, IndexStatus.WAITING)
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

    // ============================================================================
    // Primary Index IN/NIN Tests
    // ============================================================================

    @Nested
    @DisplayName("Primary Index IN/NIN Tests")
    class PrimaryIndexInNinTests {

        @Test
        @DisplayName("$in on _id should use PhysicalOr with multiple EQ index scans")
        void shouldUsePhysicalOrForPrimaryIndexWithIN() {
            // Behavior: $in on _id produces PhysicalOr with EQ index scans, one per ObjectId value.
            PhysicalNode result = planQuery("""
                    { "_id": { "$in": [{"$oid": "507f1f77bcf86cd799439011"}, {"$oid": "507f1f77bcf86cd799439012"}] } }
                    """);

            assertInstanceOf(PhysicalOr.class, result);
            PhysicalOr or = (PhysicalOr) result;
            assertEquals(2, or.children().size());

            for (PhysicalNode child : or.children()) {
                assertInstanceOf(PhysicalIndexScan.class, child);
                PhysicalIndexScan indexScan = (PhysicalIndexScan) child;
                assertInstanceOf(PhysicalFilter.class, indexScan.node());
                PhysicalFilter filter = (PhysicalFilter) indexScan.node();
                assertEquals("_id", filter.selector());
                assertEquals(Operator.EQ, filter.op());
            }
        }

        @Test
        @DisplayName("$nin on _id should use PhysicalAnd with multiple NE index scans")
        void shouldUsePhysicalAndForPrimaryIndexWithNIN() {
            // Behavior: $nin on _id produces PhysicalAnd with NE index scans, one per ObjectId value.
            PhysicalNode result = planQuery("""
                    { "_id": { "$nin": [{"$oid": "507f1f77bcf86cd799439011"}, {"$oid": "507f1f77bcf86cd799439012"}] } }
                    """);

            assertInstanceOf(PhysicalAnd.class, result);
            PhysicalAnd and = (PhysicalAnd) result;
            assertEquals(2, and.children().size());

            for (PhysicalNode child : and.children()) {
                assertInstanceOf(PhysicalIndexScan.class, child);
                PhysicalIndexScan indexScan = (PhysicalIndexScan) child;
                assertInstanceOf(PhysicalFilter.class, indexScan.node());
                PhysicalFilter filter = (PhysicalFilter) indexScan.node();
                assertEquals("_id", filter.selector());
                assertEquals(Operator.NE, filter.op());
            }
        }

        @Test
        @DisplayName("Single value $in on _id should optimize to single PhysicalIndexScan(EQ)")
        void shouldOptimizeSingleValueInOnPrimaryIndex() {
            // Behavior: $in with a single ObjectId on _id is optimized to a single EQ index scan.
            PhysicalNode result = planQuery("""
                    { "_id": { "$in": [{"$oid": "507f1f77bcf86cd799439011"}] } }
                    """);

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            assertInstanceOf(PhysicalFilter.class, indexScan.node());
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("_id", filter.selector());
            assertEquals(Operator.EQ, filter.op());
        }

        @Test
        @DisplayName("Single value $nin on _id should optimize to single PhysicalIndexScan(NE)")
        void shouldOptimizeSingleValueNinOnPrimaryIndex() {
            // Behavior: $nin with a single ObjectId on _id is optimized to a single NE index scan.
            PhysicalNode result = planQuery("""
                    { "_id": { "$nin": [{"$oid": "507f1f77bcf86cd799439011"}] } }
                    """);

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            assertInstanceOf(PhysicalFilter.class, indexScan.node());
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("_id", filter.selector());
            assertEquals(Operator.NE, filter.op());
        }
    }

    // ============================================================================
    // Numeric Widening Tests
    // ============================================================================

    @Nested
    @DisplayName("Numeric Widening Tests")
    class NumericWideningTests {

        @Test
        void shouldUseIndexScanWhenInt32PredicateMatchesInt64Index() {
            // Behavior: INT32 query value (42) should use INT64 index via lossless widening
            createIndex(SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT64, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ 'age': 42 }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("age", filter.selector());
            assertEquals(Operator.EQ, filter.op());
        }

        @Test
        void shouldUseIndexScanWhenInt32PredicateMatchesDoubleIndex() {
            // Behavior: INT32 query value (42) should use DOUBLE index via lossless widening
            createIndex(SingleFieldIndexDefinition.create("price-index", "price", BsonType.DOUBLE, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ 'price': 42 }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("price", filter.selector());
            assertEquals(Operator.EQ, filter.op());
        }

        @Test
        void shouldFallbackToFullScanWhenInt64PredicateMatchesDoubleIndex() {
            // Behavior: INT64 -> DOUBLE is lossy (64-bit integer exceeds double's 53-bit mantissa),
            // so the planner must NOT use the index
            createIndex(SingleFieldIndexDefinition.create("score-index", "score", BsonType.DOUBLE, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ 'score': { '$numberLong': '42' } }");

            assertInstanceOf(PhysicalFullScan.class, result);
        }

        @Test
        void shouldFallbackToFullScanWhenDoublePredicateMatchesInt64Index() {
            // Behavior: DOUBLE -> INT64 is not a valid widening path, must fall back to full scan
            createIndex(SingleFieldIndexDefinition.create("count-index", "count", BsonType.INT64, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ 'count': 42.5 }");

            assertInstanceOf(PhysicalFullScan.class, result);
        }

        @Test
        void shouldUseIndexScanWhenInt32GtPredicateMatchesInt64Index() {
            // Behavior: INT32 range predicate should use INT64 index via lossless widening
            createIndex(SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT64, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ 'age': { '$gt': 18 } }");

            assertInstanceOf(PhysicalIndexScan.class, result);
            PhysicalIndexScan indexScan = (PhysicalIndexScan) result;
            PhysicalFilter filter = (PhysicalFilter) indexScan.node();
            assertEquals("age", filter.selector());
            assertEquals(Operator.GT, filter.op());
        }

        @Test
        void shouldUseIndexScanWhenInt32InPredicateMatchesInt64Index() {
            // Behavior: $in with INT32 values should use INT64 index via lossless widening
            createIndex(SingleFieldIndexDefinition.create("status-index", "status", BsonType.INT64, false, IndexStatus.WAITING));

            PhysicalNode result = planQuery("{ 'status': { '$in': [1, 2, 3] } }");

            assertInstanceOf(PhysicalOr.class, result);
            PhysicalOr or = (PhysicalOr) result;
            assertEquals(3, or.children().size());
            for (PhysicalNode child : or.children()) {
                assertInstanceOf(PhysicalIndexScan.class, child);
            }
        }
    }
}
