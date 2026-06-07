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

import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.planner.Operator;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PhysicalPlannerCompoundIndexTest extends BasePhysicalPlannerTest {

    void createCompoundIndex(CompoundIndexDefinition definition) {
        createIndexThenWaitForReadiness(definition);
        metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
    }

    void createSingleFieldIndex(SingleFieldIndexDefinition definition) {
        createIndexThenWaitForReadiness(definition);
        metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
    }

    @Test
    void shouldEmitCompoundIndexScanForTwoFieldEqMatch() {
        // Behavior: AND(a=1, b=2) with compound(a,b) should produce a PhysicalCompoundIndexScan
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'b': 2 }");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals("idx_ab", scan.index().name());
        assertEquals(2, scan.filters().size());
        assertEquals("a", scan.filters().get(0).selector());
        assertEquals(Operator.EQ, scan.filters().get(0).op());
        assertEquals("b", scan.filters().get(1).selector());
        assertEquals(Operator.EQ, scan.filters().get(1).op());
    }

    @Test
    void shouldEmitCompoundIndexScanForThreeFieldEqMatch() {
        // Behavior: AND(a=1, b=2, c=3) with compound(a,b,c) should produce a PhysicalCompoundIndexScan
        createCompoundIndex(CompoundIndexDefinition.create("idx_abc", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false),
                new CompoundIndexField("c", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'b': 2, 'c': 3 }");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(3, scan.filters().size());
    }

    @Test
    void shouldEmitCompoundIndexScanForPrefixMatch() {
        // Behavior: AND(a=1, b=2) with compound(a,b,c) should produce a PhysicalCompoundIndexScan (prefix of 2)
        createCompoundIndex(CompoundIndexDefinition.create("idx_abc", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false),
                new CompoundIndexField("c", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'b': 2 }");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(2, scan.filters().size());
        assertEquals("a", scan.filters().get(0).selector());
        assertEquals("b", scan.filters().get(1).selector());
    }

    @Test
    void shouldEmitCompoundIndexScanForEqPrefixPlusRange() {
        // Behavior: AND(a=1, b>5) with compound(a,b) should produce a PhysicalCompoundIndexScan with EQ+GT filters
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'b': { '$gt': 5 } }");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(2, scan.filters().size());
        assertEquals(Operator.EQ, scan.filters().get(0).op());
        assertEquals(Operator.GT, scan.filters().get(1).op());
    }

    @Test
    void shouldEmitCompoundIndexScanForEqPrefixPlusLteRange() {
        // Behavior: AND(a=1, b<=10) with compound(a,b) should produce a PhysicalCompoundIndexScan
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'b': { '$lte': 10 } }");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(2, scan.filters().size());
        assertEquals(Operator.EQ, scan.filters().get(0).op());
        assertEquals(Operator.LTE, scan.filters().get(1).op());
    }

    @Test
    void shouldIncludeMultipleRangeFiltersOnLastField() {
        // Behavior: AND(a=1, b>5, b<10) with compound(a,b) should produce a PhysicalCompoundIndexScan with 3 filters
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'b': { '$gt': 5, '$lt': 10 } }");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(3, scan.filters().size());
        assertEquals("a", scan.filters().get(0).selector());
        assertEquals(Operator.EQ, scan.filters().get(0).op());
        // The two range filters on b
        assertEquals("b", scan.filters().get(1).selector());
        assertEquals("b", scan.filters().get(2).selector());
    }

    @Test
    void shouldEmitCompoundScanForSingleFieldPrefixMatch() {
        // Behavior: AND(a=1, c=3) with compound(a,b,c) should produce a compound scan for
        // the leading prefix field 'a', with 'c' as residual (gap at b stops prefix walk).
        createCompoundIndex(CompoundIndexDefinition.create("idx_abc", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false),
                new CompoundIndexField("c", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'c': 3 }");

        assertInstanceOf(PhysicalAnd.class, result);
        PhysicalAnd and = (PhysicalAnd) result;
        assertInstanceOf(PhysicalCompoundIndexScan.class, and.children().get(0));
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) and.children().get(0);
        assertEquals(1, scan.filters().size());
        assertEquals("a", scan.filters().getFirst().selector());
        assertEquals(Operator.EQ, scan.filters().getFirst().op());
    }

    @Test
    void shouldEmitCompoundScanWithNeAsResidual() {
        // Behavior: AND(a=1, b!=2) with compound(a,b) should produce a compound scan for
        // the leading prefix 'a', with NE on 'b' as residual (NE is not compound-indexable).
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'b': { '$ne': 2 } }");

        assertInstanceOf(PhysicalAnd.class, result);
        PhysicalAnd and = (PhysicalAnd) result;
        assertInstanceOf(PhysicalCompoundIndexScan.class, and.children().get(0));
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) and.children().get(0);
        assertEquals(1, scan.filters().size());
        assertEquals("a", scan.filters().getFirst().selector());
        assertEquals(Operator.EQ, scan.filters().getFirst().op());
    }

    @Test
    void shouldNotEmitCompoundIndexScanForTypeMismatch() {
        // Behavior: AND(a="str", b=2) with compound(a:INT32, b:INT32) should NOT produce a compound scan
        // because type mismatch on 'a'
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 'str', 'b': 2 }");

        assertFalse(result instanceof PhysicalCompoundIndexScan);
    }

    @Test
    void shouldPreferCompoundIndexWithMoreMatchedFields() {
        // Behavior: Two compound indexes available, should pick the one matching more fields
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));
        createCompoundIndex(CompoundIndexDefinition.create("idx_abc", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false),
                new CompoundIndexField("c", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'b': 2, 'c': 3 }");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals("idx_abc", scan.index().name());
        assertEquals(3, scan.filters().size());
    }

    @Test
    void shouldLeaveUnmatchedFiltersInAnd() {
        // Behavior: AND(a=1, b=2, c=3) with compound(a,b) should produce
        // PhysicalAnd(PhysicalCompoundIndexScan(a,b), remaining(c))
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'b': 2, 'c': 3 }");

        assertInstanceOf(PhysicalAnd.class, result);
        PhysicalAnd and = (PhysicalAnd) result;
        assertEquals(2, and.children().size());

        // First child should be the compound index scan
        assertInstanceOf(PhysicalCompoundIndexScan.class, and.children().get(0));
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) and.children().get(0);
        assertEquals(2, scan.filters().size());

        // Second child should be a full scan for 'c' (no single-field index on c)
        assertInstanceOf(PhysicalFullScan.class, and.children().get(1));
    }

    @Test
    void shouldFallBackToSingleFieldIndexesWhenNoCompoundMatch() {
        // Behavior: No compound index defined, should use existing single-field behavior
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_a", "a", BsonType.INT32
                , false, IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'b': 2 }");

        assertInstanceOf(PhysicalAnd.class, result);
        PhysicalAnd and = (PhysicalAnd) result;
        // a should get an index scan, b should get a full scan
        boolean hasIndexScan = false;
        boolean hasFullScan = false;
        for (PhysicalNode child : and.children()) {
            if (child instanceof PhysicalIndexScan) hasIndexScan = true;
            if (child instanceof PhysicalFullScan) hasFullScan = true;
        }
        assertTrue(hasIndexScan);
        assertTrue(hasFullScan);
    }

    @Test
    void shouldEmitCompoundScanForRangeOnFirstFieldWithResidual() {
        // Behavior: AND(a>5, b=2) with compound(a,b) should produce a compound scan for a>5
        // on the leading prefix. Range stops prefix walk, so b=2 becomes residual.
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': { '$gt': 5 }, 'b': 2 }");

        assertInstanceOf(PhysicalAnd.class, result);
        PhysicalAnd and = (PhysicalAnd) result;
        assertInstanceOf(PhysicalCompoundIndexScan.class, and.children().get(0));
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) and.children().get(0);
        assertEquals(1, scan.filters().size());
        assertEquals("a", scan.filters().getFirst().selector());
        assertEquals(Operator.GT, scan.filters().getFirst().op());
    }

    // ==================== Sort-aware compound index selection ====================

    @Test
    void shouldEmitCompoundIndexScanForSingleEqFilterWithSortBy() {
        // Behavior: A single EQ filter with sortByField matching the next compound index field
        // should produce a PhysicalCompoundIndexScan, enabling index-provided sort order.
        createCompoundIndex(CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'category': 'bugs' }", "priority");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals("idx_cat_pri", scan.index().name());
        assertEquals(1, scan.filters().size());
        assertEquals("category", scan.filters().getFirst().selector());
        assertEquals(Operator.EQ, scan.filters().getFirst().op());
    }

    @Test
    void shouldEmitCompoundScanForSingleFilterWithoutSortBy() {
        // Behavior: A single filter on the leading prefix field should use compound index
        // even without sortByField, since the leading prefix scan is valid.
        createCompoundIndex(CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'category': 'bugs' }");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(1, scan.filters().size());
        assertEquals("category", scan.filters().getFirst().selector());
    }

    @Test
    void shouldEmitCompoundScanForSingleFilterWithSortByOnUnrelatedField() {
        // Behavior: A single filter on the leading prefix field should use compound index
        // even when sortByField does not match the next field. The compound scan is still
        // more efficient than a full scan.
        createCompoundIndex(CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'category': 'bugs' }", "unrelated");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(1, scan.filters().size());
        assertEquals("category", scan.filters().getFirst().selector());
    }

    @Test
    void shouldEmitCompoundScanForSingleRangeFilterWithSortBy() {
        // Behavior: A range filter on the leading prefix field should use compound index.
        // Sort order on the next field is not guaranteed across different prefix values,
        // but the compound scan is still more efficient than a full scan.
        createCompoundIndex(CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'category': { '$gt': 'a' } }", "priority");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(1, scan.filters().size());
        assertEquals("category", scan.filters().getFirst().selector());
        assertEquals(Operator.GT, scan.filters().getFirst().op());
    }

    @Test
    void shouldPreferCompoundIndexOverSingleFieldIndexWhenSortByMatches() {
        // Behavior: When both a single-field index and a compound index exist,
        // the compound index should be preferred when sortByField matches.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_cat", "category", BsonType.STRING, false, IndexStatus.WAITING));
        createCompoundIndex(CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'category': 'bugs' }", "priority");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals("idx_cat_pri", scan.index().name());
    }

    @Test
    void shouldEmitCompoundIndexScanForAndWithSingleMatchAndSortBy() {
        // Behavior: In an AND query where only one child matches the compound index prefix,
        // the compound index should still be selected when sortByField covers the next field.
        createCompoundIndex(CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'category': 'bugs', 'status': 'open' }", "priority");

        assertInstanceOf(PhysicalAnd.class, result);
        PhysicalAnd and = (PhysicalAnd) result;
        boolean hasCompoundScan = and.children().stream()
                .anyMatch(c -> c instanceof PhysicalCompoundIndexScan);
        assertTrue(hasCompoundScan, "AND should contain a PhysicalCompoundIndexScan");
    }

    @Test
    void shouldEmitCompoundScanForSingleFilterEvenWithSortByOnThirdField() {
        // Behavior: A single EQ filter on the leading prefix field 'a' should use compound index.
        // Sort on 'c' is not provided by the index (next field is 'b'), but the compound scan
        // is still more efficient than a full scan.
        createCompoundIndex(CompoundIndexDefinition.create("idx_abc", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false),
                new CompoundIndexField("c", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1 }", "c");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(1, scan.filters().size());
        assertEquals("a", scan.filters().getFirst().selector());
    }

    @Test
    void shouldEmitCompoundIndexScanForMultipleEqPrefixWithSortBy() {
        // Behavior: Two EQ filters satisfy the minimum-2 requirement, and the compound index
        // also provides natural sort order on the third field when it matches sortByField.
        createCompoundIndex(CompoundIndexDefinition.create("idx_cat_stat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("status", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'category': 'bugs', 'status': 'open' }", "priority");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(2, scan.filters().size());
        assertEquals("category", scan.filters().get(0).selector());
        assertEquals("status", scan.filters().get(1).selector());
    }

    @Test
    void shouldSelectCompoundIndexMatchingSortByField() {
        // Behavior: When multiple compound indexes exist and only one has sortByField
        // as the next field after the EQ prefix, the matching compound index is selected.
        createCompoundIndex(CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING));
        createCompoundIndex(CompoundIndexDefinition.create("idx_cat_ts", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("created_at", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'category': 'bugs' }", "created_at");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals("idx_cat_ts", scan.index().name());
    }

    @Test
    void shouldEmitCompoundScanWhenSortByMatchesEqFilterField() {
        // Behavior: A single EQ filter on the leading prefix field should use compound index.
        // SortBy matching the same EQ field is trivially satisfied (all values identical).
        createCompoundIndex(CompoundIndexDefinition.create("idx_pri_cat", List.of(
                new CompoundIndexField("priority", BsonType.INT32, false),
                new CompoundIndexField("category", BsonType.STRING, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'priority': 5 }", "priority");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(1, scan.filters().size());
        assertEquals("priority", scan.filters().getFirst().selector());
    }

    @Test
    void shouldNotEmitCompoundIndexScanForInOperatorWithSortBy() {
        // Behavior: $in operator is not EQ, so compound index SORTBY optimization
        // does not apply. Even if it did, the sort across multiple prefix values would be wrong.
        createCompoundIndex(CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'category': { '$in': ['bugs', 'features'] } }", "priority");

        assertFalse(result instanceof PhysicalCompoundIndexScan);
    }

    @Test
    void shouldEmitCompoundIndexScanWhenMultipleCompoundsMatchSortBy() {
        // Behavior: When multiple compound indexes both have sortByField as the next
        // field after the EQ prefix, one of them is selected.
        createCompoundIndex(CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING));
        createCompoundIndex(CompoundIndexDefinition.create("idx_cat_pri_ts", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false),
                new CompoundIndexField("created_at", BsonType.INT64, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'category': 'bugs' }", "priority");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
    }

    // ==================== Leading prefix range scans ====================

    @Test
    void shouldEmitCompoundScanForSingleRangeOnFirstField() {
        // Behavior: Single range filter {a: {$gte: 20}} with compound(a,b) should produce
        // a compound index scan since it covers the leading prefix field.
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': { '$gte': 20 } }");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(1, scan.filters().size());
        assertEquals("a", scan.filters().getFirst().selector());
        assertEquals(Operator.GTE, scan.filters().getFirst().op());
    }

    @Test
    void shouldEmitCompoundScanForRangeOnFirstFieldPlusResidual() {
        // Behavior: AND(a>5, b=2) with compound(a,b) should produce a compound scan for a>5
        // with b=2 as residual. Range on first field stops prefix walk, so only a is matched.
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': { '$gt': 5 }, 'b': 2 }");

        assertInstanceOf(PhysicalAnd.class, result);
        PhysicalAnd and = (PhysicalAnd) result;
        assertEquals(2, and.children().size());
        assertInstanceOf(PhysicalCompoundIndexScan.class, and.children().get(0));
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) and.children().get(0);
        assertEquals(1, scan.filters().size());
        assertEquals("a", scan.filters().getFirst().selector());
        assertEquals(Operator.GT, scan.filters().getFirst().op());
    }

    // ============================================================================
    // Numeric Widening Tests
    // ============================================================================

    @Test
    void shouldEmitCompoundIndexScanWhenInt32PredicateMatchesInt64Field() {
        // Behavior: INT32 query literals (1, 2) should match INT64 compound index fields via lossless widening
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT64, false),
                new CompoundIndexField("b", BsonType.INT64, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'b': 2 }");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(2, scan.filters().size());
        assertEquals("a", scan.filters().get(0).selector());
        assertEquals("b", scan.filters().get(1).selector());
    }

    @Test
    void shouldEmitCompoundIndexScanWhenMixedNumericPredicatesMatchCompoundFields() {
        // Behavior: INT32 query literal should widen to DOUBLE for the second compound field
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT64, false),
                new CompoundIndexField("b", BsonType.DOUBLE, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'b': 2 }");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(2, scan.filters().size());
    }

    @Test
    void shouldNotEmitCompoundIndexScanWhenInt64PredicateMatchesDoubleField() {
        // Behavior: INT64 -> DOUBLE is lossy, compound index must NOT be selected
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.DOUBLE, false),
                new CompoundIndexField("b", BsonType.DOUBLE, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': { '$numberLong': '42' }, 'b': 1.5 }");

        // 'a' field has INT64 predicate vs DOUBLE index — lossy, breaks compound prefix
        assertFalse(result instanceof PhysicalCompoundIndexScan);
    }

    @Test
    void shouldEmitCompoundIndexScanWhenInt32RangePredicateMatchesInt64Field() {
        // Behavior: INT32 range predicate ($gt) should match INT64 compound index field
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT64, false),
                new CompoundIndexField("b", BsonType.INT64, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': 1, 'b': { '$gt': 10 } }");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(2, scan.filters().size());
        assertEquals(Operator.EQ, scan.filters().get(0).op());
        assertEquals(Operator.GT, scan.filters().get(1).op());
    }

    @Test
    void shouldEmitCompoundIndexScanWhenInt32RangeOnFirstFieldMatchesInt64() {
        // Behavior: INT32 range predicate ($gte) on the leading INT64 compound field should emit compound scan
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT64, false),
                new CompoundIndexField("b", BsonType.INT64, false)
        ), IndexStatus.WAITING));

        PhysicalNode result = planQuery("{ 'a': { '$gte': 10 } }");

        assertInstanceOf(PhysicalCompoundIndexScan.class, result);
        PhysicalCompoundIndexScan scan = (PhysicalCompoundIndexScan) result;
        assertEquals(1, scan.filters().size());
        assertEquals("a", scan.filters().getFirst().selector());
        assertEquals(Operator.GTE, scan.filters().getFirst().op());
    }

    @Test
    void shouldNotEmitCompoundIndexScanWhenInt64PredicateMatchesDoubleSecondField() {
        // Behavior: INT64 predicate on second DOUBLE compound field is lossy; compound index must
        // NOT be selected even though first field matches
        createCompoundIndex(CompoundIndexDefinition.create("idx_ab", List.of(
                new CompoundIndexField("a", BsonType.INT64, false),
                new CompoundIndexField("b", BsonType.DOUBLE, false)
        ), IndexStatus.WAITING));

        // field 'a': INT32 -> INT64 (valid), field 'b': INT64 -> DOUBLE (lossy)
        PhysicalNode result = planQuery("{ 'a': 1, 'b': { '$numberLong': '42' } }");

        assertFalse(result instanceof PhysicalCompoundIndexScan);
    }
}
