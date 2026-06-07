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

import com.kronotop.bucket.Collation;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.optimizer.Optimizer;
import com.kronotop.bucket.planner.logical.LogicalNode;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PhysicalPlanValidatorTest extends BasePhysicalPlannerTest {

    private final Optimizer optimizer = new Optimizer();

    private static Collation collation(String locale) {
        return Collation.create(locale, 3, null, null, null, null, null, null, null);
    }

    void planAndValidate(String bqlQuery, String sortByField, Collation collation) {
        BqlExpr expr = BqlParser.parse(bqlQuery);
        LogicalNode logicalPlan = logicalPlanner.plan(expr);
        PlannerContext context = new PlannerContext(metadata);
        if (sortByField != null) {
            context.setSortByField(sortByField);
        }
        if (collation != null) {
            context.setCollation(collation);
        }
        PhysicalNode physicalPlan = physicalPlanner.plan(context, logicalPlan);
        PhysicalNode optimizedPlan = optimizer.optimize(context, physicalPlan);
        PhysicalPlanValidator.validate(context, optimizedPlan);
    }

    void planAndValidate(String bqlQuery, String sortByField) {
        BqlExpr expr = BqlParser.parse(bqlQuery);
        LogicalNode logicalPlan = logicalPlanner.plan(expr);
        PlannerContext context = new PlannerContext(metadata);
        if (sortByField != null) {
            context.setSortByField(sortByField);
        }
        PhysicalNode physicalPlan = physicalPlanner.plan(context, logicalPlan);
        PhysicalNode optimizedPlan = optimizer.optimize(context, physicalPlan);
        PhysicalPlanValidator.validate(context, optimizedPlan);
    }

    void createCompoundIndex(CompoundIndexDefinition definition) {
        createIndexThenWaitForReadiness(definition);
        metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
    }

    void createSingleFieldIndex(SingleFieldIndexDefinition definition) {
        createIndexThenWaitForReadiness(definition);
        metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
    }

    // --- Pass cases ---

    @Test
    void shouldPassWhenNoSortByField() {
        // Behavior: No exception when sortByField is null — validation is skipped entirely.
        assertDoesNotThrow(() -> planAndValidate("{ 'category': 'bugs' }", null));
    }

    @Test
    void shouldPassWhenFullScanSortsByIdField() {
        // Behavior: FullScan provides natural ordering by _id, so SORTBY _id always passes.
        assertDoesNotThrow(() -> planAndValidate("{ 'category': 'bugs' }", "_id"));
    }

    @Test
    void shouldPassWhenIndexScanMatchesSortField() {
        // Behavior: IndexScan on a field provides natural ordering on that field.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_age", "age", BsonType.INT32, false, IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ 'age': { '$gte': 0 } }", "age"));
    }

    @Test
    void shouldPassWhenRangeScanMatchesSortField() {
        // Behavior: RangeScan on a field provides natural ordering on that field.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_age", "age", BsonType.INT32, false, IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ 'age': { '$gt': 0, '$lt': 100 } }", "age"));
    }

    @Test
    void shouldPassWhenCompoundIndexCoversSortFieldWithEqPrefix() {
        // Behavior: Compound index {category, priority} with EQ on category preserves
        // natural ordering of priority — SORTBY priority passes.
        createCompoundIndex(CompoundIndexDefinition.create("idx_cat_pri", List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("priority", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ 'category': 'bugs' }", "priority"));
    }

    @Test
    void shouldPassWhenCompoundIndexSortFieldIsLastWithAllEqPrefixes() {
        // Behavior: Compound index {a, b, c} with EQ on a and b preserves ordering of c.
        createCompoundIndex(CompoundIndexDefinition.create("idx_abc", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false),
                new CompoundIndexField("c", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ 'a': 1, 'b': 2 }", "c"));
    }

    @Test
    void shouldPassWhenAndChildScanMatchesSortField() {
        // Behavior: An $and query passes when one child is a scan matching the sort field.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_age", "age", BsonType.INT32, false, IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate(
                "{ '$and': [{ 'status': 'active' }, { 'age': { '$gte': 0 } }] }", "age"));
    }

    @Test
    void shouldPassWhenTrueNodeAndSortFieldIndexExists() {
        // Behavior: PhysicalTrue (from empty $and) passes when a sort-field index exists
        // because PipelineRewriter converts it to a RangeScan using that index.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_age", "age", BsonType.INT32, false, IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ '$and': [] }", "age"));
    }

    @Test
    void shouldRejectWhenPlanDoesNotUseSortFieldIndex() {
        // Behavior: Having an index on the sort field is not enough — the execution plan
        // must actually provide ordering on that field. A FullScan on an unindexed field
        // iterates by _id, not by the sort field.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_priority", "priority", BsonType.INT32, false, IndexStatus.WAITING));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'category': 'bugs' }", "priority"));
        assertTrue(ex.getMessage().contains("SORTBY 'priority'"));
        assertTrue(ex.getMessage().contains("RESULTSORT"));
    }

    @Test
    void shouldRejectWhenFullScanDoesNotProvideSortOrder() {
        // Behavior: A filter on an unindexed field produces a FullScan on _id. The sort
        // field index exists but is not used by the plan, so SORTBY is rejected.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_score", "score", BsonType.INT32, false, IndexStatus.WAITING));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'age': { '$gt': 20 } }", "score"));
        assertTrue(ex.getMessage().contains("SORTBY 'score'"));
        assertTrue(ex.getMessage().contains("RESULTSORT"));
    }

    @Test
    void shouldRejectWhenOrNodeDoesNotProvideSortOrder() {
        // Behavior: PhysicalOr scans role and status indexes — neither provides ordering
        // on salary. The salary index exists but is not part of the plan.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_role", "role", BsonType.STRING, false, IndexStatus.WAITING));
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_status", "status", BsonType.STRING, false, IndexStatus.WAITING));
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_salary", "salary", BsonType.INT32, false, IndexStatus.WAITING));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ '$or': [{'role': 'admin'}, {'status': 'active'}] }", "salary"));
        assertTrue(ex.getMessage().contains("SORTBY 'salary'"));
        assertTrue(ex.getMessage().contains("RESULTSORT"));
    }

    @Test
    void shouldRejectWhenCompoundIndexNotUsedAndSingleFieldIndexNotInPlan() {
        // Behavior: Compound index {age, score} is not used because the leading prefix (age)
        // is not filtered. The plan is a FullScan on score. A single-field index on age
        // exists but is not part of the execution plan, so SORTBY age is rejected.
        createCompoundIndex(CompoundIndexDefinition.create("idx_age_score", List.of(
                new CompoundIndexField("age", BsonType.INT32, false),
                new CompoundIndexField("score", BsonType.INT32, false)
        ), IndexStatus.WAITING));
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_age", "age", BsonType.INT32, false, IndexStatus.WAITING));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'score': { '$gt': 50 } }", "age"));
        assertTrue(ex.getMessage().contains("SORTBY 'age'"));
        assertTrue(ex.getMessage().contains("RESULTSORT"));
    }

    // --- Reject with "create an index" ---

    @Test
    void shouldRejectFullScanWhenNoSortFieldIndex() {
        // Behavior: FullScan with SORTBY on a non-_id field is rejected when no index
        // on the sort field exists. The hint suggests creating an index.
        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'category': 'bugs' }", "priority")
        );
        assertTrue(ex.getMessage().contains("create an index on 'priority'"));
    }

    @Test
    void shouldRejectIndexScanWhenSortFieldHasNoIndex() {
        // Behavior: IndexScan on 'category' with SORTBY 'priority' is rejected when
        // no index on 'priority' exists.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_cat", "category", BsonType.STRING, false, IndexStatus.WAITING));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'category': 'bugs' }", "priority")
        );
        assertTrue(ex.getMessage().contains("create an index on 'priority'"));
    }

    @Test
    void shouldRejectCompoundIndexWhenSortFieldNotInIndexAndNoSeparateIndex() {
        // Behavior: Compound index {a, b} with SORTBY 'score' is rejected when 'score'
        // is not in the index and no separate index exists.
        createCompoundIndex(CompoundIndexDefinition.create("idx_a_b", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'a': 1, 'b': 2 }", "score")
        );
        assertTrue(ex.getMessage().contains("create an index on 'score'"));
    }

    @Test
    void shouldRejectOrNodeWhenNoSortFieldIndex() {
        // Behavior: PhysicalOr does not provide natural ordering. Without a sort-field
        // index, the hint suggests creating one.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_role", "role", BsonType.STRING, false, IndexStatus.WAITING));
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_status", "status", BsonType.STRING, false, IndexStatus.WAITING));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ '$or': [{'role': 'admin'}, {'status': 'active'}] }", "salary")
        );
        assertTrue(ex.getMessage().contains("create an index on 'salary'"));
    }

    @Test
    void shouldRejectTrueNodeWhenNoSortFieldIndex() {
        // Behavior: PhysicalTrue (from empty $and) is rejected when no sort-field index exists.
        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ '$and': [] }", "x")
        );
        assertTrue(ex.getMessage().contains("create an index on 'x'"));
    }

    // --- Compound index prefix range edge cases ---

    @Test
    void shouldRejectThreeFieldCompoundWhenMiddleRangeAndNoSeparateIndex() {
        // Behavior: Compound index {a, b, c} with EQ on a, range on b, SORTBY c —
        // range on b breaks ordering of c. No separate index on c → "create an index".
        createCompoundIndex(CompoundIndexDefinition.create("idx_abc", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false),
                new CompoundIndexField("c", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'a': 1, 'b': { '$lt': 10 } }", "c")
        );
        assertTrue(ex.getMessage().contains("create an index on 'c'"));
    }

    @Test
    void shouldRejectWhenNoSortFieldIndex() {
        // Behavior: No index on the sort field always rejects with "create an index" hint.
        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'category': 'bugs' }", "priority")
        );
        assertTrue(ex.getMessage().contains("create an index on 'priority'"));
    }

    @Test
    void shouldPassCompoundIndexWhenSortFieldMatchesEqPrefix() {
        // Behavior: Compound index {age, score} with EQ on age and SORTBY score —
        // EQ prefix preserves trailing-field ordering.
        createCompoundIndex(CompoundIndexDefinition.create("idx_age_score", List.of(
                new CompoundIndexField("age", BsonType.INT32, false),
                new CompoundIndexField("score", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ 'age': 25 }", "score"));
    }

    @Test
    void shouldPassWhenCompoundIndexSortFieldIsFirstField() {
        // Behavior: Compound index {a, b} with SORTBY a — sort field is the first field,
        // so there are no prefix fields to check. Ordering is always preserved.
        createCompoundIndex(CompoundIndexDefinition.create("idx_a_b", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ 'a': 1 }", "a"));
    }

    @Test
    void shouldRejectTwoFieldCompoundWhenFirstFieldHasRangeAndSortBySecond() {
        // Behavior: Compound index {a, b} with range on a and SORTBY b — range on the
        // first field breaks the ordering of the second field. No separate index on b
        // exists, so the hint suggests creating one.
        createCompoundIndex(CompoundIndexDefinition.create("idx_a_b", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'a': { '$gt': 5 } }", "b")
        );
        assertTrue(ex.getMessage().contains("create an index on 'b'"));
    }

    @Test
    void shouldRejectWhenSortFieldInCompoundButLeadingPrefixNotFiltered() {
        // Behavior: Compound index {age, score} with filter only on score (second field)
        // and SORTBY age — planner cannot use the compound index without a filter on the
        // leading prefix, so plan falls back to FullScan. No single-field index on age
        // exists, so the hint suggests creating one.
        createCompoundIndex(CompoundIndexDefinition.create("idx_age_score", List.of(
                new CompoundIndexField("age", BsonType.INT32, false),
                new CompoundIndexField("score", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'score': { '$gt': 50 } }", "age")
        );
        assertTrue(ex.getMessage().contains("create an index on 'age'"));
    }

    // --- Compound index: prefix scans with residual filtering ---

    @Test
    void shouldPassWhenCompoundIndexAllEqFilters() {
        // Behavior: Compound index (a, b) with all EQ filters — no range on prefix,
        // no residual filtering needed.
        createCompoundIndex(CompoundIndexDefinition.create("idx_a_b", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ 'a': 1, 'b': 2 }", null));
    }

    @Test
    void shouldPassWhenCompoundRangeOnLastFieldNoTrailing() {
        // Behavior: Compound index (a, b) with EQ on a and range on b — range is on
        // the last field, so there are no trailing fields to filter residually.
        createCompoundIndex(CompoundIndexDefinition.create("idx_a_b", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ 'a': 1, 'b': { '$gt': 5 } }", null));
    }

    @Test
    void shouldPassWhenCompoundRangeNoResidualOnTrailingField() {
        // Behavior: Compound index (a, b) with range on a but no filter on b —
        // no residual on trailing field, just a prefix range scan.
        createCompoundIndex(CompoundIndexDefinition.create("idx_a_b", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ 'a': { '$gt': 5 } }", null));
    }

    @Test
    void shouldPassWhenCompoundRangeOnFirstWithResidualEqOnSecond() {
        // Behavior: Compound index (a, b) with range on a and EQ on b — scan covers
        // only a, and b=2 becomes a residual filter. This is allowed unconditionally.
        createCompoundIndex(CompoundIndexDefinition.create("idx_a_b", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ 'a': { '$gt': 5 }, 'b': 2 }", null));
    }

    @Test
    void shouldPassWhenCompoundRangeOnMiddleWithResidualOnLast() {
        // Behavior: Compound index (a, b, c) with EQ on a, range on b, EQ on c —
        // scan covers (a, b) but c becomes residual after the range on b. Allowed.
        createCompoundIndex(CompoundIndexDefinition.create("idx_abc", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false),
                new CompoundIndexField("c", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ 'a': 1, 'b': { '$gt': 5 }, 'c': 3 }", null));
    }

    @Test
    void shouldPassWhenCompoundRangeGteWithResidual() {
        // Behavior: $gte is also a range operator — residual detection applies but is allowed.
        createCompoundIndex(CompoundIndexDefinition.create("idx_a_b", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ 'a': { '$gte': 5 }, 'b': 2 }", null));
    }

    @Test
    void shouldPassWhenCompoundRangeLtWithResidual() {
        // Behavior: $lt is also a range operator — residual detection applies but is allowed.
        createCompoundIndex(CompoundIndexDefinition.create("idx_a_b", List.of(
                new CompoundIndexField("a", BsonType.INT32, false),
                new CompoundIndexField("b", BsonType.INT32, false)
        ), IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ 'a': { '$lt': 10 }, 'b': 2 }", null));
    }

    // --- Collation mismatch with hasSortFieldIndex ---

    @Test
    void shouldRejectSortByWhenCollationMismatchOnStringIndex() {
        // Behavior: STRING index with collation "en" cannot provide sort ordering when
        // query specifies collation "tr" — validation rejects with collation mismatch error.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_name", "name", BsonType.STRING, false, IndexStatus.WAITING, collation("en")));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'category': 'bugs' }", "name", collation("tr"))
        );
        assertTrue(ex.getMessage().contains("collation does not match the query collation"));
        assertTrue(ex.getMessage().contains("use a collation that matches the index"));
    }

    @Test
    void shouldRejectSortByWhenPlanDoesNotProvideOrderingDespiteCollationMatch() {
        // Behavior: Even though the name index has a matching collation, the plan for
        // {category: 'bugs'} is a FullScan on _id — it does not use the name index.
        // Validation rejects because the execution plan cannot provide sort ordering.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_name", "name", BsonType.STRING, false, IndexStatus.WAITING, collation("en")));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'category': 'bugs' }", "name", collation("en"))
        );
        assertTrue(ex.getMessage().contains("SORTBY 'name'"));
        assertTrue(ex.getMessage().contains("RESULTSORT"));
    }

    @Test
    void shouldRejectSortByWhenPlanDoesNotProvideOrderingWithNullCollation() {
        // Behavior: A null query collation does not bypass validation. The plan for
        // {category: 'bugs'} is a FullScan — the name index is not used regardless
        // of collation compatibility.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_name", "name", BsonType.STRING, false, IndexStatus.WAITING, collation("en")));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'category': 'bugs' }", "name", null)
        );
        assertTrue(ex.getMessage().contains("SORTBY 'name'"));
        assertTrue(ex.getMessage().contains("RESULTSORT"));
    }

    @Test
    void shouldRejectSortByWhenPlanDoesNotProvideOrderingOnNonStringIndex() {
        // Behavior: Even though INT32 indexes are unaffected by collation, the plan for
        // {category: 'bugs'} is a FullScan — the score index is not used for ordering.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_score", "score", BsonType.INT32, false, IndexStatus.WAITING));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'category': 'bugs' }", "score", collation("tr"))
        );
        assertTrue(ex.getMessage().contains("SORTBY 'score'"));
        assertTrue(ex.getMessage().contains("RESULTSORT"));
    }

    @Test
    void shouldRejectSortByWhenStringIndexHasNullCollationButQueryHasCollation() {
        // Behavior: STRING index with binary ordering (null collation) cannot serve
        // locale-aware sort ordering — validation rejects with collation mismatch error.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_name", "name", BsonType.STRING, false, IndexStatus.WAITING));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ 'category': 'bugs' }", "name", collation("en"))
        );
        assertTrue(ex.getMessage().contains("collation does not match the query collation"));
        assertTrue(ex.getMessage().contains("use a collation that matches the index"));
    }

    // --- Collation mismatch with providesSortOrder(PhysicalTrue) ---

    @Test
    void shouldRejectTrueNodeSortByWhenCollationMismatch() {
        // Behavior: PhysicalTrue (empty $and) with collation-mismatched STRING index
        // cannot provide sort ordering — validation rejects with collation mismatch error.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_name", "name", BsonType.STRING, false, IndexStatus.WAITING, collation("en")));

        PhysicalPlanValidationException ex = assertThrows(PhysicalPlanValidationException.class,
                () -> planAndValidate("{ '$and': [] }", "name", collation("tr"))
        );
        assertTrue(ex.getMessage().contains("collation does not match the query collation"));
        assertTrue(ex.getMessage().contains("use a collation that matches the index"));
    }

    @Test
    void shouldPassTrueNodeSortByWhenCollationMatches() {
        // Behavior: PhysicalTrue with matching collation on STRING index — sort is valid.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_name", "name", BsonType.STRING, false, IndexStatus.WAITING, collation("en")));

        assertDoesNotThrow(() -> planAndValidate("{ '$and': [] }", "name", collation("en")));
    }

    @Test
    void shouldPassTrueNodeSortByWithNullQueryCollation() {
        // Behavior: No query collation means any index is usable (backward compat).
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_name", "name", BsonType.STRING, false, IndexStatus.WAITING, collation("en")));

        assertDoesNotThrow(() -> planAndValidate("{ '$and': [] }", "name", null));
    }

    @Test
    void shouldPassTrueNodeSortByOnNonStringIndexWithCollation() {
        // Behavior: Non-string (INT32) index is unaffected by query collation in PhysicalTrue path.
        createSingleFieldIndex(SingleFieldIndexDefinition.create(
                "idx_age", "age", BsonType.INT32, false, IndexStatus.WAITING));

        assertDoesNotThrow(() -> planAndValidate("{ '$and': [] }", "age", collation("tr")));
    }
}
