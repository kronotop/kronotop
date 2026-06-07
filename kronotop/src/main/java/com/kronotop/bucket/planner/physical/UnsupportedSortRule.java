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
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.planner.Operator;
import org.bson.BsonType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Validates that the physical plan can produce results ordered by the requested SORTBY field.
 * Rejects queries where no natural ordering is available, guiding the user toward
 * creating an index on the sort field.
 */
public class UnsupportedSortRule implements PhysicalPlanValidationRule {

    @Override
    public String getName() {
        return "UnsupportedSortRule";
    }

    @Override
    public List<Violation> check(PlannerContext context, PhysicalNode plan) {
        String sortByField = context.getSortByField();
        if (sortByField == null) {
            return List.of();
        }

        if (providesSortOrder(context, plan, sortByField)) {
            return List.of();
        }

        String rangePrefixField = findCompoundRangePrefixConflict(context, plan, sortByField);
        if (rangePrefixField != null) {
            return List.of(new Violation(
                    "SORTBY '" + sortByField + "' cannot be executed because a range filter on '"
                            + rangePrefixField + "' breaks compound index ordering",
                    "use an equality filter on '" + rangePrefixField
                            + "' or create an index on '" + sortByField + "'"));
        }

        Index index = context.getMetadata().indexes().getIndex(sortByField, IndexSelectionPolicy.READ);
        if (index != null) {
            if (!isCollationCompatible(context.getCollation(), index.definition())) {
                return List.of(new Violation(
                        "SORTBY '" + sortByField + "' cannot use the existing index because its collation does not match the query collation",
                        "use a collation that matches the index on '" + sortByField + "'"));
            }
            return List.of(new Violation(
                    "SORTBY '" + sortByField + "' cannot be used because the query's execution plan does not provide natural ordering on this field",
                    "use RESULTSORT '" + sortByField + "' for in-memory sorting"));
        }

        return List.of(new Violation(
                "SORTBY '" + sortByField + "' requires an index that provides natural ordering",
                "create an index on '" + sortByField + "'"));
    }

    private boolean providesSortOrder(PlannerContext context, PhysicalNode node, String sortByField) {
        return switch (node) {
            case PhysicalFullScan ignored -> PrimaryIndex.SELECTOR.equals(sortByField);
            case PhysicalIndexScan scan -> scan.index().selector().equals(sortByField);
            case PhysicalRangeScan scan -> scan.selector().equals(sortByField);
            case PhysicalCompoundIndexScan scan -> compoundIndexProvidesSortOrder(scan, sortByField);
            case PhysicalAnd and -> hasChildWithSortOrder(context, and, sortByField);
            case PhysicalTrue ignored -> {
                Index index = context.getMetadata().indexes().getIndex(sortByField, IndexSelectionPolicy.READ);
                yield index != null && isCollationCompatible(context.getCollation(), index.definition());
            }
            case PhysicalOr or -> allChildrenAreEqScansOnField(or.children(), sortByField);
            case PhysicalNot ignored -> false;
            case PhysicalFilter ignored -> false;
            case PhysicalElemMatch ignored -> false;
            case PhysicalFalse ignored -> false;
            case PhysicalIndexIntersection ignored -> false;
        };
    }

    private boolean allChildrenAreEqScansOnField(List<PhysicalNode> children, String sortByField) {
        for (PhysicalNode child : children) {
            if (!(child instanceof PhysicalIndexScan scan)) return false;
            if (!scan.index().selector().equals(sortByField)) return false;
            if (!(scan.node() instanceof PhysicalFilter filter)) return false;
            if (filter.op() != Operator.EQ) return false;
        }
        return !children.isEmpty();
    }

    private boolean hasChildWithSortOrder(PlannerContext context, PhysicalAnd and, String sortByField) {
        for (PhysicalNode child : and.children()) {
            if (providesSortOrder(context, child, sortByField)) {
                return true;
            }
        }
        return false;
    }

    private boolean compoundIndexProvidesSortOrder(PhysicalCompoundIndexScan scan, String sortByField) {
        List<CompoundIndexField> fields = scan.index().fields();

        int sortFieldPos = -1;
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).selector().equals(sortByField)) {
                sortFieldPos = i;
                break;
            }
        }

        if (sortFieldPos == -1) {
            return false;
        }

        // All prefix fields before the sort field must have EQ-only filters.
        // A range filter on a prefix field breaks the trailing-field ordering.
        for (int i = 0; i < sortFieldPos; i++) {
            String prefixSelector = fields.get(i).selector();
            for (PhysicalFilter filter : scan.filters()) {
                if (filter.selector().equals(prefixSelector) && isRangeOperator(filter.op())) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Checks available compound indexes for the case where a compound index contains the
     * sortByField but a range filter on a prefix field breaks the ordering guarantee.
     * Returns the conflicting prefix field name, or null if no such conflict exists.
     */
    private String findCompoundRangePrefixConflict(PlannerContext context, PhysicalNode plan, String sortByField) {
        List<PhysicalFilter> filters = new ArrayList<>();
        collectFilters(plan, filters);

        for (CompoundIndex compoundIndex : context.getMetadata().compoundIndexes().getIndexes(IndexSelectionPolicy.READ)) {
            List<CompoundIndexField> fields = compoundIndex.definition().fields();

            int sortFieldPos = -1;
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).selector().equals(sortByField)) {
                    sortFieldPos = i;
                    break;
                }
            }

            if (sortFieldPos == -1) {
                continue;
            }

            for (int i = 0; i < sortFieldPos; i++) {
                String prefixSelector = fields.get(i).selector();
                for (PhysicalFilter filter : filters) {
                    if (filter.selector().equals(prefixSelector) && isRangeOperator(filter.op())) {
                        return prefixSelector;
                    }
                }
            }
        }

        return null;
    }

    private void collectFilters(PhysicalNode node, List<PhysicalFilter> filters) {
        switch (node) {
            case PhysicalFilter filter -> filters.add(filter);
            case PhysicalFullScan scan -> collectFilters(scan.node(), filters);
            case PhysicalAnd and -> {
                for (PhysicalNode child : and.children()) {
                    collectFilters(child, filters);
                }
            }
            case PhysicalCompoundIndexScan scan -> filters.addAll(scan.filters());
            default -> {
            }
        }
    }

    private boolean isRangeOperator(Operator op) {
        return op == Operator.GT || op == Operator.GTE || op == Operator.LT || op == Operator.LTE;
    }

    private boolean isCollationCompatible(Collation queryCollation, SingleFieldIndexDefinition definition) {
        if (queryCollation == null) {
            return true;
        }
        if (definition.bsonType() != BsonType.STRING) {
            return true;
        }
        return Objects.equals(queryCollation, definition.collation());
    }
}
