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
import com.kronotop.bucket.NumericWidening;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.logical.*;
import org.bson.BsonType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PhysicalPlanner {

    public PhysicalPlanner() {
    }

    private static BsonType bqlValueToBsonType(BqlValue bqlValue) {
        return switch (bqlValue) {
            case Int32Val ignored -> BsonType.INT32;
            case Int64Val ignored -> BsonType.INT64;
            case DoubleVal ignored -> BsonType.DOUBLE;
            case Decimal128Val ignored -> BsonType.DECIMAL128;
            case StringVal ignored -> BsonType.STRING;
            case BooleanVal ignored -> BsonType.BOOLEAN;
            case BinaryVal ignored -> BsonType.BINARY;
            case DateTimeVal ignored -> BsonType.DATE_TIME;
            case TimestampVal ignored -> BsonType.TIMESTAMP;
            case ObjectIdVal ignored -> BsonType.OBJECT_ID;
            default -> null;
        };
    }

    /**
     * Efficiently transposes a LogicalNode to PhysicalNode with minimal CPU and memory overhead.
     * <p>
     * Key optimizations:
     * - Direct field reuse between logical and physical nodes
     * - Singleton instance reuse for constants (True/False)
     * - In-place list transformation to avoid intermediate collections
     * - Pattern matching for efficient dispatch
     */
    public PhysicalNode plan(PlannerContext context, LogicalNode logicalPlan) {
        return plan(context, logicalPlan, null);
    }

    /**
     * Internal planning method that tracks parent $elemMatch selector for scalar array index lookup.
     *
     * @param elemMatchSelector the selector from parent $elemMatch (null if not inside $elemMatch)
     */
    private PhysicalNode plan(PlannerContext context, LogicalNode logicalPlan, String elemMatchSelector) {
        return switch (logicalPlan) {
            // Direct field transposition - zero-copy for primitive fields
            case LogicalFilter filter -> transposeFilter(context, filter, elemMatchSelector);

            // Efficient list transformation - reuse child structure
            case LogicalAnd and -> transposeAnd(context, and, elemMatchSelector);
            case LogicalOr or ->
                    new PhysicalOr(context.nextId(), transposeChildren(context, or.children(), elemMatchSelector));

            // Single child transposition
            case LogicalNot not -> new PhysicalNot(context.nextId(), plan(context, not.child(), elemMatchSelector));
            case LogicalElemMatch elemMatch -> new PhysicalElemMatch(
                    context.nextId(),
                    elemMatch.selector(),
                    // Pass the $elemMatch selector as context for scalar array index lookup
                    plan(context, elemMatch.subPlan(), elemMatch.selector())
            );

            // Singleton instance reuse - no object allocation
            case LogicalTrue ignored -> new PhysicalTrue(context.nextId());
            case LogicalFalse ignored -> new PhysicalFalse(context.nextId());
        };
    }

    /**
     * Transposes a LogicalAnd node, first attempting compound index matching before
     * falling back to individual child transposition.
     */
    private PhysicalNode transposeAnd(PlannerContext context, LogicalAnd and, String elemMatchSelector) {
        CompoundIndexMatchResult match = tryCompoundIndexMatch(context, and.children(), elemMatchSelector);
        if (match != null) {
            PhysicalCompoundIndexScan compoundScan = new PhysicalCompoundIndexScan(
                    context.nextId(), match.index, match.matchedFilters
            );
            if (match.remainingChildren.isEmpty()) {
                return compoundScan;
            }
            List<PhysicalNode> children = new ArrayList<>();
            children.add(compoundScan);
            for (LogicalNode remaining : match.remainingChildren) {
                children.add(plan(context, remaining, elemMatchSelector));
            }
            return new PhysicalAnd(context.nextId(), children);
        }
        return new PhysicalAnd(context.nextId(), transposeChildren(context, and.children(), elemMatchSelector));
    }

    /**
     * Attempts to match filter children against available compound indexes using the prefix rule.
     * Returns the best match (most matched fields), or null if no match found.
     * Normally requires minimum 2 matched filters; relaxes to 1 when sortByField covers the next index field.
     */
    private CompoundIndexMatchResult tryCompoundIndexMatch(PlannerContext context, List<LogicalNode> children, String elemMatchSelector) {
        CompoundIndexMatchResult best = null;

        for (CompoundIndex compoundIndex : context.getMetadata().compoundIndexes().getIndexes(IndexSelectionPolicy.READ)) {
            CompoundIndexDefinition definition = compoundIndex.definition();

            if (!isCollationCompatible(context.getCollation(), definition)) {
                continue;
            }

            List<CompoundIndexField> fields = definition.fields();

            List<PhysicalFilter> matchedFilters = new ArrayList<>();
            List<LogicalNode> matchedChildren = new ArrayList<>();

            for (CompoundIndexField field : fields) {
                List<LogicalFilter> fieldMatches = findMatchingFilters(children, matchedChildren, field, elemMatchSelector);
                if (fieldMatches.isEmpty()) {
                    break;
                }

                boolean hasRange = false;
                for (LogicalFilter filter : fieldMatches) {
                    matchedChildren.add(filter);
                    matchedFilters.add(new PhysicalFilter(
                            context.nextId(), filter.selector(), filter.op(), filter.operand()
                    ));
                    if (filter.op() != Operator.EQ) {
                        hasRange = true;
                    }
                }

                if (hasRange) {
                    break;
                }
            }

            int minRequired = computeMinRequiredFilters(fields, matchedFilters);
            if (matchedFilters.size() >= minRequired && isBetterMatch(context, best, matchedFilters, fields)) {
                List<LogicalNode> remaining = new ArrayList<>();
                for (LogicalNode child : children) {
                    if (!matchedChildren.contains(child)) {
                        remaining.add(child);
                    }
                }
                best = new CompoundIndexMatchResult(definition, matchedFilters, remaining);
            }
        }

        return best;
    }

    /**
     * Determines the minimum number of matched filters required for compound index selection.
     * Allows 1 when matched filters cover the leading prefix field, since a compound index
     * can serve range scans on the leading field via FoundationDB tuple ordering.
     */
    private int computeMinRequiredFilters(List<CompoundIndexField> fields, List<PhysicalFilter> matchedFilters) {
        if (matchedFilters.isEmpty()) {
            return 2;
        }
        // If matched filters cover the first compound index field, the index
        // can serve the query via a prefix range scan. Allow minimum 1.
        if (matchedFilters.getFirst().selector().equals(fields.getFirst().selector())) {
            return 1;
        }
        return 2;
    }

    /**
     * Checks whether the current candidate is a better match than the existing best.
     * More matched fields wins. On tie, prefer the index whose next field matches sortByField.
     */
    private boolean isBetterMatch(PlannerContext context, CompoundIndexMatchResult best,
                                  List<PhysicalFilter> matchedFilters, List<CompoundIndexField> fields) {
        if (best == null) {
            return true;
        }
        if (matchedFilters.size() != best.matchedFilters.size()) {
            return matchedFilters.size() > best.matchedFilters.size();
        }
        // Tiebreaker: prefer the index whose next field matches sortByField
        String sortByField = context.getSortByField();
        if (sortByField != null) {
            int next = matchedFilters.size();
            return next < fields.size()
                    && fields.get(next).selector().equals(sortByField);
        }
        return false;
    }

    /**
     * Finds LogicalFilter children matching a compound index field by selector, operator, and type.
     */
    private List<LogicalFilter> findMatchingFilters(List<LogicalNode> children, List<LogicalNode> alreadyMatched,
                                                    CompoundIndexField field, String elemMatchSelector) {
        List<LogicalFilter> result = new ArrayList<>();
        for (LogicalNode child : children) {
            if (alreadyMatched.contains(child)) {
                continue;
            }
            if (!(child instanceof LogicalFilter filter)) {
                continue;
            }
            if (!isCompoundIndexableOperator(filter.op())) {
                continue;
            }

            String filterSelector = resolveSelector(filter.selector(), elemMatchSelector);
            if (!filterSelector.equals(field.selector())) {
                continue;
            }
            if (!isOperandTypeMatch(filter.operand(), field.bsonType())) {
                continue;
            }
            result.add(filter);
        }
        return result;
    }

    /**
     * Resolves the effective selector for index lookup, considering $elemMatch context.
     */
    private String resolveSelector(String filterSelector, String elemMatchSelector) {
        if (elemMatchSelector == null) {
            return filterSelector;
        }
        if (filterSelector.isEmpty()) {
            return elemMatchSelector;
        }
        if (!filterSelector.contains(".")) {
            return elemMatchSelector + "." + filterSelector;
        }
        return filterSelector;
    }

    private boolean isCompoundIndexableOperator(Operator op) {
        return switch (op) {
            case EQ, GT, GTE, LT, LTE -> true;
            default -> false;
        };
    }

    /**
     * Converts a LogicalFilter to PhysicalIndexScan when an index exists and the operator/type
     * are compatible, otherwise falls back to PhysicalFullScan.
     * <p>
     * For $in operator with an index, transforms to PhysicalOr with multiple PhysicalIndexScan(EQ) nodes.
     * This enables the indexed $in queries by leveraging the existing UnionNode infrastructure.
     * Without an index, $in remains as-is and PredicateEvaluator handles it with proper array semantics.
     * <p>
     * For scalar array $elemMatch, when the filter's selector is empty but we're inside an $elemMatch
     * context, we use the $elemMatch selector for index lookup.
     *
     * @param elemMatchSelector the selector from parent $elemMatch (null if not inside $elemMatch)
     */
    private PhysicalNode transposeFilter(PlannerContext context, LogicalFilter filter, String elemMatchSelector) {
        // Build the index lookup selector based on $elemMatch context
        String indexLookupSelector = filter.selector();
        if (elemMatchSelector != null) {
            if (indexLookupSelector.isEmpty()) {
                // Scalar array $elemMatch: use the $elemMatch selector directly
                indexLookupSelector = elemMatchSelector;
            } else if (!indexLookupSelector.contains(".")) {
                // Document array $elemMatch with a simple field name: combine with $elemMatch selector
                // e.g., elemMatchSelector="items", filter.selector()="status" -> "items.status"
                indexLookupSelector = elemMatchSelector + "." + indexLookupSelector;
            }
            // If filter.selector() already contains a dot (nested path like "items.price"),
            // it's already fully qualified and shouldn't be combined
        }
        Index index = context.getMetadata().indexes().getIndex(indexLookupSelector, IndexSelectionPolicy.READ);

        if (index != null && !isCollationCompatible(context.getCollation(), index.definition())) {
            index = null;
        }

        // Handle empty $in: matches nothing regardless of index
        if (filter.op() == Operator.IN && filter.operand() instanceof List<?> list && list.isEmpty()) {
            return new PhysicalFalse(context.nextId());
        }

        // Handle empty $nin: matches everything regardless of index
        if (filter.op() == Operator.NIN && filter.operand() instanceof List<?> list && list.isEmpty()) {
            return new PhysicalTrue(context.nextId());
        }

        // Handle $in operator with index: transform to OR with multiple EQ index scans
        if (filter.op() == Operator.IN && index != null &&
                isOperandTypeMatch(filter.operand(), index.definition().bsonType())) {
            return transposeInToOr(filter, index, context);
        }

        // Handle $nin operator with index: transform to AND with multiple NE index scans
        // NOTE: Skip optimization for multi-key indexes because NE on multi-key indexes has
        // incorrect semantics for array fields (matches documents where AT LEAST ONE element
        // doesn't match, but $nin requires documents where NO element matches).
        if (filter.op() == Operator.NIN && index != null &&
                !index.definition().multiKey() &&
                isOperandTypeMatch(filter.operand(), index.definition().bsonType())) {
            return transposeNinToAnd(filter, index, context);
        }

        // Skip index scan for $ne on multi-key indexes - would return incorrect results
        // because index scan finds "any element != value" but $ne requires "no element == value"
        if (filter.op() == Operator.NE && index != null && index.definition().multiKey()) {
            PhysicalFilter node = new PhysicalFilter(
                    context.nextId(),
                    filter.selector(),
                    filter.op(),
                    filter.operand()
            );
            return new PhysicalFullScan(context.nextId(), node);
        }

        PhysicalFilter node = new PhysicalFilter(
                context.nextId(),
                filter.selector(),
                filter.op(),
                filter.operand()
        );

        if (index != null && PrimaryIndex.isPrimary(index.definition())) {
            // Index scan on the primary index.
            return new PhysicalIndexScan(context.nextId(), node, index.definition());
        }

        // Compound index for SORTBY: when filter is EQ and sortByField is set,
        // a compound index on (filterField, sortByField) provides both filtering
        // and natural sort order — better than single-field index + in-memory sort.
        if (context.getSortByField() != null && filter.op() == Operator.EQ) {
            CompoundIndexMatchResult compoundMatch = tryCompoundIndexMatch(
                    context, List.of(filter), elemMatchSelector);
            if (compoundMatch != null) {
                return new PhysicalCompoundIndexScan(
                        context.nextId(), compoundMatch.index, compoundMatch.matchedFilters);
            }
        }

        if (index != null &&
                isIndexableOperator(filter.op()) &&
                isOperandTypeMatch(filter.operand(), index.definition().bsonType())) {
            // Index available – use index scan with filter pushdown
            return new PhysicalIndexScan(context.nextId(), node, index.definition());
        }

        // Compound index fallback: when no single-field index exists, try using a compound
        // index whose leading prefix field matches this filter.
        if (isCompoundIndexableOperator(filter.op())) {
            CompoundIndexMatchResult compoundMatch = tryCompoundIndexMatch(
                    context, List.of(filter), elemMatchSelector);
            if (compoundMatch != null) {
                return new PhysicalCompoundIndexScan(
                        context.nextId(), compoundMatch.index, compoundMatch.matchedFilters);
            }
        }

        // Fallback: full scan
        return new PhysicalFullScan(context.nextId(), node);
    }

    /**
     * Transforms $in operator with an index into PhysicalOr with multiple PhysicalIndexScan(EQ) nodes.
     * <p>
     * Examples:
     * <ul>
     *   <li>{@code {'role': {'$in': ['admin', 'editor']}}} with index on 'role'
     *       → {@code PhysicalOr([PhysicalIndexScan(EQ, 'admin'), PhysicalIndexScan(EQ, 'editor')])}</li>
     *   <li>{@code {'role': {'$in': ['admin']}}} with index on 'role'
     *       → {@code PhysicalIndexScan(EQ, 'admin')} (single value optimization)</li>
     *   <li>{@code {'role': {'$in': []}}} with index on 'role'
     *       → {@code PhysicalFalse} (an empty list matches nothing)</li>
     * </ul>
     */
    @SuppressWarnings("unchecked")
    private PhysicalNode transposeInToOr(LogicalFilter filter, Index index, PlannerContext context) {
        List<BqlValue> values = (List<BqlValue>) filter.operand();

        // Empty $in matches nothing
        if (values.isEmpty()) {
            return new PhysicalFalse(context.nextId());
        }

        // Single value optimization: just use EQ directly
        if (values.size() == 1) {
            PhysicalFilter eqFilter = new PhysicalFilter(
                    context.nextId(),
                    filter.selector(),
                    Operator.EQ,
                    values.getFirst()
            );
            return new PhysicalIndexScan(context.nextId(), eqFilter, index.definition());
        }

        // Multiple values: create PhysicalOr with EQ index scans for each value
        List<PhysicalNode> indexScans = new ArrayList<>(values.size());
        for (BqlValue value : values) {
            PhysicalFilter eqFilter = new PhysicalFilter(
                    context.nextId(),
                    filter.selector(),
                    Operator.EQ,
                    value
            );
            indexScans.add(new PhysicalIndexScan(context.nextId(), eqFilter, index.definition()));
        }
        return new PhysicalOr(context.nextId(), indexScans);
    }

    /**
     * Transforms $nin operator with an index into PhysicalAnd with multiple PhysicalIndexScan(NE) nodes.
     * <p>
     * Examples:
     * <ul>
     *   <li>{@code {'role': {'$nin': ['admin', 'editor']}}} with index on 'role'
     *       → {@code PhysicalAnd([PhysicalIndexScan(NE, 'admin'), PhysicalIndexScan(NE, 'editor')])}</li>
     *   <li>{@code {'role': {'$nin': ['admin']}}} with index on 'role'
     *       → {@code PhysicalIndexScan(NE, 'admin')} (single value optimization)</li>
     *   <li>{@code {'role': {'$nin': []}}} with index on 'role'
     *       → {@code PhysicalTrue} (empty list matches everything)</li>
     * </ul>
     */
    @SuppressWarnings("unchecked")
    private PhysicalNode transposeNinToAnd(LogicalFilter filter, Index index, PlannerContext context) {
        List<BqlValue> values = (List<BqlValue>) filter.operand();

        // Empty $nin matches everything
        if (values.isEmpty()) {
            return new PhysicalTrue(context.nextId());
        }

        // Single value optimization: just use NE directly
        if (values.size() == 1) {
            PhysicalFilter neFilter = new PhysicalFilter(
                    context.nextId(),
                    filter.selector(),
                    Operator.NE,
                    values.getFirst()
            );
            return new PhysicalIndexScan(context.nextId(), neFilter, index.definition());
        }

        // Multiple values: create PhysicalAnd with NE index scans for each value
        List<PhysicalNode> indexScans = new ArrayList<>(values.size());
        for (BqlValue value : values) {
            PhysicalFilter neFilter = new PhysicalFilter(
                    context.nextId(),
                    filter.selector(),
                    Operator.NE,
                    value
            );
            indexScans.add(new PhysicalIndexScan(context.nextId(), neFilter, index.definition()));
        }
        return new PhysicalAnd(context.nextId(), indexScans);
    }

    /**
     * Returns true if the operator supports index scan execution.
     * Note: IN operator is handled specially in transposeFilter via transposeInToOr.
     * Note: NIN operator is handled specially in transposeFilter via transposeNinToAnd.
     */
    private boolean isIndexableOperator(Operator op) {
        return switch (op) {
            case EQ, NE, GT, GTE, LT, LTE -> true;
            default -> false;
        };
    }

    /**
     * Returns true if the operand type(s) match the index type. Handles both single values and lists.
     */
    private boolean isOperandTypeMatch(Object operand, BsonType bsonType) {
        if (operand instanceof BqlValue bqlValue) {
            return isTypeMatch(bqlValue, bsonType);
        }
        if (operand instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof BqlValue bqlValue && !isTypeMatch(bqlValue, bsonType)) {
                    return false;
                }
            }
            return !list.isEmpty();
        }
        return false;
    }

    /**
     * Returns true if the query predicate type matches the index type,
     * including lossless numeric widening (e.g. INT32 predicate matches INT64 index).
     * NullVal matches any index type.
     */
    private boolean isTypeMatch(BqlValue bqlValue, BsonType bsonType) {
        if (bqlValue instanceof NullVal) {
            return true;
        }
        BsonType sourceType = bqlValueToBsonType(bqlValue);
        if (sourceType == null) {
            return false;
        }
        return NumericWidening.canWiden(sourceType, bsonType);
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

    private boolean isCollationCompatible(Collation queryCollation, CompoundIndexDefinition definition) {
        if (queryCollation == null) {
            return true;
        }
        boolean hasStringField = definition.fields().stream()
                .anyMatch(f -> f.bsonType() == BsonType.STRING);
        if (!hasStringField) {
            return true;
        }
        return Objects.equals(queryCollation, definition.collation());
    }

    /**
     * Efficiently transposes a list of LogicalNodes to PhysicalNodes.
     * Pre-sizes the result list to avoid array reallocation.
     *
     * @param elemMatchSelector the selector from parent $elemMatch (null if not inside $elemMatch)
     */
    private List<PhysicalNode> transposeChildren(PlannerContext context, List<LogicalNode> children, String elemMatchSelector) {
        // Pre-size to avoid array growth and copying
        List<PhysicalNode> physicalChildren = new ArrayList<>(children.size());

        // Transform each child - compiler can optimize this loop
        for (LogicalNode child : children) {
            physicalChildren.add(plan(context, child, elemMatchSelector));
        }

        return physicalChildren;
    }

    private record CompoundIndexMatchResult(
            CompoundIndexDefinition index,
            List<PhysicalFilter> matchedFilters,
            List<LogicalNode> remainingChildren
    ) {
    }
}
