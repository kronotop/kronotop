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

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.logical.*;
import org.bson.BsonType;

import java.util.ArrayList;
import java.util.List;

public class PhysicalPlanner {

    public PhysicalPlanner() {
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
    public PhysicalNode plan(BucketMetadata metadata, LogicalNode logicalPlan, PlannerContext context) {
        return plan(metadata, logicalPlan, context, null);
    }

    /**
     * Internal planning method that tracks parent $elemMatch selector for scalar array index lookup.
     *
     * @param elemMatchSelector the selector from parent $elemMatch (null if not inside $elemMatch)
     */
    private PhysicalNode plan(BucketMetadata metadata, LogicalNode logicalPlan, PlannerContext context, String elemMatchSelector) {
        return switch (logicalPlan) {
            // Direct field transposition - zero-copy for primitive fields
            case LogicalFilter filter -> transposeFilter(metadata, filter, context, elemMatchSelector);

            // Efficient list transformation - reuse child structure
            case LogicalAnd and ->
                    new PhysicalAnd(context.nextId(), transposeChildren(metadata, and.children(), context, elemMatchSelector));
            case LogicalOr or -> new PhysicalOr(context.nextId(), transposeChildren(metadata, or.children(), context, elemMatchSelector));

            // Single child transposition
            case LogicalNot not -> new PhysicalNot(context.nextId(), plan(metadata, not.child(), context, elemMatchSelector));
            case LogicalElemMatch elemMatch -> new PhysicalElemMatch(
                    context.nextId(),
                    elemMatch.selector(),
                    // Pass the $elemMatch selector as context for scalar array index lookup
                    plan(metadata, elemMatch.subPlan(), context, elemMatch.selector())
            );

            // Singleton instance reuse - no object allocation
            case LogicalTrue ignored -> new PhysicalTrue(context.nextId());
            case LogicalFalse ignored -> new PhysicalFalse(context.nextId());
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
    private PhysicalNode transposeFilter(BucketMetadata metadata, LogicalFilter filter, PlannerContext context, String elemMatchSelector) {
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
        Index index = metadata.indexes().getIndex(indexLookupSelector, IndexSelectionPolicy.READ);

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
                index.definition().id() != DefaultIndexDefinition.ID.id() &&
                isOperandTypeMatch(filter.operand(), index.definition().bsonType())) {
            return transposeInToOr(filter, index, context);
        }

        // Handle $nin operator with index: transform to AND with multiple NE index scans
        // NOTE: Skip optimization for multi-key indexes because NE on multi-key indexes has
        // incorrect semantics for array fields (matches documents where AT LEAST ONE element
        // doesn't match, but $nin requires documents where NO element matches).
        if (filter.op() == Operator.NIN && index != null &&
                index.definition().id() != DefaultIndexDefinition.ID.id() &&
                !index.definition().multiKey() &&
                isOperandTypeMatch(filter.operand(), index.definition().bsonType())) {
            return transposeNinToAnd(filter, index, context);
        }

        PhysicalFilter node = new PhysicalFilter(
                context.nextId(),
                filter.selector(),
                filter.op(),
                filter.operand()
        );

        if (index != null && index.definition().id() == DefaultIndexDefinition.ID.id()) {
            // Index scan on the primary index.
            return new PhysicalIndexScan(context.nextId(), node, index.definition());
        }

        if (index != null &&
                isIndexableOperator(filter.op()) &&
                isOperandTypeMatch(filter.operand(), index.definition().bsonType())) {
            // Index available – use index scan with filter pushdown
            return new PhysicalIndexScan(context.nextId(), node, index.definition());
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
     *       → {@code PhysicalFalse} (empty list matches nothing)</li>
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
            case EQ, NE, GT, GTE, LT, LTE, ALL -> true;
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
     * Returns true if the query predicate type matches the index type. NullVal matches any index type.
     */
    private boolean isTypeMatch(BqlValue bqlValue, BsonType bsonType) {
        return switch (bqlValue) {
            case DoubleVal ignored -> bsonType == BsonType.DOUBLE;
            case StringVal ignored -> bsonType == BsonType.STRING;
            case BinaryVal ignored -> bsonType == BsonType.BINARY;
            case BooleanVal ignored -> bsonType == BsonType.BOOLEAN;
            case DateTimeVal ignored -> bsonType == BsonType.DATE_TIME;
            case Int32Val ignored -> bsonType == BsonType.INT32;
            case TimestampVal ignored -> bsonType == BsonType.TIMESTAMP;
            case Int64Val ignored -> bsonType == BsonType.INT64;
            case Decimal128Val ignored -> bsonType == BsonType.DECIMAL128;
            case NullVal ignored -> true;
            default -> false;
        };
    }

    /**
     * Efficiently transposes a list of LogicalNodes to PhysicalNodes.
     * Pre-sizes the result list to avoid array reallocation.
     *
     * @param elemMatchSelector the selector from parent $elemMatch (null if not inside $elemMatch)
     */
    private List<PhysicalNode> transposeChildren(BucketMetadata metadata, List<LogicalNode> children, PlannerContext context, String elemMatchSelector) {
        // Pre-size to avoid array growth and copying
        List<PhysicalNode> physicalChildren = new ArrayList<>(children.size());

        // Transform each child - compiler can optimize this loop
        for (LogicalNode child : children) {
            physicalChildren.add(plan(metadata, child, context, elemMatchSelector));
        }

        return physicalChildren;
    }
}
