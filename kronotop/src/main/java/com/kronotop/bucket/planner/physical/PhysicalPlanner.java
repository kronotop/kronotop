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
        return switch (logicalPlan) {
            // Direct field transposition - zero-copy for primitive fields
            case LogicalFilter filter -> transposeFilter(metadata, filter, context);

            // Efficient list transformation - reuse child structure
            case LogicalAnd and ->
                    new PhysicalAnd(context.nextId(), transposeChildren(metadata, and.children(), context));
            case LogicalOr or -> new PhysicalOr(context.nextId(), transposeChildren(metadata, or.children(), context));

            // Single child transposition
            case LogicalNot not -> new PhysicalNot(context.nextId(), plan(metadata, not.child(), context));
            case LogicalElemMatch elemMatch -> new PhysicalElemMatch(
                    context.nextId(),
                    elemMatch.selector(),
                    plan(metadata, elemMatch.subPlan(), context)
            );

            // Singleton instance reuse - no object allocation
            case LogicalTrue ignored -> new PhysicalTrue(context.nextId());
            case LogicalFalse ignored -> new PhysicalFalse(context.nextId());
        };
    }

    /**
     * Converts a LogicalFilter to PhysicalIndexScan when an index exists and the operator/type
     * are compatible, otherwise falls back to PhysicalFullScan.
     */
    private PhysicalNode transposeFilter(BucketMetadata metadata, LogicalFilter filter, PlannerContext context) {
        Index index = metadata.indexes().getIndex(filter.selector(), IndexSelectionPolicy.READ);

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

        if (index != null
                && isIndexableOperator(filter.op())
                && filter.operand() instanceof BqlValue bqlValue
                && isTypeMatch(bqlValue, index.definition().bsonType())) {

            // Index available – use index scan with filter pushdown
            return new PhysicalIndexScan(context.nextId(), node, index.definition());
        }

        // Fallback: full scan
        return new PhysicalFullScan(context.nextId(), node);
    }

    /**
     * Returns true if the operator supports index scan execution.
     */
    private boolean isIndexableOperator(Operator op) {
        return switch (op) {
            case EQ, NE, GT, GTE, LT, LTE -> true;
            default -> false;
        };
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
     */
    private List<PhysicalNode> transposeChildren(BucketMetadata metadata, List<LogicalNode> children, PlannerContext context) {
        // Pre-size to avoid array growth and copying
        List<PhysicalNode> physicalChildren = new ArrayList<>(children.size());

        // Transform each child - compiler can optimize this loop
        for (LogicalNode child : children) {
            physicalChildren.add(plan(metadata, child, context));
        }

        return physicalChildren;
    }
}
