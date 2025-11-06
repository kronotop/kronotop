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

import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import com.kronotop.bucket.planner.physical.*;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;

/**
 * Base test class for optimizer integration tests.
 * Provides helper methods for creating test scenarios and verifying optimizations.
 */
class BaseOptimizerTest extends BaseStandaloneInstanceTest {
    protected Optimizer optimizer;
    protected PhysicalPlanner physicalPlanner;
    protected LogicalPlanner logicalPlanner;
    protected BucketMetadata metadata;

    @BeforeEach
    void setUp() {
        optimizer = new Optimizer();
        physicalPlanner = new PhysicalPlanner();
        logicalPlanner = new LogicalPlanner();
        metadata = getBucketMetadata(TEST_BUCKET);
    }

    /**
     * Helper method to plan a BQL query through logical and physical planners,
     * then optimize the result
     */
    PhysicalNode planAndOptimize(String bqlQuery) {
        BqlExpr expr = BqlParser.parse(bqlQuery);
        LogicalNode logicalPlan = logicalPlanner.plan(expr);
        PhysicalNode physicalPlan = physicalPlanner.plan(metadata, logicalPlan, new PlannerContext());
        return optimizer.optimize(metadata, physicalPlan, new PlannerContext());
    }

    /**
     * Helper method to plan a query without optimization (for comparison)
     */
    PhysicalNode planWithoutOptimization(String bqlQuery) {
        BqlExpr expr = BqlParser.parse(bqlQuery);
        LogicalNode logicalPlan = logicalPlanner.plan(expr);
        return physicalPlanner.plan(metadata, logicalPlan, new PlannerContext());
    }

    /**
     * Helper method to optimize an existing physical plan
     */
    PhysicalNode optimize(PhysicalNode physicalPlan) {
        return optimizer.optimize(metadata, physicalPlan, new PlannerContext());
    }


    /**
     * Helper method to create an index
     */
    void createIndex(IndexDefinition definition) {
        createIndexThenWaitForReadiness(definition);
        // Refresh the index registry
        metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
    }

    /**
     * Helper method to create multiple indexes at once
     */
    void createIndexes(IndexDefinition... definitions) {
        for (IndexDefinition definition : definitions) {
            createIndex(definition);
        }
    }

    /**
     * Helper method to extract the actual value from BqlValue objects
     */
    Object extractValue(Object operand) {
        if (operand instanceof StringVal(String value)) {
            return value;
        } else if (operand instanceof Int32Val(int value)) {
            return value;
        } else if (operand instanceof Int64Val(long value)) {
            return value;
        } else if (operand instanceof Decimal128Val(java.math.BigDecimal value)) {
            return value;
        } else if (operand instanceof DoubleVal(double value)) {
            return value;
        } else if (operand instanceof BooleanVal(boolean value)) {
            return value;
        } else if (operand instanceof NullVal) {
            return null;
        } else if (operand instanceof BinaryVal(byte[] value)) {
            return value;
        } else if (operand instanceof DateTimeVal(long value)) {
            return value;
        } else if (operand instanceof List<?> list) {
            return list.stream()
                    .map(this::extractValue)
                    .collect(java.util.stream.Collectors.toList());
        }
        return operand;
    }

    /**
     * Helper method to create a manual PhysicalFilter for testing
     */
    PhysicalFilter createFilter(String selector, Operator op, Object operand) {
        return new PhysicalFilter(1, selector, op, operand);
    }

    /**
     * Helper method to create a manual PhysicalIndexScan for testing
     */
    PhysicalIndexScan createIndexScan(PhysicalFilter filter) {
        return new PhysicalIndexScan(1, filter, null);
    }

    /**
     * Helper method to create a manual PhysicalAnd for testing
     */
    PhysicalAnd createAnd(PhysicalNode... children) {
        return new PhysicalAnd(1, List.of(children));
    }

    /**
     * Helper method to create a manual PhysicalOr for testing
     */
    PhysicalOr createOr(PhysicalNode... children) {
        return new PhysicalOr(1, List.of(children));
    }

    /**
     * Helper method to count the number of nodes of a specific type in a plan
     */
    int countNodeType(PhysicalNode plan, Class<? extends PhysicalNode> nodeType) {
        if (nodeType.isInstance(plan)) {
            int count = 1;
            if (plan instanceof PhysicalAnd and) {
                for (PhysicalNode child : and.children()) {
                    count += countNodeType(child, nodeType);
                }
            } else if (plan instanceof PhysicalOr or) {
                for (PhysicalNode child : or.children()) {
                    count += countNodeType(child, nodeType);
                }
            } else if (plan instanceof PhysicalNot not) {
                count += countNodeType(not.child(), nodeType);
            } else if (plan instanceof PhysicalElemMatch elemMatch) {
                count += countNodeType(elemMatch.subPlan(), nodeType);
            } else if (plan instanceof PhysicalIndexScan indexScan) {
                count += countNodeType(indexScan.node(), nodeType);
            }
            return count;
        } else {
            int count = 0;
            if (plan instanceof PhysicalAnd and) {
                for (PhysicalNode child : and.children()) {
                    count += countNodeType(child, nodeType);
                }
            } else if (plan instanceof PhysicalOr or) {
                for (PhysicalNode child : or.children()) {
                    count += countNodeType(child, nodeType);
                }
            } else if (plan instanceof PhysicalNot not) {
                count += countNodeType(not.child(), nodeType);
            } else if (plan instanceof PhysicalElemMatch elemMatch) {
                count += countNodeType(elemMatch.subPlan(), nodeType);
            } else if (plan instanceof PhysicalIndexScan indexScan) {
                count += countNodeType(indexScan.node(), nodeType);
            }
            return count;
        }
    }
}