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

import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;

class BasePhysicalPlannerTest extends BaseStandaloneInstanceTest {
    protected PhysicalPlanner physicalPlanner;
    protected LogicalPlanner logicalPlanner;
    protected BucketMetadata metadata;

    @BeforeEach
    void setUp() {
        physicalPlanner = new PhysicalPlanner();
        logicalPlanner = new LogicalPlanner();
        metadata = getBucketMetadata(TEST_BUCKET);
    }

    /**
     * Helper method to plan a BQL query through logical and physical planners
     */
    PhysicalNode planQuery(String bqlQuery) {
        BqlExpr expr = BqlParser.parse(bqlQuery);
        LogicalNode logicalPlan = logicalPlanner.plan(expr);
        return physicalPlanner.plan(metadata, logicalPlan, new PlannerContext());
    }

    /**
     * Helper method to create logical plan directly
     */
    PhysicalNode planLogical(LogicalNode logicalPlan) {
        return physicalPlanner.plan(metadata, logicalPlan, new PlannerContext());
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
}
