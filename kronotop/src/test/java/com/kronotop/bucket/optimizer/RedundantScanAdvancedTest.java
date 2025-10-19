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

import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.PhysicalFilter;
import com.kronotop.bucket.planner.physical.PhysicalIndexScan;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Advanced flat tests for RedundantScanEliminationRule covering complex scenarios.
 */
class RedundantScanAdvancedTest extends BaseOptimizerTest {

    @Test
    void shouldHandleDeeplyNestedRedundantStructures() {
        // Create index for name
        IndexDefinition nameIndex = IndexDefinition.create(
                "name-index", "name", BsonType.STRING
        );
        createIndex(nameIndex);

        // Test deeply nested redundant query: AND(AND(AND(name="john", name="john"), name="john"), name="john")
        String query = "{ $and: [" +
                "{ $and: [" +
                "{ $and: [{ \"name\": \"john\" }, { \"name\": \"john\" }] }, " +
                "{ \"name\": \"john\" }" +
                "] }, " +
                "{ \"name\": \"john\" }" +
                "] }";
        PhysicalNode optimized = planAndOptimize(query);

        // Should be reduced to single index scan regardless of nesting depth
        assertInstanceOf(PhysicalIndexScan.class, optimized);
        PhysicalIndexScan indexScan = (PhysicalIndexScan) optimized;
        assertInstanceOf(PhysicalFilter.class, indexScan.node());
        PhysicalFilter filter = (PhysicalFilter) indexScan.node();
        assertEquals("name", filter.selector());
        assertEquals(Operator.EQ, filter.op());
        assertEquals("john", extractValue(filter.operand()));
    }

    @Test
    void shouldEliminateRedundancyInNestedStructures() {
        // Create index for status
        IndexDefinition statusIndex = IndexDefinition.create(
                "status-index", "status", BsonType.STRING
        );
        createIndex(statusIndex);

        // Test nested OR with redundancy: OR(OR(status="active", status="active"), status="active")
        String query = "{ $or: [" +
                "{ $or: [{ \"status\": \"active\" }, { \"status\": \"active\" }] }, " +
                "{ \"status\": \"active\" }" +
                "] }";
        PhysicalNode optimized = planAndOptimize(query);

        // Should be reduced to single index scan
        assertInstanceOf(PhysicalIndexScan.class, optimized);
        PhysicalIndexScan indexScan = (PhysicalIndexScan) optimized;
        assertInstanceOf(PhysicalFilter.class, indexScan.node());
        PhysicalFilter filter = (PhysicalFilter) indexScan.node();
        assertEquals("status", filter.selector());
        assertEquals(Operator.EQ, filter.op());
        assertEquals("active", extractValue(filter.operand()));
    }
}