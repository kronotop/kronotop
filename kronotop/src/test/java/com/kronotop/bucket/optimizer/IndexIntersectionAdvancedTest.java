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
import com.kronotop.bucket.planner.physical.PhysicalIndexIntersection;
import com.kronotop.bucket.planner.physical.PhysicalIndexScan;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Advanced flat tests for IndexIntersectionRule covering complex scenarios.
 */
class IndexIntersectionAdvancedTest extends BaseOptimizerTest {

    @Test
    void shouldCreateIntersectionInNestedAndOperations() {
        // Create indexes for name and age
        IndexDefinition nameIndex = IndexDefinition.create(
                "name-index", "name", BsonType.STRING
        );
        IndexDefinition ageIndex = IndexDefinition.create(
                "age-index", "age", BsonType.INT32
        );
        createIndexes(nameIndex, ageIndex);

        // Test nested AND: { $and: [{ $and: [{ name: "john" }, { age: 25 }] }] }
        String query = "{ $and: [{ $and: [{ \"name\": \"john\" }, { \"age\": 25 }] }] }";
        PhysicalNode optimized = planAndOptimize(query);

        // Should create intersection even in nested structure
        assertInstanceOf(PhysicalIndexIntersection.class, optimized);
        PhysicalIndexIntersection intersection = (PhysicalIndexIntersection) optimized;
        assertEquals(2, intersection.indexes().size());
        assertEquals(2, intersection.filters().size());
    }

    @Test
    void shouldHandleSingleChildGracefully() {
        // Create index for name
        IndexDefinition nameIndex = IndexDefinition.create(
                "name-index", "name", BsonType.STRING
        );
        createIndex(nameIndex);

        // Test query with single condition in AND
        String query = "{ $and: [{ \"name\": \"john\" }] }";
        PhysicalNode optimized = planAndOptimize(query);

        // Should be simplified to just the index scan
        assertInstanceOf(PhysicalIndexScan.class, optimized);
        PhysicalIndexScan indexScan = (PhysicalIndexScan) optimized;
        assertInstanceOf(PhysicalFilter.class, indexScan.node());
        PhysicalFilter filter = (PhysicalFilter) indexScan.node();
        assertEquals("name", filter.selector());
        assertEquals(Operator.EQ, filter.op());
        assertEquals("john", extractValue(filter.operand()));
    }
}