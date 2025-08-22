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
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple flat tests for IndexIntersectionRule without nested classes.
 */
class IndexIntersectionTest extends BaseOptimizerTest {

    @Test
    void shouldCreateIndexIntersection() {
        // Create indexes for name and age fields
        IndexDefinition nameIndex = IndexDefinition.create("name_index", "name", BsonType.STRING, SortOrder.ASCENDING);
        IndexDefinition ageIndex = IndexDefinition.create("age_index", "age", BsonType.INT32, SortOrder.ASCENDING);
        createIndexes(nameIndex, ageIndex);

        // Create: AND(name="john", age=25)
        PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
        PhysicalFilter filter2 = createFilter("age", Operator.EQ, 25);
        PhysicalIndexScan scan1 = createIndexScan(filter1);
        PhysicalIndexScan scan2 = createIndexScan(filter2);
        PhysicalAnd originalPlan = createAnd(scan1, scan2);

        PhysicalNode optimized = optimize(originalPlan);

        // Should be converted to index intersection
        assertInstanceOf(PhysicalIndexIntersection.class, optimized);
        PhysicalIndexIntersection result = (PhysicalIndexIntersection) optimized;
        assertEquals(2, result.indexes().size());
        assertEquals(2, result.filters().size());
    }

    @Test
    void shouldCreateIndexIntersectionWithThreeIndexes() {
        // Create indexes for name, age, and status fields
        IndexDefinition nameIndex = IndexDefinition.create("name_index", "name", BsonType.STRING, SortOrder.ASCENDING);
        IndexDefinition ageIndex = IndexDefinition.create("age_index", "age", BsonType.INT32, SortOrder.ASCENDING);
        IndexDefinition statusIndex = IndexDefinition.create("status_index", "status", BsonType.STRING, SortOrder.ASCENDING);
        createIndexes(nameIndex, ageIndex, statusIndex);

        // Create: AND(name="john", age=25, status="active")
        PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
        PhysicalFilter filter2 = createFilter("age", Operator.EQ, 25);
        PhysicalFilter filter3 = createFilter("status", Operator.EQ, "active");
        PhysicalIndexScan scan1 = createIndexScan(filter1);
        PhysicalIndexScan scan2 = createIndexScan(filter2);
        PhysicalIndexScan scan3 = createIndexScan(filter3);
        PhysicalAnd originalPlan = createAnd(scan1, scan2, scan3);

        PhysicalNode optimized = optimize(originalPlan);

        // Should be converted to index intersection
        assertInstanceOf(PhysicalIndexIntersection.class, optimized);
        PhysicalIndexIntersection result = (PhysicalIndexIntersection) optimized;
        assertEquals(3, result.indexes().size());
        assertEquals(3, result.filters().size());
    }

    @Test
    void shouldNotCreateIntersectionWithSingleIndex() {
        // Create index for name field only
        IndexDefinition nameIndex = IndexDefinition.create("name_index", "name", BsonType.STRING, SortOrder.ASCENDING);
        createIndex(nameIndex);

        // Create: name="john" (single condition, no AND needed)
        PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
        PhysicalIndexScan originalPlan = createIndexScan(filter1);

        PhysicalNode optimized = optimize(originalPlan);

        // Should remain as single index scan since there's nothing to intersect
        assertInstanceOf(PhysicalIndexScan.class, optimized);
    }

    @Test
    void shouldNotCreateIntersectionWithNonEqualityOperators() {
        // Create indexes for name and age fields
        IndexDefinition nameIndex = IndexDefinition.create("name_index", "name", BsonType.STRING, SortOrder.ASCENDING);
        IndexDefinition ageIndex = IndexDefinition.create("age_index", "age", BsonType.INT32, SortOrder.ASCENDING);
        createIndexes(nameIndex, ageIndex);

        // Create: AND(name="john", age>25) - mixed operators
        PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
        PhysicalFilter filter2 = createFilter("age", Operator.GT, 25);
        PhysicalIndexScan scan1 = createIndexScan(filter1);
        PhysicalIndexScan scan2 = createIndexScan(filter2);
        PhysicalAnd originalPlan = createAnd(scan1, scan2);

        PhysicalNode optimized = optimize(originalPlan);

        // Should remain as AND with separate scans (only EQ operators qualify for intersection)
        assertInstanceOf(PhysicalAnd.class, optimized);
        PhysicalAnd result = (PhysicalAnd) optimized;
        assertEquals(2, result.children().size());
    }

    @Test
    void shouldMixIntersectionWithOtherNodes() {
        // Create indexes for name and age fields
        IndexDefinition nameIndex = IndexDefinition.create("name_index", "name", BsonType.STRING, SortOrder.ASCENDING);
        IndexDefinition ageIndex = IndexDefinition.create("age_index", "age", BsonType.INT32, SortOrder.ASCENDING);
        createIndexes(nameIndex, ageIndex);

        // Create: AND(name="john", age=25, score>90) - mix of indexed and non-indexed
        PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
        PhysicalFilter filter2 = createFilter("age", Operator.EQ, 25);
        PhysicalFilter filter3 = createFilter("score", Operator.GT, 90); // no index
        PhysicalIndexScan scan1 = createIndexScan(filter1);
        PhysicalIndexScan scan2 = createIndexScan(filter2);
        PhysicalFullScan scan3 = new PhysicalFullScan(1, filter3);
        PhysicalAnd originalPlan = createAnd(scan1, scan2, scan3);

        PhysicalNode optimized = optimize(originalPlan);

        // Should create intersection for indexed conditions and keep full scan separate
        assertInstanceOf(PhysicalAnd.class, optimized);
        PhysicalAnd result = (PhysicalAnd) optimized;
        assertEquals(2, result.children().size());

        // One child should be index intersection, other should be full scan
        boolean hasIntersection = result.children().stream()
                .anyMatch(child -> child instanceof PhysicalIndexIntersection);
        boolean hasFullScan = result.children().stream()
                .anyMatch(child -> child instanceof PhysicalFullScan);

        assertTrue(hasIntersection);
        assertTrue(hasFullScan);
    }
}