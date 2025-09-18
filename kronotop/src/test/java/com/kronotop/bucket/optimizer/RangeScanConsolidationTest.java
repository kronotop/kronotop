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
import com.kronotop.bucket.planner.physical.*;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple flat tests for RangeScanConsolidationRule without nested classes.
 */
class RangeScanConsolidationTest extends BaseOptimizerTest {

    @Test
    void shouldConsolidateRangeConditions() {
        // Create index for age field
        IndexDefinition ageIndex = IndexDefinition.create("age_index", "age", BsonType.INT32);
        createIndex(ageIndex);

        // Create: AND(age > 18, age < 65)
        PhysicalFilter filter1 = createFilter("age", Operator.GT, 18);
        PhysicalFilter filter2 = createFilter("age", Operator.LT, 65);
        PhysicalIndexScan scan1 = createIndexScan(filter1);
        PhysicalIndexScan scan2 = createIndexScan(filter2);
        PhysicalAnd originalPlan = createAnd(scan1, scan2);

        PhysicalNode optimized = optimize(originalPlan);

        // Should be consolidated to single range scan
        assertInstanceOf(PhysicalRangeScan.class, optimized);
        PhysicalRangeScan result = (PhysicalRangeScan) optimized;
        assertEquals("age", result.selector());
        assertEquals(18, result.lowerBound());
        assertEquals(65, result.upperBound());
        assertFalse(result.includeLower());
        assertFalse(result.includeUpper());
    }

    @Test
    void shouldConsolidateRangeWithEqualityBounds() {
        // Create index for score field
        IndexDefinition scoreIndex = IndexDefinition.create("score_index", "score", BsonType.INT32);
        createIndex(scoreIndex);

        // Create: AND(score >= 80, score <= 100)
        PhysicalFilter filter1 = createFilter("score", Operator.GTE, 80);
        PhysicalFilter filter2 = createFilter("score", Operator.LTE, 100);
        PhysicalIndexScan scan1 = createIndexScan(filter1);
        PhysicalIndexScan scan2 = createIndexScan(filter2);
        PhysicalAnd originalPlan = createAnd(scan1, scan2);

        PhysicalNode optimized = optimize(originalPlan);

        // Should be consolidated to single range scan
        assertInstanceOf(PhysicalRangeScan.class, optimized);
        PhysicalRangeScan result = (PhysicalRangeScan) optimized;
        assertEquals("score", result.selector());
        assertEquals(80, result.lowerBound());
        assertEquals(100, result.upperBound());
        assertTrue(result.includeLower());
        assertTrue(result.includeUpper());
    }

    @Test
    void shouldNotConsolidateDifferentFields() {
        // Create indexes for age and score fields
        IndexDefinition ageIndex = IndexDefinition.create("age_index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score_index", "score", BsonType.INT32);
        createIndexes(ageIndex, scoreIndex);

        // Create: AND(age > 18, score < 100)
        PhysicalFilter filter1 = createFilter("age", Operator.GT, 18);
        PhysicalFilter filter2 = createFilter("score", Operator.LT, 100);
        PhysicalIndexScan scan1 = createIndexScan(filter1);
        PhysicalIndexScan scan2 = createIndexScan(filter2);
        PhysicalAnd originalPlan = createAnd(scan1, scan2);

        PhysicalNode optimized = optimize(originalPlan);

        // Should remain as AND with separate index scans
        assertInstanceOf(PhysicalAnd.class, optimized);
        PhysicalAnd result = (PhysicalAnd) optimized;
        assertEquals(2, result.children().size());
    }

    @Test
    void shouldNotConsolidateNonRangeOperators() {
        // Create index for name field
        IndexDefinition nameIndex = IndexDefinition.create("name_index", "name", BsonType.STRING);
        createIndex(nameIndex);

        // Create: AND(name = "john", name = "jane") - equality operators
        PhysicalFilter filter1 = createFilter("name", Operator.EQ, "john");
        PhysicalFilter filter2 = createFilter("name", Operator.EQ, "jane");
        PhysicalIndexScan scan1 = createIndexScan(filter1);
        PhysicalIndexScan scan2 = createIndexScan(filter2);
        PhysicalAnd originalPlan = createAnd(scan1, scan2);

        PhysicalNode optimized = optimize(originalPlan);

        // Should be handled by IndexIntersectionRule instead
        assertInstanceOf(PhysicalIndexIntersection.class, optimized);
    }
}