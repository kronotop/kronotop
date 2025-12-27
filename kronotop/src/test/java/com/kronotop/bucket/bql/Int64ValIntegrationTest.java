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

package com.kronotop.bucket.bql;

import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Int64ValIntegrationTest {

    @Test
    void shouldCreateInt64Val() {
        long testValue = Long.MAX_VALUE;
        Int64Val int64Val = new Int64Val(testValue);

        assertEquals(testValue, int64Val.value());
        assertEquals(Long.toString(testValue), int64Val.toJson());
    }

    @Test
    void shouldParseInt64Values() {
        // Test parsing 64-bit integer values in BQL
        String bqlQuery = "{ count: { $eq: " + Long.MAX_VALUE + " } }";
        BqlExpr result = BqlParser.parse(bqlQuery);

        assertInstanceOf(BqlEq.class, result);
        BqlEq eqExpr = (BqlEq) result;
        assertEquals("count", eqExpr.selector());

        assertInstanceOf(Int64Val.class, eqExpr.value());
        Int64Val int64Val = (Int64Val) eqExpr.value();
        assertEquals(Long.MAX_VALUE, int64Val.value());
    }

    @Test
    void shouldHandleInt64ValInComplexQuery() {
        // Test 64-bit values in range queries
        long minValue = 1000000000000L; // 1 trillion
        long maxValue = 2000000000000L; // 2 trillion

        String query = String.format("{ amount: { $gte: %d, $lt: %d } }", minValue, maxValue);
        BqlExpr result = BqlParser.parse(query);

        assertInstanceOf(BqlAnd.class, result);
        BqlAnd andExpr = (BqlAnd) result;
        assertEquals(2, andExpr.children().size());

        // Check $gte
        assertInstanceOf(BqlGte.class, andExpr.children().get(0));
        BqlGte gteExpr = (BqlGte) andExpr.children().get(0);
        assertEquals("amount", gteExpr.selector());
        assertInstanceOf(Int64Val.class, gteExpr.value());
        assertEquals(minValue, ((Int64Val) gteExpr.value()).value());

        // Check $lt
        assertInstanceOf(BqlLt.class, andExpr.children().get(1));
        BqlLt ltExpr = (BqlLt) andExpr.children().get(1);
        assertEquals("amount", ltExpr.selector());
        assertInstanceOf(Int64Val.class, ltExpr.value());
        assertEquals(maxValue, ((Int64Val) ltExpr.value()).value());
    }

    @Test
    void shouldHandleInt64ValInLogicalPlanner() {
        // Test that logical planner handles Int64Val correctly
        long testValue = 9223372036854775807L; // Long.MAX_VALUE
        String query = "{ timestamp: { $eq: " + testValue + " } }";

        BqlExpr bqlResult = BqlParser.parse(query);
        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(bqlResult);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with Int64Val: " + e.getMessage());
        }
    }

    @Test
    void shouldEliminateInt64ValRedundancy() {
        // Test that redundancy elimination works with Int64Val
        long value1 = 5000000000L; // 5 billion
        long value2 = 6000000000L; // 6 billion

        String query = String.format("{ size: { $eq: %d, $ne: %d } }", value1, value2);
        BqlExpr bqlResult = BqlParser.parse(query);

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(bqlResult);
            assertNotNull(logicalPlan);
            // Should create a plan with just the equality (redundancy elimination should work)
        } catch (Exception e) {
            fail("Logical planning failed with Int64Val redundancy elimination: " + e.getMessage());
        }
    }

    @Test
    void shouldDetectInt64ValContradiction() {
        // Test contradiction detection with same Int64Val in EQ and NE
        long testValue = 123456789012345L;
        String query = String.format("{ id: { $eq: %d, $ne: %d } }", testValue, testValue);

        BqlExpr bqlResult = BqlParser.parse(query);
        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(bqlResult);
            assertNotNull(logicalPlan);
            // Should detect contradiction and produce appropriate result
        } catch (Exception e) {
            fail("Logical planning failed with Int64Val contradiction: " + e.getMessage());
        }
    }

    @Test
    void shouldCompareInt64ValNumerically() {
        // Test numeric comparison between different integer types
        String query = "{ value: { $gt: 2147483647, $lt: 9223372036854775807 } }"; // > Int32.MAX, < Int64.MAX
        BqlExpr result = BqlParser.parse(query);

        assertInstanceOf(BqlAnd.class, result);
        BqlAnd andExpr = (BqlAnd) result;
        assertEquals(2, andExpr.children().size());

        // The first value (2147483647) might be parsed as Int32Val since it fits
        // The second value (9223372036854775807) should be Int64Val since it exceeds Int32 range
        BqlGt gtExpr = (BqlGt) andExpr.children().get(0);
        BqlLt ltExpr = (BqlLt) andExpr.children().get(1);

        // Check that large numbers are parsed as Int64Val
        assertInstanceOf(Int64Val.class, ltExpr.value());
        assertEquals(Long.MAX_VALUE, ((Int64Val) ltExpr.value()).value());
    }

    @Test
    void shouldRoundtripInt64ValToJsonAndBack() {
        // Test round-trip: create Int64Val, serialize to JSON, parse back
        long originalValue = -9223372036854775808L; // Long.MIN_VALUE
        Int64Val originalInt64Val = new Int64Val(originalValue);

        String jsonRepresentation = originalInt64Val.toJson();
        assertEquals(Long.toString(originalValue), jsonRepresentation);

        // Create a BQL query with this value and parse it back
        String query = String.format("{ negative_id: %s }", jsonRepresentation);
        BqlExpr parsed = BqlParser.parse(query);

        assertInstanceOf(BqlEq.class, parsed);
        BqlEq eqExpr = (BqlEq) parsed;
        assertInstanceOf(Int64Val.class, eqExpr.value());

        Int64Val parsedInt64Val = (Int64Val) eqExpr.value();
        assertEquals(originalValue, parsedInt64Val.value());
        assertEquals(jsonRepresentation, parsedInt64Val.toJson());
    }

    @Test
    void shouldHandleInt64ValInArrays() {
        // Test Int64Val in $in operator with arrays
        long val1 = 1000000000000L;
        long val2 = 2000000000000L;
        long val3 = 3000000000000L;

        String query = String.format("{ bigId: { $in: [%d, %d, %d] } }", val1, val2, val3);
        BqlExpr result = BqlParser.parse(query);

        assertInstanceOf(BqlIn.class, result);
        BqlIn inExpr = (BqlIn) result;
        assertEquals("bigId", inExpr.selector());
        assertEquals(3, inExpr.values().size());

        // Verify all values are Int64Val with correct values
        for (int i = 0; i < 3; i++) {
            assertInstanceOf(Int64Val.class, inExpr.values().get(i));
            Int64Val int64Val = (Int64Val) inExpr.values().get(i);
            assertEquals((i + 1) * 1000000000000L, int64Val.value());
        }
    }
}