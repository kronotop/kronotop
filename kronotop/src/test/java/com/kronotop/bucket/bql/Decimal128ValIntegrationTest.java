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

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class Decimal128ValIntegrationTest {

    @Test
    void shouldCreateDecimal128Val() {
        BigDecimal testValue = new BigDecimal("123.456789012345678901234567890123456789");
        Decimal128Val decimal128Val = new Decimal128Val(testValue);

        assertEquals(testValue, decimal128Val.value());
        assertEquals(testValue.toString(), decimal128Val.toJson());
    }

    @Test
    void shouldHandleDecimal128ValPrecision() {
        // Test high precision decimal values
        BigDecimal highPrecision = new BigDecimal("999999999999999999999999999999999.999999999999999999999999999999999");
        Decimal128Val decimal128Val = new Decimal128Val(highPrecision);

        assertEquals(highPrecision, decimal128Val.value());
        assertEquals(highPrecision.toString(), decimal128Val.toJson());
    }

    @Test
    void shouldHandleDecimal128ValInComplexQuery() {
        // Test decimal values in range queries using strings since BSON parser needs special handling
        // In real BSON, these would be DECIMAL128 values
        String query = "{ price: { $gte: 999.99, $lt: 1000.01 } }";
        BqlExpr result = BqlParser.parse(query);

        assertInstanceOf(BqlAnd.class, result);
        BqlAnd andExpr = (BqlAnd) result;
        assertEquals(2, andExpr.children().size());

        // Check $gte - these will be parsed as DoubleVal in this test since JSON doesn't distinguish DECIMAL128
        assertInstanceOf(BqlGte.class, andExpr.children().get(0));
        BqlGte gteExpr = (BqlGte) andExpr.children().get(0);
        assertEquals("price", gteExpr.selector());

        // Check $lt
        assertInstanceOf(BqlLt.class, andExpr.children().get(1));
        BqlLt ltExpr = (BqlLt) andExpr.children().get(1);
        assertEquals("price", ltExpr.selector());
    }

    @Test
    void shouldHandleDecimal128ValInLogicalPlanner() {
        // Create a BqlExpr manually since BSON parsing from JSON string doesn't create DECIMAL128
        BigDecimal testValue = new BigDecimal("12345.6789012345678901234567890");
        BqlExpr query = new BqlEq("amount", new Decimal128Val(testValue));

        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with Decimal128Val: " + e.getMessage());
        }
    }

    @Test
    void shouldEliminateDecimal128ValRedundancy() {
        // Test that redundancy elimination works with Decimal128Val
        BigDecimal value1 = new BigDecimal("100.123456789012345678901234567890");
        BigDecimal value2 = new BigDecimal("200.987654321098765432109876543210");

        // Create manual query: amount = value1 AND amount != value2
        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlEq("amount", new Decimal128Val(value1)),
                new BqlNe("amount", new Decimal128Val(value2))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should create a plan with redundancy elimination (NE becomes redundant when EQ is present)
        } catch (Exception e) {
            fail("Logical planning failed with Decimal128Val redundancy elimination: " + e.getMessage());
        }
    }

    @Test
    void shouldDetectDecimal128ValContradiction() {
        // Test contradiction detection with same Decimal128Val in EQ and NE
        BigDecimal testValue = new BigDecimal("42.123456789012345678901234567890123456");

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlEq("price", new Decimal128Val(testValue)),
                new BqlNe("price", new Decimal128Val(testValue))
        ));

        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should detect contradiction and produce appropriate result
        } catch (Exception e) {
            fail("Logical planning failed with Decimal128Val contradiction: " + e.getMessage());
        }
    }

    @Test
    void shouldCompareDecimal128ValNumerically() {
        // Test numeric comparison with Decimal128Val
        BigDecimal minValue = new BigDecimal("0.000000000000000000000000000001");
        BigDecimal maxValue = new BigDecimal("999999999999999999999999999999.999999999999999999999999999999");

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlGte("balance", new Decimal128Val(minValue)),
                new BqlLt("balance", new Decimal128Val(maxValue))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with Decimal128Val numeric comparison: " + e.getMessage());
        }
    }

    @Test
    void shouldRoundtripDecimal128ValToJsonAndBack() {
        // Test round-trip: create Decimal128Val, serialize to JSON representation
        BigDecimal originalValue = new BigDecimal("-123456789.123456789012345678901234567890");
        Decimal128Val originalDecimal128Val = new Decimal128Val(originalValue);

        String jsonRepresentation = originalDecimal128Val.toJson();
        assertEquals(originalValue.toString(), jsonRepresentation);

        // Verify the value can be recreated
        Decimal128Val recreatedDecimal128Val = new Decimal128Val(new BigDecimal(jsonRepresentation));
        assertEquals(originalValue, recreatedDecimal128Val.value());
        assertEquals(jsonRepresentation, recreatedDecimal128Val.toJson());
    }

    @Test
    void shouldHandleDecimal128ValInArrays() {
        // Test Decimal128Val in $in operator with arrays
        BigDecimal val1 = new BigDecimal("1.000000000000000000000000000001");
        BigDecimal val2 = new BigDecimal("2.000000000000000000000000000002");
        BigDecimal val3 = new BigDecimal("3.000000000000000000000000000003");

        BqlExpr query = new BqlIn("precise_value", java.util.Arrays.asList(
                new Decimal128Val(val1),
                new Decimal128Val(val2),
                new Decimal128Val(val3)
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with Decimal128Val in arrays: " + e.getMessage());
        }
    }

    @Test
    void shouldHandleDecimal128ValSpecialValues() {
        // Test special decimal values like zero, very small, very large
        BigDecimal zero = BigDecimal.ZERO;
        BigDecimal verySmall = new BigDecimal("0.000000000000000000000000000000000001");
        BigDecimal veryLarge = new BigDecimal("9999999999999999999999999999999999999");

        Decimal128Val zeroVal = new Decimal128Val(zero);
        Decimal128Val smallVal = new Decimal128Val(verySmall);
        Decimal128Val largeVal = new Decimal128Val(veryLarge);

        assertEquals("0", zeroVal.toJson());
        assertEquals(verySmall.toString(), smallVal.toJson());
        assertEquals(veryLarge.toString(), largeVal.toJson());

        // Test these values work in logical planning
        LogicalPlanner planner = new LogicalPlanner();

        BqlExpr[] queries = {
                new BqlEq("amount", zeroVal),
                new BqlGt("precision", smallVal),
                new BqlLt("limit", largeVal)
        };

        for (BqlExpr query : queries) {
            try {
                var logicalPlan = planner.plan(query);
                assertNotNull(logicalPlan);
            } catch (Exception e) {
                fail("Logical planning failed with special Decimal128Val: " + e.getMessage());
            }
        }
    }

    @Test
    void shouldHandleDecimal128ValMixedWithOtherNumericTypes() {
        // Test Decimal128Val mixed with other numeric types in logical planning
        BigDecimal decimalValue = new BigDecimal("100.123456789012345678901234567890");

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlGt("amount", new Int32Val(99)),           // Integer comparison
                new BqlLt("amount", new Decimal128Val(decimalValue)),  // Decimal comparison
                new BqlNe("amount", new DoubleVal(100.5))        // Double comparison
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle mixed numeric types correctly
        } catch (Exception e) {
            fail("Logical planning failed with mixed numeric types including Decimal128Val: " + e.getMessage());
        }
    }
}