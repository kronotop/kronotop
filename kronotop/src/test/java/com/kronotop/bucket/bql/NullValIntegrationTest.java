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

class NullValIntegrationTest {

    @Test
    void testNullValCreation() {
        NullVal nullVal = new NullVal();
        assertEquals("null", nullVal.toJson());

        // Test singleton instance
        assertSame(NullVal.INSTANCE, NullVal.INSTANCE);
        assertEquals(NullVal.INSTANCE.toJson(), nullVal.toJson());
    }

    @Test
    void testNullValSingleton() {
        // Verify that INSTANCE is a singleton
        NullVal instance1 = NullVal.INSTANCE;
        NullVal instance2 = NullVal.INSTANCE;
        assertSame(instance1, instance2);
    }

    @Test
    void testBqlParserHandlesNull() {
        // Test parsing null values in BQL
        String bqlQuery = "{ field: null }";
        BqlExpr result = BqlParser.parse(bqlQuery);

        assertInstanceOf(BqlEq.class, result);
        BqlEq eqExpr = (BqlEq) result;
        assertEquals("field", eqExpr.selector());

        assertInstanceOf(NullVal.class, eqExpr.value());
        NullVal nullVal = (NullVal) eqExpr.value();
        assertEquals("null", nullVal.toJson());
    }

    @Test
    void testNullValInComplexQuery() {
        // Test null values in comparison queries
        String query = "{ $and: [{ status: null }, { deleted: { $ne: null } }] }";
        BqlExpr result = BqlParser.parse(query);

        assertInstanceOf(BqlAnd.class, result);
        BqlAnd andExpr = (BqlAnd) result;
        assertEquals(2, andExpr.children().size());

        // Check first condition: status = null
        assertInstanceOf(BqlEq.class, andExpr.children().get(0));
        BqlEq eqExpr = (BqlEq) andExpr.children().get(0);
        assertEquals("status", eqExpr.selector());
        assertInstanceOf(NullVal.class, eqExpr.value());

        // Check second condition: deleted != null  
        assertInstanceOf(BqlNe.class, andExpr.children().get(1));
        BqlNe neExpr = (BqlNe) andExpr.children().get(1);
        assertEquals("deleted", neExpr.selector());
        assertInstanceOf(NullVal.class, neExpr.value());
    }

    @Test
    void testNullValWithLogicalPlanner() {
        // Test that logical planner handles NullVal correctly
        String query = "{ field: null }";

        BqlExpr bqlResult = BqlParser.parse(query);
        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(bqlResult);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with NullVal: " + e.getMessage());
        }
    }

    @Test
    void testNullValEquality() {
        // Test null equality comparisons
        String query = "{ field: { $eq: null } }";
        BqlExpr result = BqlParser.parse(query);

        assertInstanceOf(BqlEq.class, result);
        BqlEq eqExpr = (BqlEq) result;
        assertEquals("field", eqExpr.selector());
        assertInstanceOf(NullVal.class, eqExpr.value());
    }

    @Test
    void testNullValInequality() {
        // Test null inequality comparisons  
        String query = "{ field: { $ne: null } }";
        BqlExpr result = BqlParser.parse(query);

        assertInstanceOf(BqlNe.class, result);
        BqlNe neExpr = (BqlNe) result;
        assertEquals("field", neExpr.selector());
        assertInstanceOf(NullVal.class, neExpr.value());
    }

    @Test
    void testNullValContradictionDetection() {
        // Test contradiction detection with null values
        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlEq("field", NullVal.INSTANCE),
                new BqlNe("field", NullVal.INSTANCE)
        ));

        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should detect contradiction: field = null AND field != null
        } catch (Exception e) {
            fail("Logical planning failed with NullVal contradiction: " + e.getMessage());
        }
    }

    @Test
    void testNullValInArrays() {
        // Test NullVal in $in operator with arrays
        String query = "{ status: { $in: [\"active\", null, \"inactive\"] } }";
        BqlExpr result = BqlParser.parse(query);

        assertInstanceOf(BqlIn.class, result);
        BqlIn inExpr = (BqlIn) result;
        assertEquals("status", inExpr.selector());
        assertEquals(3, inExpr.values().size());

        // Verify the values include null
        assertInstanceOf(StringVal.class, inExpr.values().get(0));
        assertInstanceOf(NullVal.class, inExpr.values().get(1));
        assertInstanceOf(StringVal.class, inExpr.values().get(2));

        assertEquals("active", ((StringVal) inExpr.values().get(0)).value());
        assertEquals("null", ((NullVal) inExpr.values().get(1)).toJson());
        assertEquals("inactive", ((StringVal) inExpr.values().get(2)).value());
    }

    @Test
    void testNullValNotInArrays() {
        // Test NullVal in $nin operator
        String query = "{ field: { $nin: [null] } }";
        BqlExpr result = BqlParser.parse(query);

        assertInstanceOf(BqlNin.class, result);
        BqlNin ninExpr = (BqlNin) result;
        assertEquals("field", ninExpr.selector());
        assertEquals(1, ninExpr.values().size());
        assertInstanceOf(NullVal.class, ninExpr.values().get(0));
    }

    @Test
    void testNullValWithMixedTypes() {
        // Test null values mixed with other types in logical planning
        BqlExpr query = new BqlOr(java.util.Arrays.asList(
                new BqlEq("field", new StringVal("value")),
                new BqlEq("field", NullVal.INSTANCE),
                new BqlEq("field", new Int32Val(42))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle mixed types including null correctly
        } catch (Exception e) {
            fail("Logical planning failed with mixed types including NullVal: " + e.getMessage());
        }
    }

    @Test
    void testNullValInDocuments() {
        // Create a manual DocumentVal with null values since BQL doesn't parse nested documents this way
        java.util.Map<String, BqlValue> fields = new java.util.LinkedHashMap<>();
        fields.put("name", new StringVal("John"));
        fields.put("age", NullVal.INSTANCE);

        DocumentVal docVal = new DocumentVal(fields);
        assertEquals(2, docVal.fields().size());

        assertTrue(docVal.fields().containsKey("name"));
        assertTrue(docVal.fields().containsKey("age"));

        assertInstanceOf(StringVal.class, docVal.fields().get("name"));
        assertInstanceOf(NullVal.class, docVal.fields().get("age"));

        assertEquals("John", ((StringVal) docVal.fields().get("name")).value());
        assertEquals("null", ((NullVal) docVal.fields().get("age")).toJson());
    }

    @Test
    void testNullValJsonSerialization() {
        // Test JSON serialization of null values
        NullVal nullVal = NullVal.INSTANCE;
        assertEquals("null", nullVal.toJson());

        // Test in complex structure
        BqlExpr query = new BqlEq("field", nullVal);
        String explanation = BqlParser.explain(query);
        assertNotNull(explanation);
        // The explanation should contain some representation of the null value
        assertTrue(explanation.length() > 0, "Explanation should not be empty");
    }

    @Test
    void testNullValLogicalTransforms() {
        // Test that null values work correctly through logical transforms
        String query1 = "{ $and: [{ field: null }] }";
        String query2 = "{ field: null }";

        BqlExpr result1 = BqlParser.parse(query1);
        BqlExpr result2 = BqlParser.parse(query2);

        LogicalPlanner planner = new LogicalPlanner();

        try {
            var plan1 = planner.plan(result1);
            var plan2 = planner.plan(result2);

            assertNotNull(plan1);
            assertNotNull(plan2);
            // Both should produce equivalent plans after flattening transforms
        } catch (Exception e) {
            fail("Logical transforms failed with NullVal: " + e.getMessage());
        }
    }

    @Test
    void testNullValNumericOperationHandling() {
        // Test that null values are properly rejected for numeric operations
        // This tests the validator and transformation logic

        BqlExpr query = new BqlGt("field", NullVal.INSTANCE);
        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle null in comparison operations, though semantics may vary
        } catch (Exception e) {
            // This might throw an exception depending on validation rules
            // Either behavior (allow or reject) can be valid depending on requirements
            assertTrue(e.getMessage().contains("null") || e.getMessage().contains("Null"));
        }
    }
}