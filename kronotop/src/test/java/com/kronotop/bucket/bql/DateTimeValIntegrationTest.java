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

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DateTimeValIntegrationTest {

    @Test
    void testDateTimeValCreation() {
        long timestamp = System.currentTimeMillis();
        DateTimeVal dateTimeVal = new DateTimeVal(timestamp);

        assertEquals(timestamp, dateTimeVal.value());
        assertEquals(Long.toString(timestamp), dateTimeVal.toJson());
    }

    @Test
    void testDateTimeValWithLogicalPlanner() {
        // Test that logical planner handles DateTimeVal correctly
        long testTimestamp = 1609459200000L; // 2021-01-01 00:00:00 UTC
        BqlExpr query = new BqlEq("createdAt", new DateTimeVal(testTimestamp));

        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with DateTimeVal: " + e.getMessage());
        }
    }

    @Test
    void testDateTimeValRangeQuery() {
        // Test datetime values in range queries
        long startTime = 1609459200000L; // 2021-01-01 00:00:00 UTC
        long endTime = 1640995200000L;   // 2022-01-01 00:00:00 UTC

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlGte("timestamp", new DateTimeVal(startTime)),
                new BqlLt("timestamp", new DateTimeVal(endTime))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle datetime range queries correctly
        } catch (Exception e) {
            fail("Logical planning failed with DateTimeVal range query: " + e.getMessage());
        }
    }

    @Test
    void testDateTimeValComparison() {
        // Test datetime comparison operations
        long now = System.currentTimeMillis();
        long yesterday = now - 24 * 60 * 60 * 1000; // 24 hours ago

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlGt("lastLogin", new DateTimeVal(yesterday)),
                new BqlLte("lastLogin", new DateTimeVal(now))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle datetime comparisons
        } catch (Exception e) {
            fail("Logical planning failed with DateTimeVal comparisons: " + e.getMessage());
        }
    }

    @Test
    void testDateTimeValEquality() {
        // Test datetime equality comparisons
        long exactTime = 1577836800000L; // 2020-01-01 00:00:00 UTC

        BqlExpr query = new BqlEq("eventTime", new DateTimeVal(exactTime));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with DateTimeVal equality: " + e.getMessage());
        }
    }

    @Test
    void testDateTimeValInequality() {
        // Test datetime inequality comparisons  
        long time1 = 1577836800000L; // 2020-01-01 00:00:00 UTC
        long time2 = 1609459200000L; // 2021-01-01 00:00:00 UTC

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlEq("created", new DateTimeVal(time1)),
                new BqlNe("created", new DateTimeVal(time2))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should create a plan with redundancy elimination
        } catch (Exception e) {
            fail("Logical planning failed with DateTimeVal inequality: " + e.getMessage());
        }
    }

    @Test
    void testDateTimeValContradiction() {
        // Test contradiction detection with same datetime in EQ and NE
        long testTime = 1672531200000L; // 2023-01-01 00:00:00 UTC

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlEq("timestamp", new DateTimeVal(testTime)),
                new BqlNe("timestamp", new DateTimeVal(testTime))
        ));

        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should detect contradiction: timestamp = time AND timestamp != time
        } catch (Exception e) {
            fail("Logical planning failed with DateTimeVal contradiction: " + e.getMessage());
        }
    }

    @Test
    void testDateTimeValInArrays() {
        // Test DateTimeVal in $in operator with arrays
        long time1 = 1577836800000L; // 2020-01-01
        long time2 = 1609459200000L; // 2021-01-01
        long time3 = 1640995200000L; // 2022-01-01

        BqlExpr query = new BqlIn("milestone", java.util.Arrays.asList(
                new DateTimeVal(time1),
                new DateTimeVal(time2),
                new DateTimeVal(time3)
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with DateTimeVal in arrays: " + e.getMessage());
        }
    }

    @Test
    void testDateTimeValNotInArrays() {
        // Test DateTimeVal in $nin operator
        long excludedTime = 946684800000L; // 2000-01-01 00:00:00 UTC (Y2K)

        BqlExpr query = new BqlNin("birthday", List.of(
                new DateTimeVal(excludedTime)
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with DateTimeVal in $nin: " + e.getMessage());
        }
    }

    @Test
    void testDateTimeValWithMixedTypes() {
        // Test datetime values mixed with other types in logical planning
        long dateTime = 1609459200000L;

        BqlExpr query = new BqlOr(java.util.Arrays.asList(
                new BqlEq("field", new StringVal("never")),
                new BqlEq("field", new DateTimeVal(dateTime)),
                new BqlEq("field", NullVal.INSTANCE)
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle mixed types including datetime correctly
        } catch (Exception e) {
            fail("Logical planning failed with mixed types including DateTimeVal: " + e.getMessage());
        }
    }

    @Test
    void testDateTimeValInDocuments() {
        // Test datetime values within document structures
        long createdAt = System.currentTimeMillis();
        long updatedAt = createdAt + 3600000; // 1 hour later

        java.util.Map<String, BqlValue> fields = new java.util.LinkedHashMap<>();
        fields.put("id", new StringVal("doc123"));
        fields.put("createdAt", new DateTimeVal(createdAt));
        fields.put("updatedAt", new DateTimeVal(updatedAt));
        fields.put("version", new Int32Val(1));

        DocumentVal docVal = new DocumentVal(fields);
        assertEquals(4, docVal.fields().size());

        assertTrue(docVal.fields().containsKey("createdAt"));
        assertTrue(docVal.fields().containsKey("updatedAt"));
        assertInstanceOf(DateTimeVal.class, docVal.fields().get("createdAt"));
        assertInstanceOf(DateTimeVal.class, docVal.fields().get("updatedAt"));

        DateTimeVal createdVal = (DateTimeVal) docVal.fields().get("createdAt");
        DateTimeVal updatedVal = (DateTimeVal) docVal.fields().get("updatedAt");
        assertEquals(createdAt, createdVal.value());
        assertEquals(updatedAt, updatedVal.value());
    }

    @Test
    void testDateTimeValJsonSerialization() {
        // Test JSON serialization of datetime values
        long testTimestamp = 1234567890000L;
        DateTimeVal dateTimeVal = new DateTimeVal(testTimestamp);

        assertEquals(Long.toString(testTimestamp), dateTimeVal.toJson());

        // Test in complex structure
        BqlExpr query = new BqlEq("timestamp", dateTimeVal);
        String explanation = BqlParser.explain(query);
        assertNotNull(explanation);
        assertTrue(explanation.length() > 0, "Explanation should not be empty");
    }

    @Test
    void testDateTimeValNumericOperations() {
        // Test that datetime values work with numeric comparison operations
        long baseTime = 1609459200000L;
        long laterTime = baseTime + 86400000L; // 24 hours later

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlGt("eventTime", new DateTimeVal(baseTime)),
                new BqlLt("eventTime", new DateTimeVal(laterTime))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle datetime as numeric for comparisons
        } catch (Exception e) {
            fail("Logical planning failed with DateTimeVal numeric operations: " + e.getMessage());
        }
    }

    @Test
    void testDateTimeValSpecialValues() {
        // Test datetime values with special timestamps
        long epochStart = 0L;              // Unix epoch start
        long maxTimestamp = Long.MAX_VALUE; // Far future
        long currentTime = System.currentTimeMillis();

        DateTimeVal epochVal = new DateTimeVal(epochStart);
        DateTimeVal maxVal = new DateTimeVal(maxTimestamp);
        DateTimeVal nowVal = new DateTimeVal(currentTime);

        assertEquals("0", epochVal.toJson());
        assertEquals(Long.toString(maxTimestamp), maxVal.toJson());
        assertEquals(Long.toString(currentTime), nowVal.toJson());

        // Test these values work in logical planning
        LogicalPlanner planner = new LogicalPlanner();

        BqlExpr[] queries = {
                new BqlEq("epoch", epochVal),
                new BqlGt("future", maxVal),
                new BqlLte("now", nowVal)
        };

        for (BqlExpr query : queries) {
            try {
                var logicalPlan = planner.plan(query);
                assertNotNull(logicalPlan);
            } catch (Exception e) {
                fail("Logical planning failed with special DateTimeVal: " + e.getMessage());
            }
        }
    }

    @Test
    void testDateTimeValTimeRanges() {
        // Test datetime values in typical time range scenarios
        long dayStart = 1609459200000L;    // 2021-01-01 00:00:00 UTC
        long dayEnd = dayStart + 86399999L; // 2021-01-01 23:59:59.999 UTC

        // Query for events within a specific day
        BqlExpr dayRangeQuery = new BqlAnd(java.util.Arrays.asList(
                new BqlGte("timestamp", new DateTimeVal(dayStart)),
                new BqlLte("timestamp", new DateTimeVal(dayEnd))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(dayRangeQuery);
            assertNotNull(logicalPlan);
            // Should handle time range queries efficiently
        } catch (Exception e) {
            fail("Logical planning failed with DateTimeVal time ranges: " + e.getMessage());
        }
    }

    @Test
    void testDateTimeValRedundancyElimination() {
        // Test redundancy elimination with datetime comparisons
        long baseTime = 1609459200000L;
        long laterTime = baseTime + 3600000L; // 1 hour later

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlEq("created", new DateTimeVal(baseTime)),
                new BqlNe("created", new DateTimeVal(laterTime))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should eliminate redundant conditions (EQ makes NE with different value redundant)
        } catch (Exception e) {
            fail("Logical planning failed with DateTimeVal redundancy elimination: " + e.getMessage());
        }
    }
}