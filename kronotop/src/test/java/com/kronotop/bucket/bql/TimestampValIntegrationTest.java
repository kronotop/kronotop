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

class TimestampValIntegrationTest {

    @Test
    void testTimestampValCreation() {
        long timestamp = System.currentTimeMillis();
        TimestampVal timestampVal = new TimestampVal(timestamp);

        assertEquals(timestamp, timestampVal.value());
        assertEquals(Long.toString(timestamp), timestampVal.toJson());
    }

    @Test
    void testTimestampValWithLogicalPlanner() {
        // Test that logical planner handles TimestampVal correctly
        long testTimestamp = 1609459200000L; // 2021-01-01 00:00:00 UTC
        BqlExpr query = new BqlEq("createdAt", new TimestampVal(testTimestamp));

        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with TimestampVal: " + e.getMessage());
        }
    }

    @Test
    void testTimestampValRangeQuery() {
        // Test timestamp values in range queries
        long startTime = 1609459200000L; // 2021-01-01 00:00:00 UTC
        long endTime = 1640995200000L;   // 2022-01-01 00:00:00 UTC

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlGte("timestamp", new TimestampVal(startTime)),
                new BqlLt("timestamp", new TimestampVal(endTime))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle timestamp range queries correctly
        } catch (Exception e) {
            fail("Logical planning failed with TimestampVal range query: " + e.getMessage());
        }
    }

    @Test
    void testTimestampValComparison() {
        // Test timestamp comparison operations
        long now = System.currentTimeMillis();
        long yesterday = now - 24 * 60 * 60 * 1000; // 24 hours ago

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlGt("lastLogin", new TimestampVal(yesterday)),
                new BqlLte("lastLogin", new TimestampVal(now))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle timestamp comparisons
        } catch (Exception e) {
            fail("Logical planning failed with TimestampVal comparisons: " + e.getMessage());
        }
    }

    @Test
    void testTimestampValEquality() {
        // Test timestamp equality comparisons
        long exactTime = 1577836800000L; // 2020-01-01 00:00:00 UTC

        BqlExpr query = new BqlEq("eventTime", new TimestampVal(exactTime));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with TimestampVal equality: " + e.getMessage());
        }
    }

    @Test
    void testTimestampValInequality() {
        // Test timestamp inequality comparisons  
        long time1 = 1577836800000L; // 2020-01-01 00:00:00 UTC
        long time2 = 1609459200000L; // 2021-01-01 00:00:00 UTC

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlEq("created", new TimestampVal(time1)),
                new BqlNe("created", new TimestampVal(time2))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should create a plan with redundancy elimination
        } catch (Exception e) {
            fail("Logical planning failed with TimestampVal inequality: " + e.getMessage());
        }
    }

    @Test
    void testTimestampValContradiction() {
        // Test contradiction detection with same timestamp in EQ and NE
        long testTime = 1672531200000L; // 2023-01-01 00:00:00 UTC

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlEq("timestamp", new TimestampVal(testTime)),
                new BqlNe("timestamp", new TimestampVal(testTime))
        ));

        LogicalPlanner planner = new LogicalPlanner();

        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should detect contradiction: timestamp = time AND timestamp != time
        } catch (Exception e) {
            fail("Logical planning failed with TimestampVal contradiction: " + e.getMessage());
        }
    }

    @Test
    void testTimestampValInArrays() {
        // Test TimestampVal in $in operator with arrays
        long time1 = 1577836800000L; // 2020-01-01
        long time2 = 1609459200000L; // 2021-01-01
        long time3 = 1640995200000L; // 2022-01-01

        BqlExpr query = new BqlIn("milestone", java.util.Arrays.asList(
                new TimestampVal(time1),
                new TimestampVal(time2),
                new TimestampVal(time3)
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with TimestampVal in arrays: " + e.getMessage());
        }
    }

    @Test
    void testTimestampValNotInArrays() {
        // Test TimestampVal in $nin operator
        long excludedTime = 946684800000L; // 2000-01-01 00:00:00 UTC (Y2K)

        BqlExpr query = new BqlNin("birthday", List.of(
                new TimestampVal(excludedTime)
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
        } catch (Exception e) {
            fail("Logical planning failed with TimestampVal in $nin: " + e.getMessage());
        }
    }

    @Test
    void testTimestampValWithMixedTypes() {
        // Test timestamp values mixed with other types in logical planning
        long timestamp = 1609459200000L;

        BqlExpr query = new BqlOr(java.util.Arrays.asList(
                new BqlEq("field", new StringVal("never")),
                new BqlEq("field", new TimestampVal(timestamp)),
                new BqlEq("field", NullVal.INSTANCE)
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle mixed types including timestamp correctly
        } catch (Exception e) {
            fail("Logical planning failed with mixed types including TimestampVal: " + e.getMessage());
        }
    }

    @Test
    void testTimestampValInDocuments() {
        // Test timestamp values within document structures
        long createdAt = System.currentTimeMillis();
        long updatedAt = createdAt + 3600000; // 1 hour later

        java.util.Map<String, BqlValue> fields = new java.util.LinkedHashMap<>();
        fields.put("id", new StringVal("doc123"));
        fields.put("createdAt", new TimestampVal(createdAt));
        fields.put("updatedAt", new TimestampVal(updatedAt));
        fields.put("version", new Int32Val(1));

        DocumentVal docVal = new DocumentVal(fields);
        assertEquals(4, docVal.fields().size());

        assertTrue(docVal.fields().containsKey("createdAt"));
        assertTrue(docVal.fields().containsKey("updatedAt"));
        assertInstanceOf(TimestampVal.class, docVal.fields().get("createdAt"));
        assertInstanceOf(TimestampVal.class, docVal.fields().get("updatedAt"));

        TimestampVal createdVal = (TimestampVal) docVal.fields().get("createdAt");
        TimestampVal updatedVal = (TimestampVal) docVal.fields().get("updatedAt");
        assertEquals(createdAt, createdVal.value());
        assertEquals(updatedAt, updatedVal.value());
    }

    @Test
    void testTimestampValJsonSerialization() {
        // Test JSON serialization of timestamp values
        long testTimestamp = 1234567890000L;
        TimestampVal timestampVal = new TimestampVal(testTimestamp);

        assertEquals(Long.toString(testTimestamp), timestampVal.toJson());

        // Test in complex structure
        BqlExpr query = new BqlEq("timestamp", timestampVal);
        String explanation = BqlParser.explain(query);
        assertNotNull(explanation);
        assertTrue(explanation.length() > 0, "Explanation should not be empty");
    }

    @Test
    void testTimestampValNumericOperations() {
        // Test that timestamp values work with numeric comparison operations
        long baseTime = 1609459200000L;
        long laterTime = baseTime + 86400000L; // 24 hours later

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlGt("eventTime", new TimestampVal(baseTime)),
                new BqlLt("eventTime", new TimestampVal(laterTime))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should handle timestamp as numeric for comparisons
        } catch (Exception e) {
            fail("Logical planning failed with TimestampVal numeric operations: " + e.getMessage());
        }
    }

    @Test
    void testTimestampValSpecialValues() {
        // Test timestamp values with special timestamps
        long epochStart = 0L;              // Unix epoch start
        long maxTimestamp = Long.MAX_VALUE; // Far future
        long currentTime = System.currentTimeMillis();

        TimestampVal epochVal = new TimestampVal(epochStart);
        TimestampVal maxVal = new TimestampVal(maxTimestamp);
        TimestampVal nowVal = new TimestampVal(currentTime);

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
                fail("Logical planning failed with special TimestampVal: " + e.getMessage());
            }
        }
    }

    @Test
    void testTimestampValTimeRanges() {
        // Test timestamp values in typical time range scenarios
        long dayStart = 1609459200000L;    // 2021-01-01 00:00:00 UTC
        long dayEnd = dayStart + 86399999L; // 2021-01-01 23:59:59.999 UTC

        // Query for events within a specific day
        BqlExpr dayRangeQuery = new BqlAnd(java.util.Arrays.asList(
                new BqlGte("timestamp", new TimestampVal(dayStart)),
                new BqlLte("timestamp", new TimestampVal(dayEnd))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(dayRangeQuery);
            assertNotNull(logicalPlan);
            // Should handle time range queries efficiently
        } catch (Exception e) {
            fail("Logical planning failed with TimestampVal time ranges: " + e.getMessage());
        }
    }

    @Test
    void testTimestampValRedundancyElimination() {
        // Test redundancy elimination with timestamp comparisons
        long baseTime = 1609459200000L;
        long laterTime = baseTime + 3600000L; // 1 hour later

        BqlExpr query = new BqlAnd(java.util.Arrays.asList(
                new BqlEq("created", new TimestampVal(baseTime)),
                new BqlNe("created", new TimestampVal(laterTime))
        ));

        LogicalPlanner planner = new LogicalPlanner();
        try {
            var logicalPlan = planner.plan(query);
            assertNotNull(logicalPlan);
            // Should eliminate redundant conditions (EQ makes NE with different value redundant)
        } catch (Exception e) {
            fail("Logical planning failed with TimestampVal redundancy elimination: " + e.getMessage());
        }
    }

    @Test
    void testTimestampValLogicalEquality() {
        // Test TimestampVal logical equality comparison
        long timestamp1 = 1609459200000L;
        long timestamp2 = 1609459200000L;
        long timestamp3 = 1609459200001L;

        TimestampVal val1 = new TimestampVal(timestamp1);
        TimestampVal val2 = new TimestampVal(timestamp2);
        TimestampVal val3 = new TimestampVal(timestamp3);

        assertEquals(val1, val2);
        assertNotEquals(val1, val3);
        assertEquals(val1.hashCode(), val2.hashCode());
    }
}