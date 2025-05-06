// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.logical;

import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.bql.values.Int32Val;
import com.kronotop.bucket.bql.values.StringVal;
import com.kronotop.bucket.planner.TestQuery;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class LogicalPlannerTest {
    private final String testBucket = "test-bucket";

    private LogicalNode getLogicalPlan(String query) {
        LogicalPlanner planner = new LogicalPlanner(query);
        return planner.plan();
    }

    @Test
    void when_planning_or_filter_with_two_sub_filters() {
        LogicalNode node = getLogicalPlan(TestQuery.OR_FILTER_WITH_TWO_SUB_FILTERS);

        assertInstanceOf(LogicalFullScan.class, node);
        LogicalFullScan fullScan = (LogicalFullScan) node;
        LogicalNode logicalOrFilter = fullScan.getChildren().getFirst();
        {
            LogicalNode logicalNode = logicalOrFilter.getChildren().get(0);
            assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
            LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;
            assertThat(logicalComparisonFilter.getOperatorType()).isEqualTo(OperatorType.EQ);
            assertThat(logicalComparisonFilter.getField()).isEqualTo("status");
            assertThat(logicalComparisonFilter.bqlValue().value()).isEqualTo("A");
        }

        {
            LogicalNode logicalNode = logicalOrFilter.getChildren().get(1);
            assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
            LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;
            assertThat(logicalComparisonFilter.getOperatorType()).isEqualTo(OperatorType.LT);
            assertThat(logicalComparisonFilter.getField()).isEqualTo("qty");
            assertThat(logicalComparisonFilter.bqlValue().value()).isEqualTo(30);
        }
    }

    @Test
    void when_planning_single_field_with_string_type_and_gte() {
        LogicalNode node = getLogicalPlan(TestQuery.SINGLE_FIELD_WITH_STRING_TYPE_AND_GTE);
        assertInstanceOf(LogicalFullScan.class, node);

        LogicalFullScan fullBucketScan = (LogicalFullScan) node;

        assertEquals(1, fullBucketScan.getChildren().size());

        LogicalFilter eqFilter = (LogicalFilter) fullBucketScan.getChildren().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, eqFilter);

        assertEquals(OperatorType.GTE, eqFilter.getOperatorType());

        LogicalComparisonFilter comparisonFilter = (LogicalComparisonFilter) eqFilter;
        assertEquals("a", comparisonFilter.getField());
        assertEquals(BsonType.STRING, comparisonFilter.bqlValue().bsonType());
        assertEquals("string-value", comparisonFilter.bqlValue().value());
    }

    @Test
    void when_planning_single_field_with_int32_type_and_eq() {
        LogicalNode node = getLogicalPlan(TestQuery.SINGLE_FIELD_WITH_IN32_TYPE_AND_EQ);
        assertInstanceOf(LogicalFullScan.class, node);

        LogicalFullScan fullBucketScan = (LogicalFullScan) node;

        assertEquals(1, fullBucketScan.getChildren().size());

        LogicalFilter eqFilter = (LogicalFilter) fullBucketScan.getChildren().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, eqFilter);

        assertEquals(OperatorType.EQ, eqFilter.getOperatorType());

        LogicalComparisonFilter comparisonFilter = (LogicalComparisonFilter) eqFilter;
        assertEquals("a", comparisonFilter.getField());
        assertEquals(BsonType.INT32, comparisonFilter.bqlValue().bsonType());
        assertEquals(20, comparisonFilter.bqlValue().value());
    }

    @Test
    void when_planning_implicit_and_filter() {
        LogicalNode node = getLogicalPlan(TestQuery.IMPLICIT_AND_FILTER);

        assertInstanceOf(LogicalFullScan.class, node);

        LogicalFullScan fullBucketScan = (LogicalFullScan) node;

        assertEquals(2, fullBucketScan.getChildren().size());

        {
            LogicalFilter eqFilter = (LogicalFilter) fullBucketScan.getChildren().get(0);
            assertInstanceOf(LogicalComparisonFilter.class, eqFilter);

            assertEquals(OperatorType.EQ, eqFilter.getOperatorType());

            LogicalComparisonFilter comparisonFilter = (LogicalComparisonFilter) eqFilter;
            assertEquals("status", comparisonFilter.getField());
            assertEquals(BsonType.STRING, comparisonFilter.bqlValue().bsonType());
            assertEquals("ALIVE", comparisonFilter.bqlValue().value());
        }

        {
            LogicalFilter eqFilter = (LogicalFilter) fullBucketScan.getChildren().get(1);
            assertInstanceOf(LogicalComparisonFilter.class, eqFilter);

            assertEquals(OperatorType.EQ, eqFilter.getOperatorType());

            LogicalComparisonFilter comparisonFilter = (LogicalComparisonFilter) eqFilter;
            assertEquals("username", comparisonFilter.getField());
            assertEquals(BsonType.STRING, comparisonFilter.bqlValue().bsonType());
            assertEquals("kronotop-admin", comparisonFilter.bqlValue().value());
        }
    }

    @Test
    void when_no_child_expression() {
        LogicalNode node = getLogicalPlan(TestQuery.NO_CHILD_EXPRESSION);

        assertInstanceOf(LogicalFullScan.class, node);
        assertTrue(node.getChildren().isEmpty());
    }

    @Test
    void when_implicit_EQ_FILTER() {
        LogicalNode node = getLogicalPlan(TestQuery.IMPLICIT_EQ_FILTER);

        assertInstanceOf(LogicalFullScan.class, node);
        assertEquals(1, node.getChildren().size());

        LogicalFullScan logicalFullBucketScan = (LogicalFullScan) node;
        LogicalNode logicalNode = logicalFullBucketScan.getChildren().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
        LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;

        LogicalComparisonFilter expected = new LogicalComparisonFilter(OperatorType.EQ);
        expected.setField("status");
        StringVal stringVal = new StringVal("ALIVE");
        expected.addBqlValue(stringVal);

        assertThat(logicalComparisonFilter).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void when_planning_explicit_EQ_filter() {
        LogicalNode node = getLogicalPlan(TestQuery.EXPLICIT_EQ_FILTER);

        assertInstanceOf(LogicalFullScan.class, node);
        assertEquals(1, node.getChildren().size());

        LogicalFullScan logicalFullBucketScan = (LogicalFullScan) node;
        LogicalNode logicalNode = logicalFullBucketScan.getChildren().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
        LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;

        LogicalComparisonFilter expected = new LogicalComparisonFilter(OperatorType.EQ);
        expected.setField("status");
        StringVal stringVal = new StringVal("ALIVE");
        expected.addBqlValue(stringVal);

        assertThat(logicalComparisonFilter).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void when_planning_explicit_EQ_filter_with_implicit_and_filter() {
        LogicalNode node = getLogicalPlan(TestQuery.EXPLICIT_EQ_FILTER_WITH_IMPLICIT_AND_FILTER);

        assertInstanceOf(LogicalFullScan.class, node);
        assertEquals(2, node.getChildren().size());

        LogicalFullScan logicalFullBucketScan = (LogicalFullScan) node;
        {
            LogicalNode logicalNode = logicalFullBucketScan.getChildren().getFirst();
            assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
            LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;

            LogicalComparisonFilter eqFilter_status = new LogicalComparisonFilter(OperatorType.EQ);
            eqFilter_status.setField("status");
            StringVal stringVal = new StringVal("ALIVE");
            eqFilter_status.addBqlValue(stringVal);

            assertThat(logicalComparisonFilter).usingRecursiveComparison().isEqualTo(eqFilter_status);
        }

        {
            LogicalNode logicalNode = logicalFullBucketScan.getChildren().get(1);
            assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
            LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;

            LogicalComparisonFilter eqFilter_status = new LogicalComparisonFilter(OperatorType.LT);
            eqFilter_status.setField("qty");
            Int32Val int32Val = new Int32Val(30);
            eqFilter_status.addBqlValue(int32Val);

            assertThat(logicalComparisonFilter).usingRecursiveComparison().isEqualTo(eqFilter_status);
        }
    }

    @Test
    void when_planning_explicit_and_filter_with_two_sub_filters() {
        LogicalNode node = getLogicalPlan(TestQuery.EXPLICIT_AND_FILTER_WITH_TWO_SUB_FILTERS);
        assertInstanceOf(LogicalFullScan.class, node);
        assertEquals(1, node.getChildren().size());

        LogicalNode childNode = node.getChildren().getFirst();
        assertInstanceOf(LogicalAndFilter.class, childNode);
        LogicalAndFilter logicalAndFilter = (LogicalAndFilter) childNode;
        assertEquals(2, logicalAndFilter.getChildren().size());

        {
            LogicalNode logicalNode = logicalAndFilter.getChildren().getFirst();
            assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
            LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;
            assertEquals(OperatorType.EQ, logicalComparisonFilter.getOperatorType());
            assertEquals("status", logicalComparisonFilter.getField());
            assertEquals("A", logicalComparisonFilter.bqlValue().value());
        }

        {
            LogicalNode logicalNode = logicalAndFilter.getChildren().get(1);
            assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
            LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;
            assertEquals(OperatorType.LT, logicalComparisonFilter.getOperatorType());
            assertEquals("qty", logicalComparisonFilter.getField());
            assertEquals(30, logicalComparisonFilter.bqlValue().value());
        }
    }

    @Test
    void when_planning_complex_query_one() {
        LogicalNode node = getLogicalPlan(TestQuery.COMPLEX_QUERY_ONE);
        assertInstanceOf(LogicalFullScan.class, node);

        LogicalFullScan logicalFullScan = (LogicalFullScan) node;
        assertEquals(1, logicalFullScan.getChildren().size());

        LogicalAndFilter logicalAndFilter = (LogicalAndFilter) logicalFullScan.getChildren().getFirst();
        assertEquals(2, logicalAndFilter.getChildren().size());

        {
            LogicalNode logicalNode = logicalAndFilter.getChildren().getFirst();
            assertInstanceOf(LogicalOrFilter.class, logicalNode);
            LogicalOrFilter logicalOrFilter = (LogicalOrFilter) logicalNode;
            assertEquals(2, logicalOrFilter.getChildren().size());
            {
                LogicalNode first = logicalOrFilter.getChildren().get(0);
                assertInstanceOf(LogicalComparisonFilter.class, first);
                LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) first;
                assertEquals(OperatorType.LT, logicalComparisonFilter.getOperatorType());
                assertEquals("qty", logicalComparisonFilter.getField());
                assertEquals(BsonType.INT32, logicalComparisonFilter.bqlValue().bsonType());
                assertEquals(10, logicalComparisonFilter.bqlValue().value());
            }

            {
                LogicalNode first = logicalOrFilter.getChildren().get(1);
                assertInstanceOf(LogicalComparisonFilter.class, first);
                LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) first;
                assertEquals(OperatorType.GT, logicalComparisonFilter.getOperatorType());
                assertEquals("qty", logicalComparisonFilter.getField());
                assertEquals(BsonType.INT32, logicalComparisonFilter.bqlValue().bsonType());
                assertEquals(50, logicalComparisonFilter.bqlValue().value());
            }
        }

        {
            LogicalNode logicalNode = logicalAndFilter.getChildren().get(1);
            assertInstanceOf(LogicalOrFilter.class, logicalNode);
            LogicalOrFilter logicalOrFilter = (LogicalOrFilter) logicalNode;
            assertEquals(2, logicalOrFilter.getChildren().size());

            {
                LogicalNode first = logicalOrFilter.getChildren().get(0);
                assertInstanceOf(LogicalComparisonFilter.class, first);
                LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) first;
                assertEquals(OperatorType.EQ, logicalComparisonFilter.getOperatorType());
                assertEquals("sale", logicalComparisonFilter.getField());
                assertEquals(BsonType.BOOLEAN, logicalComparisonFilter.bqlValue().bsonType());
                assertEquals(true, logicalComparisonFilter.bqlValue().value());
            }

            {
                LogicalNode first = logicalOrFilter.getChildren().get(1);
                assertInstanceOf(LogicalComparisonFilter.class, first);
                LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) first;
                assertEquals(OperatorType.LT, logicalComparisonFilter.getOperatorType());
                assertEquals("price", logicalComparisonFilter.getField());
                assertEquals(BsonType.INT32, logicalComparisonFilter.bqlValue().bsonType());
                assertEquals(5, logicalComparisonFilter.bqlValue().value());
            }
        }
    }

    @Test
    void when_planning_implicit_and_with_ne_and_exists_filter() {
        LogicalNode node = getLogicalPlan(TestQuery.IMPLICIT_AND_WITH_NE_AND_EXISTS);

        assertInstanceOf(LogicalFullScan.class, node);
        assertEquals(2, node.getChildren().size());

        {
            LogicalNode childNode = node.getChildren().getFirst();
            assertInstanceOf(LogicalComparisonFilter.class, childNode);
            LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) childNode;
            assertEquals(OperatorType.NE, logicalComparisonFilter.getOperatorType());
            assertEquals("price", logicalComparisonFilter.getField());
            assertEquals(1.99, logicalComparisonFilter.bqlValue().value());
        }

        {
            LogicalNode childNode = node.getChildren().get(1);
            assertInstanceOf(LogicalExistsFilter.class, childNode);
            LogicalExistsFilter logicalExistsFilter = (LogicalExistsFilter) childNode;
            assertEquals(OperatorType.EXISTS, logicalExistsFilter.getOperatorType());
            assertEquals("price", logicalExistsFilter.getField());
            assertTrue(logicalExistsFilter.value());
        }
    }

    @Test
    void when_planning_exists_filter() {
        LogicalNode node = getLogicalPlan(TestQuery.EXISTS_FILTER);

        assertInstanceOf(LogicalFullScan.class, node);
        assertEquals(1, node.getChildren().size());

        LogicalNode childNode = node.getChildren().getFirst();
        assertInstanceOf(LogicalExistsFilter.class, childNode);
        LogicalExistsFilter logicalExistsFilter = (LogicalExistsFilter) childNode;
        assertEquals(OperatorType.EXISTS, logicalExistsFilter.getOperatorType());
        assertEquals("price", logicalExistsFilter.getField());
        assertTrue(logicalExistsFilter.value());
    }

    @Test
    void when_planning_ne_filter_with_implicit_eq_filter() {
        LogicalNode node = getLogicalPlan(TestQuery.NOT_EQUALS_FILTER_WITH_IMPLICIT_EQ_FILTER);
        assertInstanceOf(LogicalFullScan.class, node);
        assertEquals(1, node.getChildren().size());

        LogicalNode childNode = node.getChildren().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, childNode);
        LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) childNode;
        assertEquals(OperatorType.NE, logicalComparisonFilter.getOperatorType());
        assertEquals("status", logicalComparisonFilter.getField());
        assertEquals("A", logicalComparisonFilter.bqlValue().value());
    }

    @Test
    void when_planning_ne_filter_with_explicit_eq_filter() {
        LogicalNode node = getLogicalPlan(TestQuery.NOT_EQUALS_FILTER_WITH_EXPLICIT_EQ_FILTER);

        assertInstanceOf(LogicalFullScan.class, node);
        assertEquals(1, node.getChildren().size());

        LogicalNode childNode = node.getChildren().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, childNode);
        LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) childNode;
        assertEquals(OperatorType.EQ, logicalComparisonFilter.getOperatorType());
        assertEquals("status", logicalComparisonFilter.getField());
        assertEquals("A", logicalComparisonFilter.bqlValue().value());
    }
}