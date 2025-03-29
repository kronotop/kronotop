// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.logical;

import com.kronotop.bucket.bql.BqlValue;
import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.planner.TestQueries;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class LogicalPlannerTest {
    private final String testBucket = "test-bucket";

    private LogicalNode getLogicalPlan(String query) {
        LogicalPlanner planner = new LogicalPlanner(testBucket, query);
        return planner.plan();
    }

    @Test
    void test_prepareLogicalPlan() {
        //LogicalPlanner planner = new LogicalPlanner(testBucket, "{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: 30 } } ], username: { $eq: 'buraksezer' }, tags: { $all: ['foo', 32]} }");
        //QueryOptimizer planner = new QueryOptimizer("{ status: {$eq: 'ALIVE'}, username: {$eq: 'kronotop-admin'}, age: {$lt: 35} }");
        //QueryOptimizer planner = new QueryOptimizer("{ status: 'ALIVE', username: 'kronotop-admin' }");
        //QueryOptimizer planner = new QueryOptimizer("{}");
        LogicalPlanner optimizer = new LogicalPlanner(testBucket, "{_id: {$gte: '00010CRQ5VIMO0000000xxxx'}}");
        //LogicalPlanner planner = new LogicalPlanner(testBucket, "{ status: {$eq: 'ALIVE'}, username: {$eq: 'kronotop-admin'}, age: {$lt: 35} }");

        //LogicalPlanner planner = new LogicalPlanner(testBucket, "{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: 30 } } ], username: { $eq: 'buraksezer' }, tags: { $all: ['foo', 32]} }");
        LogicalNode node = optimizer.plan();
        System.out.println(node);
    }

    @Test
    void when_plan_or_filter_with_two_sub_filters() {
        LogicalNode node = getLogicalPlan(TestQueries.OR_FILTER_WITH_TWO_SUB_FILTERS);

        assertInstanceOf(LogicalFullScan.class, node);
        LogicalFullScan fullScan = (LogicalFullScan) node;
        LogicalNode logicalOrFilter = fullScan.getChildren().getFirst();
        {
            LogicalNode logicalNode = logicalOrFilter.getChildren().get(0);
            assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
            LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;
            assertThat(logicalComparisonFilter.getOperatorType()).isEqualTo(OperatorType.EQ);
            assertThat(logicalComparisonFilter.getField()).isEqualTo("status");
            assertThat(logicalComparisonFilter.getValue().getValue()).isEqualTo("A");
        }

        {
            LogicalNode logicalNode = logicalOrFilter.getChildren().get(1);
            assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
            LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;
            assertThat(logicalComparisonFilter.getOperatorType()).isEqualTo(OperatorType.LT);
            assertThat(logicalComparisonFilter.getField()).isEqualTo("qty");
            assertThat(logicalComparisonFilter.getValue().getValue()).isEqualTo(30);
        }
    }

    @Test
    void when_plan_single_field_with_string_type_and_gte() {
        LogicalNode node = getLogicalPlan(TestQueries.SINGLE_FIELD_WITH_STRING_TYPE_AND_GTE);
        assertInstanceOf(LogicalFullScan.class, node);

        LogicalFullScan fullBucketScan = (LogicalFullScan)node;

        assertThat(fullBucketScan.getBucket()).isEqualTo(testBucket);
        assertEquals(1, fullBucketScan.getChildren().size());

        LogicalFilter eqFilter = (LogicalFilter) fullBucketScan.getChildren().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, eqFilter);

        assertEquals(OperatorType.GTE, eqFilter.getOperatorType());

        LogicalComparisonFilter comparisonFilter = (LogicalComparisonFilter) eqFilter;
        assertEquals("a", comparisonFilter.getField());
        assertEquals(BsonType.STRING, comparisonFilter.getValue().getBsonType());
        assertEquals("string-value", comparisonFilter.getValue().getValue());
    }

    @Test
    void when_plan_single_field_with_int32_type_and_eq() {
        LogicalNode node = getLogicalPlan(TestQueries.SINGLE_FIELD_WITH_IN32_TYPE_AND_EQ);
        assertInstanceOf(LogicalFullScan.class, node);

        LogicalFullScan fullBucketScan = (LogicalFullScan)node;

        assertThat(fullBucketScan.getBucket()).isEqualTo(testBucket);
        assertEquals(1, fullBucketScan.getChildren().size());

        LogicalFilter eqFilter = (LogicalFilter) fullBucketScan.getChildren().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, eqFilter);

        assertEquals(OperatorType.EQ, eqFilter.getOperatorType());

        LogicalComparisonFilter comparisonFilter = (LogicalComparisonFilter) eqFilter;
        assertEquals("a", comparisonFilter.getField());
        assertEquals(BsonType.INT32, comparisonFilter.getValue().getBsonType());
        assertEquals(20, comparisonFilter.getValue().getValue());
    }

    @Test
    void when_plan_implicit_and_filter() {
        LogicalNode node = getLogicalPlan(TestQueries.IMPLICIT_AND_FILTER);

        assertInstanceOf(LogicalFullScan.class, node);

        LogicalFullScan fullBucketScan = (LogicalFullScan)node;

        assertThat(fullBucketScan.getBucket()).isEqualTo(testBucket);
        assertEquals(2, fullBucketScan.getChildren().size());

        {
            LogicalFilter eqFilter = (LogicalFilter) fullBucketScan.getChildren().get(0);
            assertInstanceOf(LogicalComparisonFilter.class, eqFilter);

            assertEquals(OperatorType.EQ, eqFilter.getOperatorType());

            LogicalComparisonFilter comparisonFilter = (LogicalComparisonFilter) eqFilter;
            assertEquals("status", comparisonFilter.getField());
            assertEquals(BsonType.STRING, comparisonFilter.getValue().getBsonType());
            assertEquals("ALIVE", comparisonFilter.getValue().getValue());
        }

        {
            LogicalFilter eqFilter = (LogicalFilter) fullBucketScan.getChildren().get(1);
            assertInstanceOf(LogicalComparisonFilter.class, eqFilter);

            assertEquals(OperatorType.EQ, eqFilter.getOperatorType());

            LogicalComparisonFilter comparisonFilter = (LogicalComparisonFilter) eqFilter;
            assertEquals("username", comparisonFilter.getField());
            assertEquals(BsonType.STRING, comparisonFilter.getValue().getBsonType());
            assertEquals("kronotop-admin", comparisonFilter.getValue().getValue());
        }
    }

    @Test
    void when_no_child_expression() {
        LogicalNode node = getLogicalPlan(TestQueries.NO_CHILD_EXPRESSION);

        assertInstanceOf(LogicalFullScan.class, node);
        assertTrue(node.getChildren().isEmpty());
        LogicalFullScan logicalFullBucketScan = (LogicalFullScan) node;
        assertEquals(testBucket, logicalFullBucketScan.getBucket());
    }

    @Test
    void when_implicit_EQ_FILTER() {
        LogicalNode node = getLogicalPlan(TestQueries.IMPLICIT_EQ_FILTER);

        assertInstanceOf(LogicalFullScan.class, node);
        assertEquals(1, node.getChildren().size());

        LogicalFullScan logicalFullBucketScan = (LogicalFullScan) node;
        LogicalNode logicalNode = logicalFullBucketScan.getChildren().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
        LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;

        LogicalComparisonFilter expected = new LogicalComparisonFilter(OperatorType.EQ);
        expected.setField("status");
        BqlValue<String> bqlValue = new BqlValue<>(BsonType.STRING);
        bqlValue.setValue("ALIVE");
        expected.addValue(bqlValue);

        assertThat(logicalComparisonFilter).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void when_plan_explicit_EQ_filter() {
        LogicalNode node = getLogicalPlan(TestQueries.EXPLICIT_EQ_FILTER);

        assertInstanceOf(LogicalFullScan.class, node);
        assertEquals(1, node.getChildren().size());

        LogicalFullScan logicalFullBucketScan = (LogicalFullScan) node;
        LogicalNode logicalNode = logicalFullBucketScan.getChildren().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
        LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;

        LogicalComparisonFilter expected = new LogicalComparisonFilter(OperatorType.EQ);
        expected.setField("status");
        BqlValue<String> bqlValue = new BqlValue<>(BsonType.STRING);
        bqlValue.setValue("ALIVE");
        expected.addValue(bqlValue);

        assertThat(logicalComparisonFilter).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void when_plan_explicit_EQ_filter_with_implicit_and_filter() {
        LogicalNode node = getLogicalPlan(TestQueries.EXPLICIT_EQ_FILTER_WITH_IMPLICIT_AND_FILTER);

        assertInstanceOf(LogicalFullScan.class, node);
        assertEquals(2, node.getChildren().size());

        LogicalFullScan logicalFullBucketScan = (LogicalFullScan) node;
        {
            LogicalNode logicalNode = logicalFullBucketScan.getChildren().getFirst();
            assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
            LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;

            LogicalComparisonFilter eqFilter_status = new LogicalComparisonFilter(OperatorType.EQ);
            eqFilter_status.setField("status");
            BqlValue<String> bqlValue = new BqlValue<>(BsonType.STRING);
            bqlValue.setValue("ALIVE");
            eqFilter_status.addValue(bqlValue);

            assertThat(logicalComparisonFilter).usingRecursiveComparison().isEqualTo(eqFilter_status);
        }

        {
            LogicalNode logicalNode = logicalFullBucketScan.getChildren().get(1);
            assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
            LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;

            LogicalComparisonFilter eqFilter_status = new LogicalComparisonFilter(OperatorType.LT);
            eqFilter_status.setField("qty");
            BqlValue<Integer> bqlValue = new BqlValue<>(BsonType.INT32);
            bqlValue.setValue(30);
            eqFilter_status.addValue(bqlValue);

            assertThat(logicalComparisonFilter).usingRecursiveComparison().isEqualTo(eqFilter_status);
        }
    }

    @Test
    void when_plan_explicit_and_filter_with_two_sub_filters() {
        LogicalNode node = getLogicalPlan(TestQueries.EXPLICIT_AND_FILTER_WITH_TWO_SUB_FILTERS);
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
            assertEquals("A", logicalComparisonFilter.getValue().getValue());
        }

        {
            LogicalNode logicalNode = logicalAndFilter.getChildren().get(1);
            assertInstanceOf(LogicalComparisonFilter.class, logicalNode);
            LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) logicalNode;
            assertEquals(OperatorType.LT, logicalComparisonFilter.getOperatorType());
            assertEquals("qty", logicalComparisonFilter.getField());
            assertEquals(30, logicalComparisonFilter.getValue().getValue());
        }
    }

    @Test
    void test_foo2() {
        LogicalPlanner optimizer = new LogicalPlanner(testBucket,
                "{ $and: [{ $or: [ { qty: { $lt : 10 } }, { qty : { $gt: 50 } } ] },{ $or: [ { sale: true }, { price : { $lt : 5 } } ] }]}"
        );
        LogicalNode node = optimizer.plan();
        System.out.println(node);
    }

    @Test
    void test_foo3() {
        LogicalPlanner optimizer = new LogicalPlanner(testBucket,
                "{ price: { $ne: 1.99, $exists: true } }"
        );
        LogicalNode node = optimizer.plan();
        System.out.println(node);
    }

    @Test
    void when_plan_ne_filter_with_implicit_eq_filter() {
        LogicalNode node = getLogicalPlan(TestQueries.NOT_EQUALS_FILTER_WITH_IMPLICIT_EQ_FILTER);
        assertInstanceOf(LogicalFullScan.class, node);
        assertEquals(1, node.getChildren().size());

        LogicalNode childNode = node.getChildren().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, childNode);
        LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) childNode;
        assertEquals(OperatorType.NE, logicalComparisonFilter.getOperatorType());
        assertEquals("status", logicalComparisonFilter.getField());
        assertEquals("A", logicalComparisonFilter.getValue().getValue());
    }

    @Test
    void when_plan_ne_filter_with_explicit_eq_filter() {
        LogicalNode node = getLogicalPlan(TestQueries.NOT_EQUALS_FILTER_WITH_EXPLICIT_EQ_FILTER);

        assertInstanceOf(LogicalFullScan.class, node);
        assertEquals(1, node.getChildren().size());

        LogicalNode childNode = node.getChildren().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, childNode);
        LogicalComparisonFilter logicalComparisonFilter = (LogicalComparisonFilter) childNode;
        assertEquals(OperatorType.EQ, logicalComparisonFilter.getOperatorType());
        assertEquals("status", logicalComparisonFilter.getField());
        assertEquals("A", logicalComparisonFilter.getValue().getValue());
    }
}