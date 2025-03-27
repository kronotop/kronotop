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
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class LogicalPlannerTest {
    private final String testBucket = "test-bucket";

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
    void test_prepareLogicalPlan2() {
        LogicalPlanner optimizer = new LogicalPlanner(testBucket,
                "{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: 30 } } ] }"
        );
        LogicalNode node = optimizer.plan();
        System.out.println(node);
    }

    @Test
    void single_field_string_gte() {
        LogicalPlanner optimizer = new LogicalPlanner(testBucket,
                "{ a: { $gte: 'string-value' } }"
        );
        /*
        LogicalFullBucketScan {
            bucket=test-bucket,
            filters=[
                LogicalComparisonFilter {
                    operatorType=GTE,
                    field=_id,
                    value=BqlValue { type=STRING, value=string-value }
                }
            ]
        }
        */

        LogicalNode node = optimizer.plan();
        assertInstanceOf(LogicalFullBucketScan.class, node);

        LogicalFullBucketScan fullBucketScan = (LogicalFullBucketScan)node;

        assertThat(fullBucketScan.getBucket()).isEqualTo(testBucket);
        assertEquals(1, fullBucketScan.getFilters().size());

        LogicalFilter eqFilter = fullBucketScan.getFilters().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, eqFilter);

        assertEquals(OperatorType.GTE, eqFilter.getOperatorType());

        LogicalComparisonFilter comparisonFilter = (LogicalComparisonFilter) eqFilter;
        assertEquals("a", comparisonFilter.getField());
        assertEquals(BsonType.STRING, comparisonFilter.getValue().getBsonType());
        assertEquals("string-value", comparisonFilter.getValue().getValue());
    }

    @Test
    void single_field_int32_equality() {
        LogicalPlanner optimizer = new LogicalPlanner(testBucket,
                "{ a: { $eq: 20 } }"
        );
        /*
        LogicalFullBucketScan {
            bucket=test-bucket,
            filters=[
                LogicalComparisonFilter {
                    operatorType=EQ,
                    field=a,
                    value=BqlValue { type=INT32, value=20 }
                }
            ]
        }
        */
        LogicalNode node = optimizer.plan();
        assertInstanceOf(LogicalFullBucketScan.class, node);

        LogicalFullBucketScan fullBucketScan = (LogicalFullBucketScan)node;

        assertThat(fullBucketScan.getBucket()).isEqualTo(testBucket);
        assertEquals(1, fullBucketScan.getFilters().size());

        LogicalFilter eqFilter = fullBucketScan.getFilters().getFirst();
        assertInstanceOf(LogicalComparisonFilter.class, eqFilter);

        assertEquals(OperatorType.EQ, eqFilter.getOperatorType());

        LogicalComparisonFilter comparisonFilter = (LogicalComparisonFilter) eqFilter;
        assertEquals("a", comparisonFilter.getField());
        assertEquals(BsonType.INT32, comparisonFilter.getValue().getBsonType());
        assertEquals(20, comparisonFilter.getValue().getValue());
    }

    @Test
    void test_prepareLogicalPlan4() {
        LogicalPlanner optimizer = new LogicalPlanner(testBucket,
                "{ status: 'ALIVE', username: 'kronotop-admin' }"
        );
        LogicalNode node = optimizer.plan();

        assertInstanceOf(LogicalFullBucketScan.class, node);

        LogicalFullBucketScan fullBucketScan = (LogicalFullBucketScan)node;

        assertThat(fullBucketScan.getBucket()).isEqualTo(testBucket);
        assertEquals(2, fullBucketScan.getFilters().size());

        {
            LogicalFilter eqFilter = fullBucketScan.getFilters().get(0);
            assertInstanceOf(LogicalComparisonFilter.class, eqFilter);

            assertEquals(OperatorType.EQ, eqFilter.getOperatorType());

            LogicalComparisonFilter comparisonFilter = (LogicalComparisonFilter) eqFilter;
            assertEquals("status", comparisonFilter.getField());
            assertEquals(BsonType.STRING, comparisonFilter.getValue().getBsonType());
            assertEquals("ALIVE", comparisonFilter.getValue().getValue());
        }

        {
            LogicalFilter eqFilter = fullBucketScan.getFilters().get(1);
            assertInstanceOf(LogicalComparisonFilter.class, eqFilter);

            assertEquals(OperatorType.EQ, eqFilter.getOperatorType());

            LogicalComparisonFilter comparisonFilter = (LogicalComparisonFilter) eqFilter;
            assertEquals("username", comparisonFilter.getField());
            assertEquals(BsonType.STRING, comparisonFilter.getValue().getBsonType());
            assertEquals("kronotop-admin", comparisonFilter.getValue().getValue());
        }
    }

    @Test
    public void when_no_child_expression() {
        LogicalPlanner optimizer = new LogicalPlanner(testBucket, "{}");
        LogicalNode node = optimizer.plan();
        assertInstanceOf(LogicalFullBucketScan.class, node);
        assertTrue(node.getFilters().isEmpty());
        LogicalFullBucketScan logicalFullBucketScan = (LogicalFullBucketScan) node;
        assertEquals(testBucket, logicalFullBucketScan.getBucket());
        // bucket.insert <bucket-name> <document> <document>
    }

    @Test
    void when_implicit_EQ_operator() {
        LogicalPlanner optimizer = new LogicalPlanner(testBucket, "{ status: 'ALIVE' }");
        LogicalNode node = optimizer.plan();
        assertInstanceOf(LogicalFullBucketScan.class, node);
        assertEquals(1, node.getFilters().size());

        LogicalFullBucketScan logicalFullBucketScan = (LogicalFullBucketScan) node;
        LogicalNode logicalNode = logicalFullBucketScan.getFilters().getFirst();
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
    void when_explicit_EQ_operator() {
        LogicalPlanner optimizer = new LogicalPlanner(testBucket, "{ status: { $eq: 'ALIVE' } }");
        LogicalNode node = optimizer.plan();
        assertInstanceOf(LogicalFullBucketScan.class, node);
        assertEquals(1, node.getFilters().size());

        LogicalFullBucketScan logicalFullBucketScan = (LogicalFullBucketScan) node;
        LogicalNode logicalNode = logicalFullBucketScan.getFilters().getFirst();
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
    void when_multiple_EQ_operator() {
        LogicalPlanner optimizer = new LogicalPlanner(testBucket, "{ status: { $eq: 'ALIVE' }, qty: { $lt: 30 } }");
        LogicalNode node = optimizer.plan();
        assertInstanceOf(LogicalFullBucketScan.class, node);
        System.out.println(node.getFilters());
        assertEquals(2, node.getFilters().size());

        LogicalFullBucketScan logicalFullBucketScan = (LogicalFullBucketScan) node;
        {
            LogicalNode logicalNode = logicalFullBucketScan.getFilters().getFirst();
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
            LogicalNode logicalNode = logicalFullBucketScan.getFilters().get(1);
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
}