// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.planner.PlannerContext;
import com.kronotop.bucket.planner.TestQuery;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PhysicalPlannerTest {

    private LogicalNode getLogicalPlan(String query) {
        LogicalPlanner logical = new LogicalPlanner(query);
        return logical.plan();
    }

    @Test
    void when_planning_no_child_expression() {
        LogicalNode logicalNode = getLogicalPlan(TestQuery.NO_CHILD_EXPRESSION);
        PhysicalPlanner physical = new PhysicalPlanner(new PlannerContext(), logicalNode);
        PhysicalNode physicalNode = physical.plan();

        assertInstanceOf(PhysicalFullScan.class, physicalNode);
        PhysicalFullScan physicalFullScan = (PhysicalFullScan) physicalNode;
        assertNull(physicalFullScan.getOperatorType());
        assertNull(physicalFullScan.bqlValue());
        assertNull(physicalFullScan.getField());
    }

    @Test
    void when_planning_single_field_with_string_type_and_gte() {
        LogicalNode logicalNode = getLogicalPlan(TestQuery.SINGLE_FIELD_WITH_STRING_TYPE_AND_GTE);
        Map<String, Index> indexes = Map.of(
                "a", new Index("a_idx", "a", BsonType.STRING)
        );
        PlannerContext context = new PlannerContext(indexes);
        PhysicalPlanner physical = new PhysicalPlanner(context, logicalNode);
        PhysicalNode physicalNode = physical.plan();

        assertInstanceOf(PhysicalIndexScan.class, physicalNode);

        PhysicalIndexScan physicalIndexScan = (PhysicalIndexScan) physicalNode;
        assertEquals("a_idx", physicalIndexScan.getIndex().name());
        assertEquals(OperatorType.GTE, physicalIndexScan.getOperatorType());
        assertEquals(BsonType.STRING, physicalIndexScan.bqlValue().bsonType());
        assertEquals("string-value", physicalIndexScan.bqlValue().value());
    }

    @Test
    void when_planning_single_field_with_int32_type_and_eq() {
        // FULL SCAN
        LogicalNode logicalNode = getLogicalPlan(TestQuery.SINGLE_FIELD_WITH_IN32_TYPE_AND_EQ);
        PhysicalPlanner physical = new PhysicalPlanner(new PlannerContext(), logicalNode);
        PhysicalNode physicalNode = physical.plan();

        assertInstanceOf(PhysicalFullScan.class, physicalNode);

        PhysicalFullScan physicalFullScan = (PhysicalFullScan) physicalNode;
        assertEquals(OperatorType.EQ, physicalFullScan.getOperatorType());
        assertEquals(BsonType.INT32, physicalFullScan.bqlValue().bsonType());
        assertEquals(20, physicalFullScan.bqlValue().value());
    }

    @Test
    void intersection_operator_with_two_indexed_fields() {
        LogicalNode logicalNode = getLogicalPlan("{ a: { $gte: 20 }, b: { $eq: 'string-value' } }");
        Map<String, Index> indexes = Map.of(
                "a", new Index("a_idx", "a", BsonType.INT32),
                "b", new Index("b_idx", "b", BsonType.STRING)
        );
        PhysicalPlanner physical = new PhysicalPlanner(new PlannerContext(indexes), logicalNode);
        PhysicalNode physicalNode = physical.plan();

        PhysicalIntersectionOperator physicalIntersectionOperator = (PhysicalIntersectionOperator) physicalNode;

        {
            PhysicalIndexScan physicalIndexScan = (PhysicalIndexScan) physicalIntersectionOperator.getChildren().getFirst();
            assertEquals(OperatorType.GTE, physicalIndexScan.getOperatorType());
            assertEquals(BsonType.INT32, physicalIndexScan.bqlValue().bsonType());
            assertEquals(20, physicalIndexScan.bqlValue().value());
        }

        {
            PhysicalIndexScan physicalIndexScan = (PhysicalIndexScan) physicalIntersectionOperator.getChildren().get(1);
            assertEquals(OperatorType.EQ, physicalIndexScan.getOperatorType());
            assertEquals(BsonType.STRING, physicalIndexScan.bqlValue().bsonType());
            assertEquals("string-value", physicalIndexScan.bqlValue().value());
        }
    }

    @Test
    void or_operator_with_two_indexed_fields() {
        LogicalNode logicalNode = getLogicalPlan("{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: 30 } } ] }");

        Map<String, Index> indexes = Map.of(
                "status", new Index("status_idx", "status", BsonType.STRING),
                "qty", new Index("qty_idx", "qty", BsonType.INT32)
        );
        PhysicalPlanner physical = new PhysicalPlanner(new PlannerContext(indexes), logicalNode);
        PhysicalNode physicalNode = physical.plan();

        assertInstanceOf(PhysicalUnionOperator.class, physicalNode);

        PhysicalUnionOperator physicalUnionOperator = (PhysicalUnionOperator) physicalNode;

        {
            PhysicalIndexScan physicalIndexScan = (PhysicalIndexScan) physicalUnionOperator.getChildren().getFirst();
            assertEquals(OperatorType.EQ, physicalIndexScan.getOperatorType());
            assertEquals(BsonType.STRING, physicalIndexScan.bqlValue().bsonType());
            assertEquals("A", physicalIndexScan.bqlValue().value());
        }

        {
            PhysicalIndexScan physicalIndexScan = (PhysicalIndexScan) physicalUnionOperator.getChildren().get(1);
            assertEquals(OperatorType.LT, physicalIndexScan.getOperatorType());
            assertEquals(BsonType.INT32, physicalIndexScan.bqlValue().bsonType());
            assertEquals(30, physicalIndexScan.bqlValue().value());
        }
    }

    @Test
    void or_operator_with_two_not_indexed_fields() {
        LogicalNode logicalNode = getLogicalPlan("{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: 30 } } ] }");
        PhysicalPlanner physical = new PhysicalPlanner(new PlannerContext(), logicalNode);
        PhysicalNode physicalNode = physical.plan();

        assertInstanceOf(PhysicalUnionOperator.class, physicalNode);

        PhysicalUnionOperator physicalUnionOperator = (PhysicalUnionOperator) physicalNode;

        {
            PhysicalFullScan physicalFullScan = (PhysicalFullScan) physicalUnionOperator.getChildren().getFirst();
            assertEquals(OperatorType.EQ, physicalFullScan.getOperatorType());
            assertEquals(BsonType.STRING, physicalFullScan.bqlValue().bsonType());
            assertEquals("A", physicalFullScan.bqlValue().value());
        }

        {
            PhysicalFullScan physicalFullScan = (PhysicalFullScan) physicalUnionOperator.getChildren().get(1);
            assertEquals(OperatorType.LT, physicalFullScan.getOperatorType());
            assertEquals(BsonType.INT32, physicalFullScan.bqlValue().bsonType());
            assertEquals(30, physicalFullScan.bqlValue().value());
        }
    }

    @Test
    void or_operator_with_only_one_indexed_field() {
        LogicalNode logicalNode = getLogicalPlan("{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: 30 } } ] }");
        Map<String, Index> indexes = Map.of(
                "status", new Index("status_idx", "status", BsonType.STRING)
        );
        PhysicalPlanner physical = new PhysicalPlanner(new PlannerContext(indexes), logicalNode);
        PhysicalNode physicalNode = physical.plan();

        assertInstanceOf(PhysicalUnionOperator.class, physicalNode);

        PhysicalUnionOperator physicalUnionOperator = (PhysicalUnionOperator) physicalNode;

        {
            PhysicalIndexScan physicalIndexScan = (PhysicalIndexScan) physicalUnionOperator.getChildren().getFirst();
            assertEquals(OperatorType.EQ, physicalIndexScan.getOperatorType());
            assertEquals(BsonType.STRING, physicalIndexScan.bqlValue().bsonType());
            assertEquals("A", physicalIndexScan.bqlValue().value());
        }

        {
            PhysicalFullScan physicalFullScan = (PhysicalFullScan) physicalUnionOperator.getChildren().get(1);
            assertEquals(OperatorType.LT, physicalFullScan.getOperatorType());
            assertEquals(BsonType.INT32, physicalFullScan.bqlValue().bsonType());
            assertEquals(30, physicalFullScan.bqlValue().value());
        }
    }

    @Test
    void when_planning_complex_query_one() {
        LogicalNode logicalNode = getLogicalPlan(TestQuery.COMPLEX_QUERY_ONE);
        PhysicalPlanner physical = new PhysicalPlanner(new PlannerContext(), logicalNode);
        PhysicalNode physicalNode = physical.plan();

        assertInstanceOf(PhysicalIntersectionOperator.class, physicalNode);
        PhysicalIntersectionOperator physicalIntersectionOperator = (PhysicalIntersectionOperator) physicalNode;
        assertEquals(2, physicalIntersectionOperator.getChildren().size());

        {
            assertInstanceOf(PhysicalUnionOperator.class, physicalIntersectionOperator.getChildren().getFirst());
            PhysicalUnionOperator physicalUnionOperator = (PhysicalUnionOperator) physicalIntersectionOperator.getChildren().getFirst();
            assertEquals(2, physicalUnionOperator.getChildren().size());
            {
                assertInstanceOf(PhysicalFullScan.class, physicalUnionOperator.getChildren().getFirst());
                PhysicalFullScan physicalFullScan = (PhysicalFullScan) physicalUnionOperator.getChildren().getFirst();
                assertEquals("qty", physicalFullScan.getField());
                assertEquals(OperatorType.LT, physicalFullScan.getOperatorType());
                assertEquals(BsonType.INT32, physicalFullScan.bqlValue().bsonType());
                assertEquals(10, physicalFullScan.bqlValue().value());
            }

            {
                assertInstanceOf(PhysicalFullScan.class, physicalUnionOperator.getChildren().get(1));
                PhysicalFullScan physicalFullScan = (PhysicalFullScan) physicalUnionOperator.getChildren().get(1);
                assertEquals("qty", physicalFullScan.getField());
                assertEquals(OperatorType.GT, physicalFullScan.getOperatorType());
                assertEquals(BsonType.INT32, physicalFullScan.bqlValue().bsonType());
                assertEquals(50, physicalFullScan.bqlValue().value());
            }
        }

        {
            assertInstanceOf(PhysicalUnionOperator.class, physicalIntersectionOperator.getChildren().get(1));
            PhysicalUnionOperator physicalUnionOperator = (PhysicalUnionOperator) physicalIntersectionOperator.getChildren().get(1);
            assertEquals(2, physicalUnionOperator.getChildren().size());
            {
                assertInstanceOf(PhysicalFullScan.class, physicalUnionOperator.getChildren().getFirst());
                PhysicalFullScan physicalFullScan = (PhysicalFullScan) physicalUnionOperator.getChildren().getFirst();
                assertEquals("sale", physicalFullScan.getField());
                assertEquals(OperatorType.EQ, physicalFullScan.getOperatorType());
                assertEquals(BsonType.BOOLEAN, physicalFullScan.bqlValue().bsonType());
                assertTrue((Boolean) physicalFullScan.bqlValue().value());
            }

            {
                assertInstanceOf(PhysicalFullScan.class, physicalUnionOperator.getChildren().get(1));
                PhysicalFullScan physicalFullScan = (PhysicalFullScan) physicalUnionOperator.getChildren().get(1);
                assertEquals("price", physicalFullScan.getField());
                assertEquals(OperatorType.LT, physicalFullScan.getOperatorType());
                assertEquals(BsonType.INT32, physicalFullScan.bqlValue().bsonType());
                assertEquals(5, physicalFullScan.bqlValue().value());
            }
        }
    }

    @Test
    void when_planning_complex_query_one_with_indexes() {
        Map<String, Index> indexes = Map.of(
                "sale", new Index("sale_idx", "sale", BsonType.BOOLEAN),
                "price", new Index("price_idx", "price", BsonType.INT32),
                "qty", new Index("qty_idx", "qty", BsonType.INT32)
        );
        LogicalNode logicalNode = getLogicalPlan(TestQuery.COMPLEX_QUERY_ONE);
        PhysicalPlanner physical = new PhysicalPlanner(new PlannerContext(indexes), logicalNode);
        PhysicalNode physicalNode = physical.plan();

        assertInstanceOf(PhysicalIntersectionOperator.class, physicalNode);
        PhysicalIntersectionOperator physicalIntersectionOperator = (PhysicalIntersectionOperator) physicalNode;
        assertEquals(2, physicalIntersectionOperator.getChildren().size());

        {
            assertInstanceOf(PhysicalUnionOperator.class, physicalIntersectionOperator.getChildren().getFirst());
            PhysicalUnionOperator physicalUnionOperator = (PhysicalUnionOperator) physicalIntersectionOperator.getChildren().getFirst();
            assertEquals(2, physicalUnionOperator.getChildren().size());
            {
                assertInstanceOf(PhysicalIndexScan.class, physicalUnionOperator.getChildren().getFirst());
                PhysicalIndexScan physicalIndexScan = (PhysicalIndexScan) physicalUnionOperator.getChildren().getFirst();
                assertEquals("qty", physicalIndexScan.getField());
                assertEquals("qty_idx", physicalIndexScan.getIndex().name());
                assertEquals(OperatorType.LT, physicalIndexScan.getOperatorType());
                assertEquals(BsonType.INT32, physicalIndexScan.bqlValue().bsonType());
                assertEquals(10, physicalIndexScan.bqlValue().value());
            }

            {
                assertInstanceOf(PhysicalIndexScan.class, physicalUnionOperator.getChildren().get(1));
                PhysicalIndexScan physicalIndexScan = (PhysicalIndexScan) physicalUnionOperator.getChildren().get(1);
                assertEquals("qty", physicalIndexScan.getField());
                assertEquals("qty_idx", physicalIndexScan.getIndex().name());
                assertEquals(OperatorType.GT, physicalIndexScan.getOperatorType());
                assertEquals(BsonType.INT32, physicalIndexScan.bqlValue().bsonType());
                assertEquals(50, physicalIndexScan.bqlValue().value());
            }
        }

        {
            assertInstanceOf(PhysicalUnionOperator.class, physicalIntersectionOperator.getChildren().get(1));
            PhysicalUnionOperator physicalUnionOperator = (PhysicalUnionOperator) physicalIntersectionOperator.getChildren().get(1);
            assertEquals(2, physicalUnionOperator.getChildren().size());
            {
                assertInstanceOf(PhysicalIndexScan.class, physicalUnionOperator.getChildren().getFirst());
                PhysicalIndexScan physicalIndexScan = (PhysicalIndexScan) physicalUnionOperator.getChildren().getFirst();
                assertEquals("sale", physicalIndexScan.getField());
                assertEquals("sale_idx", physicalIndexScan.getIndex().name());
                assertEquals(OperatorType.EQ, physicalIndexScan.getOperatorType());
                assertEquals(BsonType.BOOLEAN, physicalIndexScan.bqlValue().bsonType());
                assertTrue((Boolean) physicalIndexScan.bqlValue().value());
            }

            {
                assertInstanceOf(PhysicalIndexScan.class, physicalUnionOperator.getChildren().get(1));
                PhysicalIndexScan physicalIndexScan = (PhysicalIndexScan) physicalUnionOperator.getChildren().get(1);
                assertEquals("price", physicalIndexScan.getField());
                assertEquals("price_idx", physicalIndexScan.getIndex().name());
                assertEquals(OperatorType.LT, physicalIndexScan.getOperatorType());
                assertEquals(BsonType.INT32, physicalIndexScan.bqlValue().bsonType());
                assertEquals(5, physicalIndexScan.bqlValue().value());
            }
        }
    }
}