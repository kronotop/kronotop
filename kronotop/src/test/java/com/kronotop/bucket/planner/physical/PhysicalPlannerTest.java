// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.DefaultIndex;
import com.kronotop.bucket.ReservedFieldName;
import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.planner.PlannerContext;
import com.kronotop.bucket.planner.logical.LogicalNode;
import com.kronotop.bucket.planner.logical.LogicalPlanner;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class PhysicalPlannerTest {
    final String testBucket = "test-bucket";

    @Test
    void indexed_field_id_string_gte() {
        LogicalPlanner logical = new LogicalPlanner(
                testBucket,
                "{ _id: { $gte: '00010CRQ5VIMO0000000xxxx'} }"
        );
        LogicalNode logicalNode = logical.plan();

        Map<String, Index> indexes = Map.of(
                ReservedFieldName.ID.getValue(), new Index(DefaultIndex.ID.getValue(), BsonType.STRING)
        );
        PlannerContext context = new PlannerContext(indexes);
        PhysicalPlanner physical = new PhysicalPlanner(context, logicalNode);
        PhysicalNode physicalNode = physical.plan();

        /*
        LogicalFullBucketScan {
            bucket=test-bucket,
            filters=[
                LogicalComparisonFilter {
                    operatorType=GTE,
                    field=_id,
                    value=BqlValue { type=STRING, value=00010CRQ5VIMO0000000xxxx }
                }
            ]
        }

        PhysicalIndexScan {
            bucket=test-bucket,
            index="_id_idx",
            operatorType=GTE,
            field=_id,
            value=BqlValue { type=STRING, value=00010CRQ5VIMO0000000xxxx }
        }
        */

        assertInstanceOf(PhysicalIndexScan.class, physicalNode);

        PhysicalIndexScan physicalIndexScan = (PhysicalIndexScan) physicalNode;
        assertEquals(testBucket, physicalIndexScan.getBucket());
        assertEquals(DefaultIndex.ID.getValue(), physicalIndexScan.getIndex());
        assertEquals(OperatorType.GTE, physicalIndexScan.getOperatorType());
        assertEquals(BsonType.STRING, physicalIndexScan.getValue().getBsonType());
        assertEquals("00010CRQ5VIMO0000000xxxx", physicalIndexScan.getValue().getValue());
    }

    @Test
    void full_scan_int32_field_gte() {
        LogicalPlanner logical = new LogicalPlanner(
                testBucket,
                "{ a: { $gte: 20 } }"
        );
        LogicalNode logicalNode = logical.plan();
        PhysicalPlanner physical = new PhysicalPlanner(new PlannerContext(), logicalNode);
        PhysicalNode physicalNode = physical.plan();

        assertInstanceOf(PhysicalFullScan.class, physicalNode);

        PhysicalFullScan physicalFullScan = (PhysicalFullScan) physicalNode;
        assertEquals(testBucket, physicalFullScan.getBucket());
        assertEquals(OperatorType.GTE, physicalFullScan.getOperatorType());
        assertEquals(BsonType.INT32, physicalFullScan.getValue().getBsonType());
        assertEquals(20, physicalFullScan.getValue().getValue());
    }

    @Test
    void double_scan_int32_field_gte() {
        LogicalPlanner logical = new LogicalPlanner(
                testBucket,
                "{ a: { $gte: 20 }, b: { $eq: 'string-value' } }"
        );
        LogicalNode logicalNode = logical.plan();

        Map<String, Index> indexes = Map.of(
                "a", new Index("a_idx", BsonType.INT32),
                "b", new Index("b_idx", BsonType.STRING)
        );
        PhysicalPlanner physical = new PhysicalPlanner(new PlannerContext(indexes), logicalNode);
        PhysicalNode physicalNode = physical.plan();
        System.out.println(physicalNode);
    }

    @Test
    void foo() {
        LogicalPlanner logical = new LogicalPlanner(
                testBucket,
                "{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: 30 } } ] }"
        );
        LogicalNode logicalNode = logical.plan();

        Map<String, Index> indexes = Map.of(
                "status", new Index("status_idx", BsonType.STRING),
                "qty", new Index("qty_idx", BsonType.INT32)
        );
        PhysicalPlanner physical = new PhysicalPlanner(new PlannerContext(indexes), logicalNode);
        PhysicalNode physicalNode = physical.plan();
        System.out.println(physicalNode);
    }
}