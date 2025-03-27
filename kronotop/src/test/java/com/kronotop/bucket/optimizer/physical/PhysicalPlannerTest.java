package com.kronotop.bucket.optimizer.physical;

import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.optimizer.logical.LogicalNode;
import com.kronotop.bucket.optimizer.logical.LogicalPlanner;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class PhysicalPlannerTest {
    final String testBucket = "test-bucket";

    @Test
    void indexed_field_id_string_gte() {
        LogicalPlanner logical = new LogicalPlanner(
                testBucket,
                "{_id: {$gte: '00010CRQ5VIMO0000000xxxx'}}"
        );
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
        LogicalNode logicalNode = logical.plan();
        PhysicalPlanner physical = new PhysicalPlanner(logicalNode);
        PhysicalNode physicalNode = physical.plan();

        assertInstanceOf(PhysicalIndexScan.class, physicalNode);

        PhysicalIndexScan physicalIndexScan = (PhysicalIndexScan) physicalNode;
        assertEquals(testBucket, physicalIndexScan.getBucket());
        assertEquals("_id_index", physicalIndexScan.getIndex());
        assertEquals(OperatorType.GTE, physicalIndexScan.getOperatorType());
        assertEquals(BsonType.STRING, physicalIndexScan.getValue().getBsonType());
        assertEquals("00010CRQ5VIMO0000000xxxx", physicalIndexScan.getValue().getValue());
    }
}