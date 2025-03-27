package com.kronotop.bucket.optimizer.physical;

import com.kronotop.bucket.optimizer.logical.LogicalNode;
import com.kronotop.bucket.optimizer.logical.LogicalPlanner;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PhysicalPlannerTest {
    @Test
    void indexed_field_id_string_gte() {
        LogicalPlanner logical = new LogicalPlanner(
                "test-bucket",
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
        System.out.println(physicalNode);
    }
}