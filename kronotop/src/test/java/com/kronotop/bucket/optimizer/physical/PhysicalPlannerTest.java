package com.kronotop.bucket.optimizer.physical;

import com.kronotop.bucket.optimizer.logical.LogicalNode;
import com.kronotop.bucket.optimizer.logical.LogicalPlanner;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PhysicalPlannerTest {
    @Test
    void test_plan() {
        LogicalPlanner logical = new LogicalPlanner(
                "test-bucket",
                "{_id: {$gte: '00010CRQ5VIMO0000000xxxx'}}"
        );
        LogicalNode node = logical.plan();
        PhysicalPlanner physical = new PhysicalPlanner(node);
        physical.plan();
    }
}