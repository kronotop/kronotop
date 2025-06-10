// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.planner.PlannerException;

public class MergeOverlappingBoundaries implements PhysicalOptimizationStage {

    private PhysicalNode processIntersectionOperator(PhysicalIntersectionOperator operator) {
        for (PhysicalNode node : operator.getChildren()) {
            System.out.println(node);
        }
        return operator;
    }

    @Override
    public PhysicalNode optimize(PhysicalNode node) {
        switch (node) {
            case PhysicalIntersectionOperator operator:
                return processIntersectionOperator(operator);
            default:
                throw new PlannerException("Invalid operator: " + node.getClass().getName());
        }
    }
}

/*
PhysicalIntersectionOperator {
    children=[
        PhysicalFullScan {field=age, bounds=Bounds[lower=Bound[type=GTE, bqlValue=Int32Val[value=20]], upper=null]},
        PhysicalFullScan {field=age, bounds=Bounds[lower=null, upper=Bound[type=LTE, bqlValue=Int32Val[value=30]]]}
    ]
}
 */