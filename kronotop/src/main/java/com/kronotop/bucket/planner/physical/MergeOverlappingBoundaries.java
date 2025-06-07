// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.physical;

public class MergeOverlappingBoundaries implements PhysicalOptimizationStage {
    @Override
    public PhysicalNode optimize(PhysicalNode node) {
        // group by field
        switch (node) {
            case PhysicalIntersectionOperator intersection:
                // has more than one child
                for (PhysicalNode pnode : intersection.getChildren()) {

                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + node);
        }
        return node;
    }
}

/*
PhysicalIntersectionOperator {
    children=[
        PhysicalFullScan {operatorType=GTE, field=age, bounds=Bounds[lower=Bound[type=GTE, bqlValue=Int32Val[value=20]], upper=null]},
        PhysicalFullScan {operatorType=LTE, field=age, bounds=Bounds[lower=null, upper=Bound[type=LTE, bqlValue=Int32Val[value=30]]]}
    ]
}
 */