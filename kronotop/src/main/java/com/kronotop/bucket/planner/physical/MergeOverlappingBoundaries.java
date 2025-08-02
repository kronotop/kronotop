/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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