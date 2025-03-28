// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.planner.PlannerContext;
import com.kronotop.bucket.planner.logical.*;

import java.util.ArrayList;
import java.util.List;

public class PhysicalPlanner {
    private final PlannerContext context;
    private final LogicalNode root;

    public PhysicalPlanner(PlannerContext context, LogicalNode root) {
        this.context = context;
        this.root = root;
    }

    private List<PhysicalNode> traverse(List<LogicalFilter> filters) {
        List<PhysicalNode> nodes = new ArrayList<>();
        filters.forEach(filter -> {
            switch (filter) {
                case LogicalComparisonFilter logicalFilter -> {
                    Index index = context.indexes().get(logicalFilter.getField());
                    if (index != null) {
                        PhysicalIndexScan physicalIndexScan = new PhysicalIndexScan(
                                index.name(),
                                logicalFilter.getOperatorType()
                        );
                        physicalIndexScan.setField(logicalFilter.getField());
                        physicalIndexScan.addValue(logicalFilter.getValue());
                        nodes.add(physicalIndexScan);
                    } else {
                        PhysicalFullScan physicalFullScan = new PhysicalFullScan(logicalFilter.getOperatorType());
                        physicalFullScan.setField(logicalFilter.getField());
                        physicalFullScan.addValue(logicalFilter.getValue());
                        nodes.add(physicalFullScan);
                    }
                }
                case LogicalOrFilter f -> {
                    List<PhysicalNode> result = traverse(f.getFilters());
                    PhysicalUnionOperator node = new PhysicalUnionOperator(result);
                    nodes.add(node);
                }
                default -> throw new IllegalStateException("Unexpected value: " + filter);
            }
        });
        return nodes;
    }

    public PhysicalNode plan() {
        switch (root) {
            case LogicalFullScan node -> {
                List<PhysicalNode> result = traverse(node.getFilters());
                if (result.size() == 1) {
                    return result.getFirst();
                }
                return new PhysicalIntersectionOperator(result);
            }
            default -> throw new IllegalStateException("Unexpected value: " + root);
        }
    }
}