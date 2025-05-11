// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.bql.values.BqlValue;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.planner.Bounds;
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

    /**
     * Computes the bounds of the provided logical comparison filter based on its operator type.
     *
     * @param filter the logical comparison filter whose bounds are to be determined
     * @return a Bounds object representing the lower and upper bounds derived from the filter's operator type
     * @throws IllegalStateException if the filter has an unexpected or unsupported operator type
     */
    private Bounds getBounds(LogicalComparisonFilter filter) {
        return switch (filter.getOperatorType()) {
            case EQ -> new Bounds(filter.bqlValue(), filter.bqlValue());
            case GT, GTE -> new Bounds(filter.bqlValue(), null);
            case LT, LTE -> new Bounds(null, filter.bqlValue());
            default -> throw new IllegalStateException("Unexpected value: " + filter.getOperatorType());
        };
    }

    private List<PhysicalNode> traverse(List<LogicalNode> children) {
        List<PhysicalNode> nodes = new ArrayList<>();
        children.forEach(child -> {
            switch (child) {
                case LogicalComparisonFilter logicalFilter -> {
                    // TODO: This assumes the field hierarchy only has a single leaf and it eques to index's path
                    Index index = context.indexes().get(logicalFilter.getField());
                    if (index != null) {
                        PhysicalIndexScan physicalIndexScan = new PhysicalIndexScan(
                                index,
                                logicalFilter.getOperatorType()
                        );
                        physicalIndexScan.setField(logicalFilter.getField());
                        physicalIndexScan.setBounds(getBounds(logicalFilter));
                        nodes.add(physicalIndexScan);
                    } else {
                        PhysicalFullScan physicalFullScan = new PhysicalFullScan(logicalFilter.getOperatorType());
                        physicalFullScan.setField(logicalFilter.getField());
                        physicalFullScan.setBounds(getBounds(logicalFilter));
                        nodes.add(physicalFullScan);
                    }
                }
                case LogicalOrFilter logicalFilter -> {
                    List<PhysicalNode> result = traverse(logicalFilter.getChildren());
                    PhysicalUnionOperator node = new PhysicalUnionOperator(result);
                    nodes.add(node);
                }
                case LogicalAndFilter logicalFilter -> {
                    List<PhysicalNode> result = traverse(logicalFilter.getChildren());
                    PhysicalIntersectionOperator node = new PhysicalIntersectionOperator(result);
                    nodes.add(node);
                }
                default -> throw new IllegalStateException("Unexpected value: " + child);
            }
        });
        return nodes;
    }

    public PhysicalNode plan() {
        switch (root) {
            case LogicalFullScan node -> {
                List<PhysicalNode> result = traverse(node.getChildren());
                if (result.isEmpty()) {
                    return new PhysicalFullScan();
                } else if (result.size() == 1) {
                    return result.getFirst();
                }
                return new PhysicalIntersectionOperator(result);
            }
            default -> throw new IllegalStateException("Unexpected value: " + root);
        }
    }
}
