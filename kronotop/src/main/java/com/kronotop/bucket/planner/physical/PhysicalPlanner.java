// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.planner.Bound;
import com.kronotop.bucket.planner.Bounds;
import com.kronotop.bucket.planner.PlannerContext;
import com.kronotop.bucket.planner.logical.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PhysicalPlanner {
    private final List<PhysicalOptimizationStage> stages;
    private final PlannerContext context;
    private final LogicalNode root;

    public PhysicalPlanner(PlannerContext context, LogicalNode root, List<PhysicalOptimizationStage> stages) {
        this.context = context;
        this.root = root;
        this.stages = stages;
    }

    public PhysicalPlanner(PlannerContext context, LogicalNode root) {
        this(context, root, List.of());
    }

    /**
     * Computes the bounds of the provided logical comparison filter based on its operator type.
     *
     * @param filter the logical comparison filter whose bounds are to be determined
     * @return a Bounds object representing the lower and upper bounds derived from the filter's operator type
     * @throws IllegalStateException if the filter has an unexpected or unsupported operator type
     */
    private Bounds prepareBounds(LogicalComparisonFilter filter) {
        return switch (filter.getOperatorType()) {
            case EQ -> new Bounds(
                    new Bound(filter.getOperatorType(), filter.bqlValue()),
                    new Bound(filter.getOperatorType(), filter.bqlValue())
            );
            case GT, GTE -> new Bounds(new Bound(filter.getOperatorType(), filter.bqlValue()), null);
            case LT, LTE -> new Bounds(null, new Bound(filter.getOperatorType(), filter.bqlValue()));
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
                        PhysicalIndexScan physicalIndexScan = new PhysicalIndexScan(index);
                        physicalIndexScan.setField(logicalFilter.getField());
                        physicalIndexScan.setBounds(prepareBounds(logicalFilter));
                        nodes.add(physicalIndexScan);
                    } else {
                        PhysicalFullScan physicalFullScan = new PhysicalFullScan();
                        physicalFullScan.setField(logicalFilter.getField());
                        physicalFullScan.setBounds(prepareBounds(logicalFilter));
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

    private PhysicalNode convertLogicalPlan() {
        if (Objects.requireNonNull(root) instanceof LogicalFullScan node) {
            List<PhysicalNode> result = traverse(node.getChildren());
            if (result.isEmpty()) {
                return new PhysicalFullScan(List.of());
            } else if (result.size() == 1) {
                return result.getFirst();
            }
            return new PhysicalIntersectionOperator(result);
        }
        throw new IllegalStateException("Unexpected value: " + root);
    }

    public PhysicalNode plan() {
        PhysicalNode node = convertLogicalPlan();
        for (PhysicalOptimizationStage stage : stages) {
            node = stage.optimize(node);
        }
        return node;
    }
}
