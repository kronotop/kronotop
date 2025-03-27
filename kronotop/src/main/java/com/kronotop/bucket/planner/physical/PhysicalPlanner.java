// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.ReservedFieldName;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.planner.PlannerContext;
import com.kronotop.bucket.planner.logical.LogicalComparisonFilter;
import com.kronotop.bucket.planner.logical.LogicalFullBucketScan;
import com.kronotop.bucket.planner.logical.LogicalNode;

import java.util.ArrayList;
import java.util.List;

public class PhysicalPlanner {
    private final PlannerContext context;
    private final LogicalNode root;

    public PhysicalPlanner(PlannerContext context, LogicalNode root) {
        this.context = context;
        this.root = root;
    }

    private PhysicalNode convertFullBucketScan(LogicalFullBucketScan node) {
        List<PhysicalFilter> nodes = new ArrayList<>();
        node.getFilters().forEach(filter -> {
            switch (filter) {
                case LogicalComparisonFilter f -> {
                    Index index = context.indexes().get(f.getField());
                    if (index != null) {
                        PhysicalIndexScan physicalIndexScan = new PhysicalIndexScan(
                                node.getBucket(),
                                index.name(),
                                f.getOperatorType()
                        );
                        physicalIndexScan.setField(f.getField());
                        physicalIndexScan.addValue(f.getValue());
                        nodes.add(physicalIndexScan);
                    } else {
                        PhysicalFullScan physicalFullScan = new PhysicalFullScan(node.getBucket(), f.getOperatorType());
                        physicalFullScan.setField(f.getField());
                        physicalFullScan.addValue(f.getValue());
                        nodes.add(physicalFullScan);
                    }
                }
                default -> throw new IllegalStateException("Unexpected value: " + filter);
            }
        });
        if (nodes.size() == 1) {
            return nodes.getFirst();
        }
        // And operator
        return new PhysicalIntersectionOperator(nodes);
    }

    public PhysicalNode plan() {
        switch (root) {
            case LogicalFullBucketScan node -> {
                return convertFullBucketScan(node);
            }
            default -> throw new IllegalStateException("Unexpected value: " + root);
        }
    }
}