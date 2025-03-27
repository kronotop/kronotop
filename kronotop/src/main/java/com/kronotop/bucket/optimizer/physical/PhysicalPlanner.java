// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.optimizer.physical;

import com.kronotop.bucket.optimizer.logical.LogicalComparisonFilter;
import com.kronotop.bucket.optimizer.logical.LogicalFullBucketScan;
import com.kronotop.bucket.optimizer.logical.LogicalNode;

import java.util.LinkedList;
import java.util.List;

public class PhysicalPlanner {
    private final LogicalNode root;

    public PhysicalPlanner(LogicalNode root) {
        this.root = root;
    }

    private PhysicalNode convertFullBucketScan(LogicalFullBucketScan node) {
        List<PhysicalNode> nodes = new LinkedList<>();
        node.getFilters().forEach(filter -> {
            switch (filter) {
                case LogicalComparisonFilter f -> {
                    if (f.getField().equals("_id")) {
                        IndexComparisonFilter indexComparisonFilter = new IndexComparisonFilter(node.getBucket(), "_id_index", f.getOperatorType());
                        indexComparisonFilter.setField(f.getField());
                        indexComparisonFilter.addValue(f.getValue());
                        nodes.add(indexComparisonFilter);
                    }
                }
                default -> throw new IllegalStateException("Unexpected value: " + filter);
            }
        });
        if (nodes.size() == 1) {
            return nodes.getFirst();
        }
        // TODO
        return null;
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