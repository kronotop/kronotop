// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.optimizer.physical;

import com.kronotop.bucket.optimizer.logical.LogicalFullBucketScan;
import com.kronotop.bucket.optimizer.logical.LogicalNode;

public class PhysicalPlanner {
    private final LogicalNode root;

    public PhysicalPlanner(LogicalNode root) {
        this.root = root;
    }

    private void convertFullBucketScan(LogicalFullBucketScan node) {
        System.out.println(node.getBucket());
        node.getFilters().forEach(System.out::println);
    }

    public PhysicalNode plan() {
        switch (root) {
            case LogicalFullBucketScan node -> {
                convertFullBucketScan(node);
            }
            default -> throw new IllegalStateException("Unexpected value: " + root);
        }
        return null;
    }
}
