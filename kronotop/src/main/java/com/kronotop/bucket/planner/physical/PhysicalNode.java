// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.physical;

import java.util.ArrayList;
import java.util.List;

public class PhysicalNode {
    protected final List<PhysicalNode> children;

    public PhysicalNode() {
        this.children = new ArrayList<>();
    }

    public PhysicalNode(List<PhysicalNode> children) {
        this.children = children;
    }

    void setChildren(List<PhysicalNode> children) {
        this.children.clear();
        this.children.addAll(children);
    }

    void addChild(PhysicalNode child) {
        children.add(child);
    }

    public List<PhysicalNode> getChildren() {
        return children;
    }
}
