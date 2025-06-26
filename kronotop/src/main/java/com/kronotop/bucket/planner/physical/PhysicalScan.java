// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.planner.Bounds;

import java.util.List;

public class PhysicalScan extends PhysicalFilter {
    private Bounds bounds;
    private String field;

    public PhysicalScan() {
        super();
    }

    public PhysicalScan(List<PhysicalNode> children) {
        super(children);
    }

    public Bounds getBounds() {
        return bounds;
    }

    public void setBounds(Bounds bounds) {
        this.bounds = bounds;
    }

    public String getField() {
        return field;
    }

    void setField(String field) {
        this.field = field;
    }
}
