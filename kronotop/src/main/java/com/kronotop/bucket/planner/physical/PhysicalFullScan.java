// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.planner.physical;

import com.kronotop.bucket.bql.BqlValue;
import com.kronotop.bucket.bql.operators.OperatorType;

public class PhysicalFullScan extends PhysicalFilter {
    private final String bucket;
    private BqlValue<?> value;
    private String field;

    public PhysicalFullScan(String bucket, OperatorType operatorType) {
        super(operatorType);
        this.bucket = bucket;
    }

    public String getField() {
        return field;
    }

    void setField(String field) {
        this.field = field;
    }

    void addValue(BqlValue<?> value) {
        this.value = value;
    }

    public BqlValue<?> getValue() {
        return value;
    }

    public String getBucket() {
        return bucket;
    }

    @Override
    public String toString() {
        return "PhysicalFullScan {" +
                "bucket=" + bucket + ", " +
                "operatorType=" + getOperatorType() + ", " +
                "field=" + getField() + ", " +
                "value=" + getValue() + "}";
    }
}
