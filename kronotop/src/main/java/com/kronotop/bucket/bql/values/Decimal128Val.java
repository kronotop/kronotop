// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.bql.values;

import org.bson.BsonType;

import java.math.BigDecimal;

public record Decimal128Val(BigDecimal value) implements BqlValue<BigDecimal> {
    @Override
    public BsonType bsonType() {
        return BsonType.DECIMAL128;
    }

    @Override
    public CustomType customType() {
        return null;
    }
}
