// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.bql.values;

import org.bson.BsonType;

public record Int32Val(Integer value) implements BqlValue<Integer> {
    @Override
    public BsonType bsonType() {
        return BsonType.INT32;
    }

    @Override
    public CustomType customType() {
        return null;
    }
}

