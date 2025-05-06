// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.bql.values;

import org.bson.BsonType;

/* ---- Catch‑all: ---- */
public non-sealed class CustomVal<T> implements BqlValue<T> {
    private final T value;
    private final CustomType type;

    public CustomVal(T value, CustomType type) {
        this.value = value;
        this.type = type;
    }

    public T value() {
        return value;
    }

    public BsonType bsonType() {
        return null;
    }

    public CustomType customType() {
        return type;
    }
}