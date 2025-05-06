// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.bql.values;

import com.apple.foundationdb.tuple.Versionstamp;
import org.bson.BsonType;

public record VersionstampVal(Versionstamp vs) implements BqlValue<Versionstamp> {
    public static final CustomType TYPE = new CustomType((byte) 0x80, "VERSIONSTAMP");

    @Override
    public Versionstamp value() {
        return vs;
    }

    @Override
    public BsonType bsonType() {
        return null;
    }

    @Override
    public CustomType customType() {
        return TYPE;
    }
}