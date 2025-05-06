// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.bql.values;

import org.bson.BsonType;

import javax.annotation.Nullable;

public sealed interface BqlValue<T> permits DoubleVal, StringVal, BinaryVal, BooleanVal, DateTimeVal, NullVal, Int32Val, TimestampVal, Int64Val, Decimal128Val, VersionstampVal, CustomVal {
    T value();

    @Nullable
    BsonType bsonType();

    @Nullable
    CustomType customType();
}

