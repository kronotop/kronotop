// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.index;

import com.kronotop.internal.JSONUtil;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IndexDefinitionTest {
    @Test
    void shouldEncodeDecode() {
        IndexDefinition index = IndexDefinition.create("index-name", "_id", BsonType.BINARY, SortOrder.ASCENDING);
        byte[] data = JSONUtil.writeValueAsBytes(index);

        IndexDefinition decoded = JSONUtil.readValue(data, IndexDefinition.class);

        assertEquals(index.id(), decoded.id());
        assertEquals(index.name(), decoded.name());
        assertEquals(index.field(), decoded.field());
        assertEquals(index.sortOrder(), decoded.sortOrder());
        assertEquals(index.bsonType(), decoded.bsonType());
    }
}