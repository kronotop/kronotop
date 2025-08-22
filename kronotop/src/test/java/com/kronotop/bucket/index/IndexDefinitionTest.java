/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        assertEquals(index.selector(), decoded.selector());
        assertEquals(index.sortOrder(), decoded.sortOrder());
        assertEquals(index.bsonType(), decoded.bsonType());
    }
}