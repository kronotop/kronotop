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

import com.kronotop.NotImplementedException;
import com.kronotop.internal.JSONUtil;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IndexDefinitionTest {
    @Test
    void shouldEncodeDecode() {
        IndexDefinition index = IndexDefinition.create("index-name", "_id", BsonType.BINARY);
        byte[] data = JSONUtil.writeValueAsBytes(index);

        IndexDefinition decoded = JSONUtil.readValue(data, IndexDefinition.class);

        assertEquals(index.id(), decoded.id());
        assertEquals(index.name(), decoded.name());
        assertEquals(index.selector(), decoded.selector());
        assertEquals(index.bsonType(), decoded.bsonType());
    }

    @Test
    void DECIMAL128NotImplementedYet() {
        assertThrows(NotImplementedException.class, () -> IndexDefinition.create("index-name", "_id", BsonType.DECIMAL128));
    }

    @Test
    void shouldUpdateStatus() {
        IndexDefinition definition = IndexDefinition.create("index-name", "_id", BsonType.INT32);
        IndexDefinition updated = definition.updateStatus(IndexStatus.BUILDING);
        assertEquals(IndexStatus.BUILDING, updated.status());
    }

    @Test
    void shouldNotUpdateStatusFromDroppedToOtherStatus() {
        IndexDefinition definition = IndexDefinition.create("index-name", "_id", BsonType.INT32);
        IndexDefinition dropped = definition.updateStatus(IndexStatus.DROPPED);
        assertEquals(IndexStatus.DROPPED, dropped.status());

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
                dropped.updateStatus(IndexStatus.READY)
        );
        assertEquals("Index 'index-name' is already dropped and its status cannot be modified.", exception.getMessage());
    }

    @Test
    void shouldAllowUpdatingDroppedStatusToDroppedAgain() {
        IndexDefinition definition = IndexDefinition.create("index-name", "_id", BsonType.INT32);
        IndexDefinition dropped = definition.updateStatus(IndexStatus.DROPPED);
        assertEquals(IndexStatus.DROPPED, dropped.status());

        // Idempotent operation - should not throw
        IndexDefinition droppedAgain = dropped.updateStatus(IndexStatus.DROPPED);
        assertEquals(IndexStatus.DROPPED, droppedAgain.status());
    }
}