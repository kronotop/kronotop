/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexDefinitionTest {
    @Test
    void shouldEncodeDecode() {
        // Behavior: a single-field definition survives a JSON round-trip, including the unique flag.
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("index-name", "_id", BsonType.BINARY, false, IndexStatus.WAITING, null, true);
        byte[] data = JSONUtil.writeValueAsBytes(index);

        SingleFieldIndexDefinition decoded = JSONUtil.readValue(data, SingleFieldIndexDefinition.class);

        assertEquals(index.id(), decoded.id());
        assertEquals(index.name(), decoded.name());
        assertEquals(index.selector(), decoded.selector());
        assertEquals(index.bsonType(), decoded.bsonType());
        assertTrue(decoded.unique());
    }

    @Test
    void shouldDefaultUniqueToFalse() {
        // Behavior: factory overloads without a unique argument default the flag to false.
        assertFalse(SingleFieldIndexDefinition.create("index-name", "_id", BsonType.INT32, false, IndexStatus.WAITING).unique());
        assertFalse(SingleFieldIndexDefinition.create("index-name", "_id", BsonType.INT32, false, IndexStatus.WAITING, null).unique());
    }

    @Test
    void shouldPreserveUniqueOnStatusUpdate() {
        // Behavior: updateStatus keeps the unique flag unchanged.
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("index-name", "_id", BsonType.INT32, false, IndexStatus.WAITING, null, true);
        assertTrue(definition.updateStatus(IndexStatus.BUILDING).unique());
    }

    @Test
    void shouldRejectUniqueMultiKeyViaConstructor() {
        // Behavior: the canonical constructor rejects a unique multi-key index, so no path
        // (direct construction, deserialization) can produce this invariant-breaking definition.
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                new SingleFieldIndexDefinition(1L, "i", "f", BsonType.STRING, true, IndexStatus.WAITING, null, true));
        assertEquals("A unique index cannot be multi-key", exception.getMessage());
    }

    @Test
    void shouldRejectUniqueMultiKeyViaFactory() {
        // Behavior: the factory also rejects a unique multi-key index.
        assertThrows(IllegalArgumentException.class, () ->
                SingleFieldIndexDefinition.create("i", "f", BsonType.STRING, true, IndexStatus.WAITING, null, true));
    }

    @Test
    void DECIMAL128NotImplementedYet() {
        assertThrows(NotImplementedException.class, () -> SingleFieldIndexDefinition.create("index-name", "_id", BsonType.DECIMAL128, false, IndexStatus.WAITING));
    }

    @Test
    void shouldUpdateStatus() {
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("index-name", "_id", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition updated = definition.updateStatus(IndexStatus.BUILDING);
        assertEquals(IndexStatus.BUILDING, updated.status());
    }

    @Test
    void shouldNotUpdateStatusFromDroppedToOtherStatus() {
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("index-name", "_id", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition dropped = definition.updateStatus(IndexStatus.DROPPED);
        assertEquals(IndexStatus.DROPPED, dropped.status());

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
                dropped.updateStatus(IndexStatus.READY)
        );
        assertEquals("Index 'index-name' is already dropped and its status cannot be modified.", exception.getMessage());
    }

    @Test
    void shouldAllowUpdatingDroppedStatusToDroppedAgain() {
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("index-name", "_id", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition dropped = definition.updateStatus(IndexStatus.DROPPED);
        assertEquals(IndexStatus.DROPPED, dropped.status());

        // Idempotent operation - should not throw
        SingleFieldIndexDefinition droppedAgain = dropped.updateStatus(IndexStatus.DROPPED);
        assertEquals(IndexStatus.DROPPED, droppedAgain.status());
    }
}