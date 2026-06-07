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

import com.kronotop.internal.JSONUtil;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CompoundIndexDefinitionTest {

    private List<CompoundIndexField> createTestFields() {
        return List.of(
                new CompoundIndexField("user.name", BsonType.STRING, false),
                new CompoundIndexField("user.age", BsonType.INT32, false)
        );
    }

    @Test
    void shouldEncodeDecode() {
        // Behavior: A CompoundIndexDefinition should survive JSON round-trip serialization.
        CompoundIndexDefinition index = CompoundIndexDefinition.create("compound_idx", createTestFields(), IndexStatus.WAITING);
        byte[] data = JSONUtil.writeValueAsBytes(index);

        CompoundIndexDefinition decoded = JSONUtil.readValue(data, CompoundIndexDefinition.class);

        assertEquals(index.id(), decoded.id());
        assertEquals(index.name(), decoded.name());
        assertEquals(index.fields().size(), decoded.fields().size());
        assertEquals(index.status(), decoded.status());

        for (int i = 0; i < index.fields().size(); i++) {
            assertEquals(index.fields().get(i).selector(), decoded.fields().get(i).selector());
            assertEquals(index.fields().get(i).bsonType(), decoded.fields().get(i).bsonType());
            assertEquals(index.fields().get(i).multiKey(), decoded.fields().get(i).multiKey());
        }
    }

    @Test
    void shouldUpdateStatus() {
        // Behavior: updateStatus returns a new instance with the given status.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("compound_idx", createTestFields(), IndexStatus.WAITING);
        CompoundIndexDefinition updated = definition.updateStatus(IndexStatus.BUILDING);
        assertEquals(IndexStatus.BUILDING, updated.status());
    }

    @Test
    void shouldNotUpdateStatusFromDroppedToOtherStatus() {
        // Behavior: A DROPPED index cannot transition to any other status.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("compound_idx", createTestFields(), IndexStatus.WAITING);
        CompoundIndexDefinition dropped = definition.updateStatus(IndexStatus.DROPPED);
        assertEquals(IndexStatus.DROPPED, dropped.status());

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
                dropped.updateStatus(IndexStatus.READY)
        );
        assertEquals("Index 'compound_idx' is already dropped and its status cannot be modified.", exception.getMessage());
    }

    @Test
    void shouldAllowUpdatingDroppedStatusToDroppedAgain() {
        // Behavior: Updating a DROPPED index to DROPPED again is idempotent and does not throw.
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("compound_idx", createTestFields(), IndexStatus.WAITING);
        CompoundIndexDefinition dropped = definition.updateStatus(IndexStatus.DROPPED);
        assertEquals(IndexStatus.DROPPED, dropped.status());

        CompoundIndexDefinition droppedAgain = dropped.updateStatus(IndexStatus.DROPPED);
        assertEquals(IndexStatus.DROPPED, droppedAgain.status());
    }

    @Test
    void shouldRequireAtLeastTwoFields() {
        // Behavior: A compound index with fewer than 2 fields is rejected.
        List<CompoundIndexField> singleField = List.of(
                new CompoundIndexField("user.name", BsonType.STRING, false)
        );

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                CompoundIndexDefinition.create("compound_idx", singleField, IndexStatus.WAITING)
        );
        assertEquals("Compound index requires at least 2 fields, got 1", exception.getMessage());
    }

    @Test
    void shouldStoreFieldsImmutably() {
        // Behavior: The fields list is defensively copied and cannot be mutated externally.
        List<CompoundIndexField> mutableFields = new ArrayList<>(createTestFields());
        CompoundIndexDefinition definition = CompoundIndexDefinition.create("compound_idx", mutableFields, IndexStatus.WAITING);

        assertThrows(UnsupportedOperationException.class, () ->
                definition.fields().add(new CompoundIndexField("extra", BsonType.STRING, false))
        );
    }
}
