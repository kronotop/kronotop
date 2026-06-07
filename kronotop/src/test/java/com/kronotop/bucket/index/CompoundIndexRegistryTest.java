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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import org.bson.BsonType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CompoundIndexRegistryTest extends BaseStandaloneInstanceTest {

    private CompoundIndexRegistry registry;
    private DirectorySubspace testSubspace;

    private List<CompoundIndexField> testFields() {
        return List.of(
                new CompoundIndexField("category", BsonType.STRING, false),
                new CompoundIndexField("price", BsonType.DOUBLE, false)
        );
    }

    @BeforeEach
    void setUp() {
        registry = new CompoundIndexRegistry(context);
        testSubspace = createOrOpenSubspaceUnderCluster("compound-test");
    }

    @Test
    void shouldRegisterAndRetrieveByName() {
        // Behavior: A registered compound index is retrievable by name.
        CompoundIndexDefinition def = CompoundIndexDefinition.create("my_idx", testFields(), IndexStatus.WAITING);
        registry.register(def, testSubspace);
        CompoundIndex result = registry.getIndexByName("my_idx", IndexSelectionPolicy.ALL);
        assertNotNull(result);
        assertEquals(def, result.definition());
    }

    @Test
    void shouldRetrieveById() {
        // Behavior: A registered compound index is retrievable by its ID.
        CompoundIndexDefinition def = CompoundIndexDefinition.create("my_idx", testFields(), IndexStatus.WAITING);
        registry.register(def, testSubspace);
        CompoundIndex result = registry.getIndexById(def.id(), IndexSelectionPolicy.ALL);
        assertNotNull(result);
        assertEquals("my_idx", result.definition().name());
    }

    @Test
    void shouldRejectDuplicateName() {
        // Behavior: Registering two indexes with the same name throws.
        CompoundIndexDefinition def1 = CompoundIndexDefinition.create("my_idx", testFields(), IndexStatus.WAITING);
        registry.register(def1, testSubspace);

        CompoundIndexDefinition def2 = CompoundIndexDefinition.create("my_idx", testFields(), IndexStatus.WAITING);
        assertThrows(IndexAlreadyRegisteredException.class, () -> registry.register(def2, testSubspace));
    }

    @Test
    void shouldDeregister() {
        // Behavior: A deregistered compound index is no longer retrievable.
        CompoundIndexDefinition def = CompoundIndexDefinition.create("my_idx", testFields(), IndexStatus.WAITING);
        registry.register(def, testSubspace);
        registry.deregister(def);
        assertNull(registry.getIndexByName("my_idx", IndexSelectionPolicy.ALL));
    }

    @Test
    void shouldSegregateByPolicy() {
        // Behavior: Policy-based lists correctly filter by status.
        CompoundIndexDefinition waiting = CompoundIndexDefinition.create("waiting_idx", testFields(), IndexStatus.WAITING);
        CompoundIndexDefinition ready = CompoundIndexDefinition.create("ready_idx", testFields(), IndexStatus.READY);
        CompoundIndexDefinition building = CompoundIndexDefinition.create("building_idx", testFields(), IndexStatus.BUILDING);

        registry.register(waiting, testSubspace);
        registry.register(ready, testSubspace);
        registry.register(building, testSubspace);

        assertEquals(3, registry.getIndexes(IndexSelectionPolicy.ALL).size());
        assertEquals(1, registry.getIndexes(IndexSelectionPolicy.READ).size());
        assertEquals(2, registry.getIndexes(IndexSelectionPolicy.READWRITE).size());
    }

    @Test
    void shouldReturnNullForNonExistentName() {
        // Behavior: Querying a non-existent name returns null.
        assertNull(registry.getIndexByName("non_existent", IndexSelectionPolicy.ALL));
    }

    @Test
    void shouldReturnNullForNonExistentId() {
        // Behavior: Querying a non-existent ID returns null.
        assertNull(registry.getIndexById(999L, IndexSelectionPolicy.ALL));
    }

    @Test
    void shouldApplyPolicyFilterOnGetByName() {
        // Behavior: WAITING indexes are not returned by READ policy.
        CompoundIndexDefinition def = CompoundIndexDefinition.create("waiting_idx", testFields(), IndexStatus.WAITING);
        registry.register(def, testSubspace);
        assertNull(registry.getIndexByName("waiting_idx", IndexSelectionPolicy.READ));
        assertNotNull(registry.getIndexByName("waiting_idx", IndexSelectionPolicy.ALL));
    }

    @Test
    void shouldReturnUnmodifiableCollections() {
        // Behavior: Index collections cannot be modified externally.
        CompoundIndexDefinition def = CompoundIndexDefinition.create("my_idx", testFields(), IndexStatus.WAITING);
        registry.register(def, testSubspace);

        Collection<CompoundIndex> all = registry.getIndexes(IndexSelectionPolicy.ALL);
        assertThrows(UnsupportedOperationException.class, all::clear);
    }
}
