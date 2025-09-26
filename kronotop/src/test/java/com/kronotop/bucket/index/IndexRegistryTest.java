/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.index;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.KronotopException;
import org.bson.BsonType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class IndexRegistryTest extends BaseStandaloneInstanceTest {

    private IndexRegistry indexRegistry;
    private DirectorySubspace testSubspace;

    /**
     * Provides test data for all IndexStatus and IndexSelectionPolicy combinations
     */
    static Stream<Arguments> statusPolicyTestData() {
        return Stream.of(
                // READONLY policy tests
                Arguments.of(IndexStatus.WAITING, IndexSelectionPolicy.READONLY, false),
                Arguments.of(IndexStatus.BUILDING, IndexSelectionPolicy.READONLY, false),
                Arguments.of(IndexStatus.READY, IndexSelectionPolicy.READONLY, true),
                Arguments.of(IndexStatus.DROPPED, IndexSelectionPolicy.READONLY, false),
                Arguments.of(IndexStatus.FAILED, IndexSelectionPolicy.READONLY, false),

                // READWRITE policy tests
                Arguments.of(IndexStatus.WAITING, IndexSelectionPolicy.READWRITE, true),
                Arguments.of(IndexStatus.BUILDING, IndexSelectionPolicy.READWRITE, true),
                Arguments.of(IndexStatus.READY, IndexSelectionPolicy.READWRITE, true),
                Arguments.of(IndexStatus.DROPPED, IndexSelectionPolicy.READWRITE, false),
                Arguments.of(IndexStatus.FAILED, IndexSelectionPolicy.READWRITE, false),

                // ALL policy tests - should always return the index
                Arguments.of(IndexStatus.WAITING, IndexSelectionPolicy.ALL, true),
                Arguments.of(IndexStatus.BUILDING, IndexSelectionPolicy.ALL, true),
                Arguments.of(IndexStatus.READY, IndexSelectionPolicy.ALL, true),
                Arguments.of(IndexStatus.DROPPED, IndexSelectionPolicy.ALL, true),
                Arguments.of(IndexStatus.FAILED, IndexSelectionPolicy.ALL, true)
        );
    }

    @BeforeEach
    void setUp() {
        indexRegistry = new IndexRegistry(context);
        // Create a test subspace using the proper method
        testSubspace = createOrOpenSubspaceUnderCluster("test-index");
    }

    @ParameterizedTest
    @MethodSource("statusPolicyTestData")
    void testGetIndex_AllStatusAndPolicyCombinations(IndexStatus status, IndexSelectionPolicy policy, boolean shouldReturnIndex) {
        // Create index definition with the specific status
        IndexDefinition baseDefinition = IndexDefinition.create("test-index", "test.field", BsonType.STRING);
        IndexDefinition definitionWithStatus = baseDefinition.updateStatus(status);

        // Register the index
        indexRegistry.register(definitionWithStatus, testSubspace);

        // Test getIndex with the policy
        Index result = indexRegistry.getIndex("test.field", policy);

        if (shouldReturnIndex) {
            assertNotNull(result, String.format("Index should be returned for status=%s, policy=%s", status, policy));
            assertEquals(definitionWithStatus, result.definition());
            assertEquals(testSubspace, result.subspace());
        } else {
            assertNull(result, String.format("Index should NOT be returned for status=%s, policy=%s", status, policy));
        }
    }

    @Test
    void testGetIndex_NonExistentSelector_ReturnsNull() {
        // Test with all policies on non-existent selector
        for (IndexSelectionPolicy policy : IndexSelectionPolicy.values()) {
            Index result = indexRegistry.getIndex("non.existent.field", policy);
            assertNull(result, String.format("Should return null for non-existent selector with policy %s", policy));
        }
    }

    @Test
    void testGetIndex_ReadOnlyPolicy_OnlyReturnsReadyIndexes() {
        // Register indexes with different statuses
        IndexDefinition waitingDef = IndexDefinition.create("waiting-index", "waiting.field", BsonType.STRING)
                .updateStatus(IndexStatus.WAITING);
        IndexDefinition buildingDef = IndexDefinition.create("building-index", "building.field", BsonType.STRING)
                .updateStatus(IndexStatus.BUILDING);
        IndexDefinition readyDef = IndexDefinition.create("ready-index", "ready.field", BsonType.STRING)
                .updateStatus(IndexStatus.READY);
        IndexDefinition droppedDef = IndexDefinition.create("dropped-index", "dropped.field", BsonType.STRING)
                .updateStatus(IndexStatus.DROPPED);
        IndexDefinition failedDef = IndexDefinition.create("failed-index", "failed.field", BsonType.STRING)
                .updateStatus(IndexStatus.FAILED);

        indexRegistry.register(waitingDef, testSubspace);
        indexRegistry.register(buildingDef, testSubspace);
        indexRegistry.register(readyDef, testSubspace);
        indexRegistry.register(droppedDef, testSubspace);
        indexRegistry.register(failedDef, testSubspace);

        // Only READY index should be returned
        assertNull(indexRegistry.getIndex("waiting.field", IndexSelectionPolicy.READONLY));
        assertNull(indexRegistry.getIndex("building.field", IndexSelectionPolicy.READONLY));
        assertNotNull(indexRegistry.getIndex("ready.field", IndexSelectionPolicy.READONLY));
        assertNull(indexRegistry.getIndex("dropped.field", IndexSelectionPolicy.READONLY));
        assertNull(indexRegistry.getIndex("failed.field", IndexSelectionPolicy.READONLY));
    }

    @Test
    void testGetIndex_ReadWritePolicy_ReturnsWaitingBuildingReady() {
        // Register indexes with different statuses
        IndexDefinition waitingDef = IndexDefinition.create("waiting-index", "waiting.field", BsonType.STRING)
                .updateStatus(IndexStatus.WAITING);
        IndexDefinition buildingDef = IndexDefinition.create("building-index", "building.field", BsonType.STRING)
                .updateStatus(IndexStatus.BUILDING);
        IndexDefinition readyDef = IndexDefinition.create("ready-index", "ready.field", BsonType.STRING)
                .updateStatus(IndexStatus.READY);
        IndexDefinition droppedDef = IndexDefinition.create("dropped-index", "dropped.field", BsonType.STRING)
                .updateStatus(IndexStatus.DROPPED);
        IndexDefinition failedDef = IndexDefinition.create("failed-index", "failed.field", BsonType.STRING)
                .updateStatus(IndexStatus.FAILED);

        indexRegistry.register(waitingDef, testSubspace);
        indexRegistry.register(buildingDef, testSubspace);
        indexRegistry.register(readyDef, testSubspace);
        indexRegistry.register(droppedDef, testSubspace);
        indexRegistry.register(failedDef, testSubspace);

        // WAITING, BUILDING, and READY should be returned
        assertNotNull(indexRegistry.getIndex("waiting.field", IndexSelectionPolicy.READWRITE));
        assertNotNull(indexRegistry.getIndex("building.field", IndexSelectionPolicy.READWRITE));
        assertNotNull(indexRegistry.getIndex("ready.field", IndexSelectionPolicy.READWRITE));
        assertNull(indexRegistry.getIndex("dropped.field", IndexSelectionPolicy.READWRITE));
        assertNull(indexRegistry.getIndex("failed.field", IndexSelectionPolicy.READWRITE));
    }

    @Test
    void testGetIndex_AllPolicy_ReturnsAllIndexes() {
        // Register indexes with all statuses
        for (IndexStatus status : IndexStatus.values()) {
            String fieldName = status.name().toLowerCase() + ".field";
            IndexDefinition definition = IndexDefinition.create(status.name() + "-index", fieldName, BsonType.STRING)
                    .updateStatus(status);
            indexRegistry.register(definition, testSubspace);

            // ALL policy should return all indexes regardless of status
            Index result = indexRegistry.getIndex(fieldName, IndexSelectionPolicy.ALL);
            assertNotNull(result, String.format("ALL policy should return index with status %s", status));
            assertEquals(status, result.definition().status());
        }
    }

    @Test
    void testGetIndex_CaseSensitiveSelectors() {
        IndexDefinition lowerCaseDef = IndexDefinition.create("lower-index", "field.name", BsonType.STRING);
        IndexDefinition upperCaseDef = IndexDefinition.create("upper-index", "Field.Name", BsonType.STRING);
        IndexDefinition mixedCaseDef = IndexDefinition.create("mixed-index", "FIELD.name", BsonType.STRING);

        indexRegistry.register(lowerCaseDef, testSubspace);
        indexRegistry.register(upperCaseDef, testSubspace);
        indexRegistry.register(mixedCaseDef, testSubspace);

        // Should be case-sensitive
        assertNotNull(indexRegistry.getIndex("field.name", IndexSelectionPolicy.ALL));
        assertNotNull(indexRegistry.getIndex("Field.Name", IndexSelectionPolicy.ALL));
        assertNotNull(indexRegistry.getIndex("FIELD.name", IndexSelectionPolicy.ALL));
        assertNull(indexRegistry.getIndex("field.Name", IndexSelectionPolicy.ALL)); // Different case
    }

    @ParameterizedTest
    @EnumSource(IndexSelectionPolicy.class)
    void testGetIndex_EmptyRegistry_ReturnsNull(IndexSelectionPolicy policy) {
        Index result = indexRegistry.getIndex("any.field", policy);
        assertNull(result, String.format("Empty registry should return null for policy %s", policy));
    }

    @Test
    void testGetIndex_IndexDefinitionImmutability() {
        // Create and register an index
        IndexDefinition originalDef = IndexDefinition.create("immutable-index", "immutable.field", BsonType.STRING);
        indexRegistry.register(originalDef, testSubspace);

        // Get the index
        Index retrievedIndex = indexRegistry.getIndex("immutable.field", IndexSelectionPolicy.ALL);
        assertNotNull(retrievedIndex);

        // The retrieved definition should be the same object (referential equality)
        assertSame(originalDef, retrievedIndex.definition());
    }

    // === NEW TESTS FOR UPDATED FUNCTIONALITY ===

    @Test
    void testRegister_DuplicateSelector_ThrowsException() {
        // Register an index
        IndexDefinition firstDef = IndexDefinition.create("first-index", "shared.field", BsonType.STRING);
        indexRegistry.register(firstDef, testSubspace);

        // Try to register another index with the same selector (should throw exception)
        IndexDefinition secondDef = IndexDefinition.create("second-index", "shared.field", BsonType.INT32)
                .updateStatus(IndexStatus.BUILDING);
        DirectorySubspace secondSubspace = createOrOpenSubspaceUnderCluster("test-index-2");

        IndexAlreadyRegisteredException exception = assertThrows(IndexAlreadyRegisteredException.class, () ->
                indexRegistry.register(secondDef, secondSubspace));
        assertEquals("Index entry with 'shared.field' has already registered", exception.getMessage());

        // Should still return the first (original) index
        Index result = indexRegistry.getIndex("shared.field", IndexSelectionPolicy.ALL);
        assertNotNull(result);
        assertEquals(firstDef, result.definition());
        assertEquals(testSubspace, result.subspace());
        assertEquals("first-index", result.definition().name());
        assertEquals(BsonType.STRING, result.definition().bsonType());
        assertEquals(IndexStatus.READY, result.definition().status());
    }

    @Test
    void testDeregister_ExistingIndex_RemovesSuccessfully() {
        // Register multiple indexes
        IndexDefinition def1 = IndexDefinition.create("index1", "field1", BsonType.STRING);
        IndexDefinition def2 = IndexDefinition.create("index2", "field2", BsonType.INT32);
        indexRegistry.register(def1, testSubspace);
        indexRegistry.register(def2, testSubspace);

        // Initialize statistics to avoid NPE in deregister
        indexRegistry.updateStatistics(new java.util.HashMap<>());

        // Verify both indexes exist
        assertNotNull(indexRegistry.getIndex("field1", IndexSelectionPolicy.ALL));
        assertNotNull(indexRegistry.getIndex("field2", IndexSelectionPolicy.ALL));
        assertEquals(2, indexRegistry.getIndexes(IndexSelectionPolicy.ALL).size());

        // Deregister one index
        indexRegistry.deregister(def1);

        // Verify first index is removed and second remains
        assertNull(indexRegistry.getIndex("field1", IndexSelectionPolicy.ALL));
        assertNotNull(indexRegistry.getIndex("field2", IndexSelectionPolicy.ALL));
        assertEquals(1, indexRegistry.getIndexes(IndexSelectionPolicy.ALL).size());
    }

    @Test
    void testDeregister_NonExistentIndex_HandledGracefully() {
        IndexDefinition nonExistentDef = IndexDefinition.create("non-existent", "non.existent", BsonType.STRING);

        // Initialize statistics to avoid NPE in deregister
        indexRegistry.updateStatistics(new java.util.HashMap<>());

        // Should not throw exception when deregistering non-existent index
        assertDoesNotThrow(() -> indexRegistry.deregister(nonExistentDef));
    }

    @Test
    void testUpdateIndexDefinition_ExistingIndex_UpdatesSuccessfully() {
        // Register an index with READY status
        IndexDefinition originalDef = IndexDefinition.create("test-index", "test.field", BsonType.STRING);
        indexRegistry.register(originalDef, testSubspace);

        // Verify initial state
        Index originalIndex = indexRegistry.getIndex("test.field", IndexSelectionPolicy.ALL);
        assertEquals(IndexStatus.READY, originalIndex.definition().status());
        assertNotNull(indexRegistry.getIndex("test.field", IndexSelectionPolicy.READONLY));

        // Update to BUILDING status
        IndexDefinition updatedDef = originalDef.updateStatus(IndexStatus.BUILDING);
        indexRegistry.updateIndexDefinition(updatedDef);

        // Verify updated state
        Index updatedIndex = indexRegistry.getIndex("test.field", IndexSelectionPolicy.ALL);
        assertEquals(IndexStatus.BUILDING, updatedIndex.definition().status());
        assertNull(indexRegistry.getIndex("test.field", IndexSelectionPolicy.READONLY)); // Not available for READONLY
        assertNotNull(indexRegistry.getIndex("test.field", IndexSelectionPolicy.READWRITE)); // Available for READWRITE
    }

    @Test
    void testUpdateIndexDefinition_NonExistentIndex_ThrowsException() {
        IndexDefinition nonExistentDef = IndexDefinition.create("non-existent", "non.existent", BsonType.STRING);

        KronotopException exception = assertThrows(KronotopException.class, () ->
                indexRegistry.updateIndexDefinition(nonExistentDef));
        assertEquals("Index with 'non.existent' could not be found", exception.getMessage());
    }

    @Test
    void testUpdateIndexDefinition_StatusChanges_AffectsPolicySegregation() {
        // Register indexes with different statuses
        IndexDefinition readyDef = IndexDefinition.create("ready-index", "ready.field", BsonType.STRING);
        IndexDefinition buildingDef = IndexDefinition.create("building-index", "building.field", BsonType.STRING)
                .updateStatus(IndexStatus.BUILDING);
        IndexDefinition failedDef = IndexDefinition.create("failed-index", "failed.field", BsonType.STRING)
                .updateStatus(IndexStatus.FAILED);

        indexRegistry.register(readyDef, testSubspace);
        indexRegistry.register(buildingDef, testSubspace);
        indexRegistry.register(failedDef, testSubspace);

        // Initial counts
        assertEquals(1, indexRegistry.getIndexes(IndexSelectionPolicy.READONLY).size()); // Only READY
        assertEquals(2, indexRegistry.getIndexes(IndexSelectionPolicy.READWRITE).size()); // READY + BUILDING
        assertEquals(3, indexRegistry.getIndexes(IndexSelectionPolicy.ALL).size()); // All indexes

        // Update BUILDING to READY
        IndexDefinition updatedBuilding = buildingDef.updateStatus(IndexStatus.READY);
        indexRegistry.updateIndexDefinition(updatedBuilding);

        // Verify counts changed
        assertEquals(2, indexRegistry.getIndexes(IndexSelectionPolicy.READONLY).size()); // READY + updated READY
        assertEquals(2, indexRegistry.getIndexes(IndexSelectionPolicy.READWRITE).size()); // Both READY indexes
        assertEquals(3, indexRegistry.getIndexes(IndexSelectionPolicy.ALL).size()); // Still all indexes

        // Update READY to FAILED
        IndexDefinition updatedReady = readyDef.updateStatus(IndexStatus.FAILED);
        indexRegistry.updateIndexDefinition(updatedReady);

        // Verify counts changed again
        assertEquals(1, indexRegistry.getIndexes(IndexSelectionPolicy.READONLY).size()); // Only the updated building->ready
        assertEquals(1, indexRegistry.getIndexes(IndexSelectionPolicy.READWRITE).size()); // Only the updated building->ready
        assertEquals(3, indexRegistry.getIndexes(IndexSelectionPolicy.ALL).size()); // Still all indexes
    }

    @Test
    void testGetIndexes_PolicySegregation_ReturnsCorrectCounts() {
        // Register indexes with all possible statuses
        IndexDefinition waitingDef = IndexDefinition.create("waiting-index", "waiting.field", BsonType.STRING)
                .updateStatus(IndexStatus.WAITING);
        IndexDefinition buildingDef = IndexDefinition.create("building-index", "building.field", BsonType.STRING)
                .updateStatus(IndexStatus.BUILDING);
        IndexDefinition readyDef = IndexDefinition.create("ready-index", "ready.field", BsonType.STRING)
                .updateStatus(IndexStatus.READY);
        IndexDefinition droppedDef = IndexDefinition.create("dropped-index", "dropped.field", BsonType.STRING)
                .updateStatus(IndexStatus.DROPPED);
        IndexDefinition failedDef = IndexDefinition.create("failed-index", "failed.field", BsonType.STRING)
                .updateStatus(IndexStatus.FAILED);

        indexRegistry.register(waitingDef, testSubspace);
        indexRegistry.register(buildingDef, testSubspace);
        indexRegistry.register(readyDef, testSubspace);
        indexRegistry.register(droppedDef, testSubspace);
        indexRegistry.register(failedDef, testSubspace);

        // Verify policy-based segregation
        Collection<Index> allIndexes = indexRegistry.getIndexes(IndexSelectionPolicy.ALL);
        Collection<Index> readonlyIndexes = indexRegistry.getIndexes(IndexSelectionPolicy.READONLY);
        Collection<Index> readwriteIndexes = indexRegistry.getIndexes(IndexSelectionPolicy.READWRITE);

        // ALL policy should include all 5 indexes
        assertEquals(5, allIndexes.size());

        // READONLY policy should include only READY indexes
        assertEquals(1, readonlyIndexes.size());
        assertTrue(readonlyIndexes.stream().allMatch(idx -> idx.definition().status() == IndexStatus.READY));

        // READWRITE policy should include WAITING, BUILDING, and READY indexes
        assertEquals(3, readwriteIndexes.size());
        assertTrue(readwriteIndexes.stream().allMatch(idx ->
                idx.definition().status() == IndexStatus.WAITING ||
                idx.definition().status() == IndexStatus.BUILDING ||
                idx.definition().status() == IndexStatus.READY));
    }

    @Test
    void testGetIndexes_ReturnsUnmodifiableCollections() {
        IndexDefinition def = IndexDefinition.create("test-index", "test.field", BsonType.STRING);
        indexRegistry.register(def, testSubspace);

        // Get collections for each policy
        Collection<Index> allIndexes = indexRegistry.getIndexes(IndexSelectionPolicy.ALL);
        Collection<Index> readonlyIndexes = indexRegistry.getIndexes(IndexSelectionPolicy.READONLY);
        Collection<Index> readwriteIndexes = indexRegistry.getIndexes(IndexSelectionPolicy.READWRITE);

        // Should throw UnsupportedOperationException when trying to modify
        assertThrows(UnsupportedOperationException.class, allIndexes::clear);
        assertThrows(UnsupportedOperationException.class, readonlyIndexes::clear);
        assertThrows(UnsupportedOperationException.class, readwriteIndexes::clear);
    }
}