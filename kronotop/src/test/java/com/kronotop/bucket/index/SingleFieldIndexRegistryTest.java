/*
 * Copyright (c) 2023-2026 Burak Sezer
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

class SingleFieldIndexRegistryTest extends BaseStandaloneInstanceTest {

    private SingleFieldIndexRegistry indexRegistry;
    private DirectorySubspace testSubspace;

    /**
     * Provides test data for all IndexStatus and IndexSelectionPolicy combinations
     */
    static Stream<Arguments> statusPolicyTestData() {
        return Stream.of(
                // READONLY policy tests
                Arguments.of(IndexStatus.WAITING, IndexSelectionPolicy.READ, false),
                Arguments.of(IndexStatus.BUILDING, IndexSelectionPolicy.READ, false),
                Arguments.of(IndexStatus.READY, IndexSelectionPolicy.READ, true),
                Arguments.of(IndexStatus.DROPPED, IndexSelectionPolicy.READ, false),
                Arguments.of(IndexStatus.FAILED, IndexSelectionPolicy.READ, false),

                // READWRITE policy tests
                Arguments.of(IndexStatus.WAITING, IndexSelectionPolicy.READWRITE, false),
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
        indexRegistry = new SingleFieldIndexRegistry(context);
        // Create a test subspace using the proper method
        testSubspace = createOrOpenSubspaceUnderCluster("test-index");
    }

    @ParameterizedTest
    @MethodSource("statusPolicyTestData")
    void shouldGetIndexForAllStatusAndPolicyCombinations(IndexStatus status, IndexSelectionPolicy policy, boolean shouldReturnIndex) {
        // Create index definition with the specific status
        SingleFieldIndexDefinition baseDefinition = TestIndexDefinitions.singleField("test-index", "test.field", BsonType.STRING, false);
        SingleFieldIndexDefinition definitionWithStatus = baseDefinition.updateStatus(status);

        // Register the index
        indexRegistry.register(definitionWithStatus, testSubspace);

        // Test getIndex with the policy
        SingleFieldIndex result = indexRegistry.getIndex("test.field", policy);

        if (shouldReturnIndex) {
            assertNotNull(result, String.format("Index should be returned for status=%s, policy=%s", status, policy));
            assertEquals(definitionWithStatus, result.definition());
            assertEquals(testSubspace, result.subspace());
        } else {
            assertNull(result, String.format("Index should NOT be returned for status=%s, policy=%s", status, policy));
        }
    }

    @Test
    void shouldReturnNullForNonExistentSelector() {
        // Test with all policies on non-existent selector
        for (IndexSelectionPolicy policy : IndexSelectionPolicy.values()) {
            SingleFieldIndex result = indexRegistry.getIndex("non.existent.field", policy);
            assertNull(result, String.format("Should return null for non-existent selector with policy %s", policy));
        }
    }

    @Test
    void shouldOnlyReturnReadyIndexesForReadOnlyPolicy() {
        // Register indexes with different statuses
        SingleFieldIndexDefinition waitingDef = TestIndexDefinitions.singleField("waiting-index", "waiting.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.WAITING);
        SingleFieldIndexDefinition buildingDef = TestIndexDefinitions.singleField("building-index", "building.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.BUILDING);
        SingleFieldIndexDefinition readyDef = TestIndexDefinitions.singleField("ready-index", "ready.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.READY);
        SingleFieldIndexDefinition droppedDef = TestIndexDefinitions.singleField("dropped-index", "dropped.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.DROPPED);
        SingleFieldIndexDefinition failedDef = TestIndexDefinitions.singleField("failed-index", "failed.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.FAILED);

        indexRegistry.register(waitingDef, testSubspace);
        indexRegistry.register(buildingDef, testSubspace);
        indexRegistry.register(readyDef, testSubspace);
        indexRegistry.register(droppedDef, testSubspace);
        indexRegistry.register(failedDef, testSubspace);

        // Only READY index should be returned
        assertNull(indexRegistry.getIndex("waiting.field", IndexSelectionPolicy.READ));
        assertNull(indexRegistry.getIndex("building.field", IndexSelectionPolicy.READ));
        assertNotNull(indexRegistry.getIndex("ready.field", IndexSelectionPolicy.READ));
        assertNull(indexRegistry.getIndex("dropped.field", IndexSelectionPolicy.READ));
        assertNull(indexRegistry.getIndex("failed.field", IndexSelectionPolicy.READ));
    }

    @Test
    void shouldReturnBuildingAndReadyForReadWritePolicy() {
        // Behavior: READWRITE policy returns only BUILDING and READY indexes.
        // WAITING is excluded because foreground writes must not touch secondary
        // indexes until the boundary routine captures the boundary and transitions
        // the index to BUILDING.
        SingleFieldIndexDefinition waitingDef = TestIndexDefinitions.singleField("waiting-index", "waiting.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.WAITING);
        SingleFieldIndexDefinition buildingDef = TestIndexDefinitions.singleField("building-index", "building.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.BUILDING);
        SingleFieldIndexDefinition readyDef = TestIndexDefinitions.singleField("ready-index", "ready.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.READY);
        SingleFieldIndexDefinition droppedDef = TestIndexDefinitions.singleField("dropped-index", "dropped.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.DROPPED);
        SingleFieldIndexDefinition failedDef = TestIndexDefinitions.singleField("failed-index", "failed.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.FAILED);

        indexRegistry.register(waitingDef, testSubspace);
        indexRegistry.register(buildingDef, testSubspace);
        indexRegistry.register(readyDef, testSubspace);
        indexRegistry.register(droppedDef, testSubspace);
        indexRegistry.register(failedDef, testSubspace);

        // Only BUILDING and READY should be returned
        assertNull(indexRegistry.getIndex("waiting.field", IndexSelectionPolicy.READWRITE));
        assertNotNull(indexRegistry.getIndex("building.field", IndexSelectionPolicy.READWRITE));
        assertNotNull(indexRegistry.getIndex("ready.field", IndexSelectionPolicy.READWRITE));
        assertNull(indexRegistry.getIndex("dropped.field", IndexSelectionPolicy.READWRITE));
        assertNull(indexRegistry.getIndex("failed.field", IndexSelectionPolicy.READWRITE));
    }

    @Test
    void shouldReturnAllIndexesForAllPolicy() {
        // Register indexes with all statuses
        for (IndexStatus status : IndexStatus.values()) {
            String fieldName = status.name().toLowerCase() + ".field";
            SingleFieldIndexDefinition definition = TestIndexDefinitions.singleField(status.name() + "-index", fieldName, BsonType.STRING, false)
                    .updateStatus(status);
            indexRegistry.register(definition, testSubspace);

            // ALL policy should return all indexes regardless of status
            SingleFieldIndex result = indexRegistry.getIndex(fieldName, IndexSelectionPolicy.ALL);
            assertNotNull(result, String.format("ALL policy should return index with status %s", status));
            assertEquals(status, result.definition().status());
        }
    }

    @Test
    void shouldHandleCaseSensitiveSelectors() {
        SingleFieldIndexDefinition lowerCaseDef = TestIndexDefinitions.singleField("lower-index", "field.name", BsonType.STRING, false);
        SingleFieldIndexDefinition upperCaseDef = TestIndexDefinitions.singleField("upper-index", "Field.Name", BsonType.STRING, false);
        SingleFieldIndexDefinition mixedCaseDef = TestIndexDefinitions.singleField("mixed-index", "FIELD.name", BsonType.STRING, false);

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
    void shouldReturnNullForEmptyRegistry(IndexSelectionPolicy policy) {
        SingleFieldIndex result = indexRegistry.getIndex("any.field", policy);
        assertNull(result, String.format("Empty registry should return null for policy %s", policy));
    }

    @Test
    void shouldPreserveIndexDefinitionImmutability() {
        // Create and register an index
        SingleFieldIndexDefinition originalDef = TestIndexDefinitions.singleField("immutable-index", "immutable.field", BsonType.STRING, false);
        indexRegistry.register(originalDef, testSubspace);

        // Get the index
        SingleFieldIndex retrievedIndex = indexRegistry.getIndex("immutable.field", IndexSelectionPolicy.ALL);
        assertNotNull(retrievedIndex);

        // The retrieved definition should be the same object (referential equality)
        assertSame(originalDef, retrievedIndex.definition());
    }

    @Test
    void shouldThrowExceptionForDuplicateSelector() {
        // Register an index
        SingleFieldIndexDefinition firstDef = TestIndexDefinitions.singleField("first-index", "shared.field", BsonType.STRING, false);
        indexRegistry.register(firstDef, testSubspace);

        // Try to register another index with the same selector (should throw exception)
        SingleFieldIndexDefinition secondDef = TestIndexDefinitions.singleField("second-index", "shared.field", BsonType.INT32, false)
                .updateStatus(IndexStatus.BUILDING);
        DirectorySubspace secondSubspace = createOrOpenSubspaceUnderCluster("test-index-2");

        IndexAlreadyRegisteredException exception = assertThrows(IndexAlreadyRegisteredException.class, () ->
                indexRegistry.register(secondDef, secondSubspace));
        assertEquals("Index entry with 'shared.field' has already been registered", exception.getMessage());

        // Should still return the first (original) index
        SingleFieldIndex result = indexRegistry.getIndex("shared.field", IndexSelectionPolicy.ALL);
        assertNotNull(result);
        assertEquals(firstDef, result.definition());
        assertEquals(testSubspace, result.subspace());
        assertEquals("first-index", result.definition().name());
        assertEquals(BsonType.STRING, result.definition().bsonType());
        assertEquals(IndexStatus.WAITING, result.definition().status());
    }

    @Test
    void shouldRemoveExistingIndexOnDeregister() {
        // Register multiple indexes
        SingleFieldIndexDefinition def1 = TestIndexDefinitions.singleField("index1", "field1", BsonType.STRING, false);
        SingleFieldIndexDefinition def2 = TestIndexDefinitions.singleField("index2", "field2", BsonType.INT32, false);
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
    void shouldNotThrowWhenDeregisteringNonExistentIndex() {
        SingleFieldIndexDefinition nonExistentDef = TestIndexDefinitions.singleField("non-existent", "non.existent", BsonType.STRING, false);

        // Initialize statistics to avoid NPE in deregister
        indexRegistry.updateStatistics(new java.util.HashMap<>());

        // Should not throw exception when deregistering non-existent index
        assertDoesNotThrow(() -> indexRegistry.deregister(nonExistentDef));
    }

    @Test
    void shouldUpdateExistingIndexDefinitionSuccessfully() {
        // Register an index with READY status
        SingleFieldIndexDefinition originalDef = TestIndexDefinitions.singleField("test-index", "test.field", BsonType.STRING, false).
                updateStatus(IndexStatus.READY);
        indexRegistry.register(originalDef, testSubspace);

        // Verify initial state
        SingleFieldIndex originalIndex = indexRegistry.getIndex("test.field", IndexSelectionPolicy.ALL);
        assertEquals(IndexStatus.READY, originalIndex.definition().status());
        assertNotNull(indexRegistry.getIndex("test.field", IndexSelectionPolicy.READ));
    }

    @Test
    void shouldAffectPolicySegregationOnStatusChanges() {
        // Register indexes with different statuses
        SingleFieldIndexDefinition waitingDef = TestIndexDefinitions.singleField("waiting-index", "waiting.field", BsonType.STRING, false);
        SingleFieldIndexDefinition readyDef = TestIndexDefinitions.singleField("ready-index", "ready.field", BsonType.STRING, false).
                updateStatus(IndexStatus.READY);
        SingleFieldIndexDefinition buildingDef = TestIndexDefinitions.singleField("building-index", "building.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.BUILDING);
        SingleFieldIndexDefinition failedDef = TestIndexDefinitions.singleField("failed-index", "failed.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.FAILED);

        indexRegistry.register(waitingDef, testSubspace);
        indexRegistry.register(readyDef, testSubspace);
        indexRegistry.register(buildingDef, testSubspace);
        indexRegistry.register(failedDef, testSubspace);

        // Initial counts
        assertEquals(1, indexRegistry.getIndexes(IndexSelectionPolicy.READ).size()); // Only READY
        assertEquals(2, indexRegistry.getIndexes(IndexSelectionPolicy.READWRITE).size()); // READY + BUILDING
        assertEquals(4, indexRegistry.getIndexes(IndexSelectionPolicy.ALL).size()); // All indexes
    }

    @Test
    void shouldReturnCorrectCountsForPolicySegregation() {
        // Register indexes with all possible statuses
        SingleFieldIndexDefinition waitingDef = TestIndexDefinitions.singleField("waiting-index", "waiting.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.WAITING);
        SingleFieldIndexDefinition buildingDef = TestIndexDefinitions.singleField("building-index", "building.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.BUILDING);
        SingleFieldIndexDefinition readyDef = TestIndexDefinitions.singleField("ready-index", "ready.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.READY);
        SingleFieldIndexDefinition droppedDef = TestIndexDefinitions.singleField("dropped-index", "dropped.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.DROPPED);
        SingleFieldIndexDefinition failedDef = TestIndexDefinitions.singleField("failed-index", "failed.field", BsonType.STRING, false)
                .updateStatus(IndexStatus.FAILED);

        indexRegistry.register(waitingDef, testSubspace);
        indexRegistry.register(buildingDef, testSubspace);
        indexRegistry.register(readyDef, testSubspace);
        indexRegistry.register(droppedDef, testSubspace);
        indexRegistry.register(failedDef, testSubspace);

        // Verify policy-based segregation
        Collection<SingleFieldIndex> allIndexes = indexRegistry.getIndexes(IndexSelectionPolicy.ALL);
        Collection<SingleFieldIndex> readonlyIndexes = indexRegistry.getIndexes(IndexSelectionPolicy.READ);
        Collection<SingleFieldIndex> readwriteIndexes = indexRegistry.getIndexes(IndexSelectionPolicy.READWRITE);

        // ALL policy should include all 5 indexes
        assertEquals(5, allIndexes.size());

        // READONLY policy should include only READY indexes
        assertEquals(1, readonlyIndexes.size());
        assertTrue(readonlyIndexes.stream().allMatch(idx -> idx.definition().status() == IndexStatus.READY));

        // READWRITE policy should include BUILDING and READY indexes
        assertEquals(2, readwriteIndexes.size());
        assertTrue(readwriteIndexes.stream().allMatch(idx ->
                idx.definition().status() == IndexStatus.BUILDING ||
                        idx.definition().status() == IndexStatus.READY));
    }

    @Test
    void shouldReturnUnmodifiableCollections() {
        SingleFieldIndexDefinition def = TestIndexDefinitions.singleField("test-index", "test.field", BsonType.STRING, false);
        indexRegistry.register(def, testSubspace);

        // Get collections for each policy
        Collection<SingleFieldIndex> allIndexes = indexRegistry.getIndexes(IndexSelectionPolicy.ALL);
        Collection<SingleFieldIndex> readonlyIndexes = indexRegistry.getIndexes(IndexSelectionPolicy.READ);
        Collection<SingleFieldIndex> readwriteIndexes = indexRegistry.getIndexes(IndexSelectionPolicy.READWRITE);

        // Should throw UnsupportedOperationException when trying to modify
        assertThrows(UnsupportedOperationException.class, allIndexes::clear);
        assertThrows(UnsupportedOperationException.class, readonlyIndexes::clear);
        assertThrows(UnsupportedOperationException.class, readwriteIndexes::clear);
    }
}