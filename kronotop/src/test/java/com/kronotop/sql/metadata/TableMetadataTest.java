/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.sql.metadata;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class TableMetadataTest {

    @Test
    public void testPut() {
        TableMetadata tableMetadata = new TableMetadata();
        VersionedTableMetadata versionedTableMetadata = new VersionedTableMetadata();
        String tableName = "table";
        assertDoesNotThrow(() -> tableMetadata.put(tableName, versionedTableMetadata));
        assertTrue(tableMetadata.has(tableName));
        assertThrows(TableAlreadyExistsException.class, () -> tableMetadata.put(tableName, versionedTableMetadata));
    }

    /**
     * Tests that {@link TableNotExistsException} is thrown when trying to get a non-existent table.
     */
    @Test
    public void testGet_nonExistentTable_tableNotExistsExceptionThrown() {
        // Arrange
        final String nonExistentTable = UUID.randomUUID().toString();
        final TableMetadata tableMetadata = new TableMetadata();

        // Act and Assert
        assertThrows(TableNotExistsException.class, () -> tableMetadata.get(nonExistentTable));
    }

    /**
     * Tests that correct {@link VersionedTableMetadata} instance is returned for an existing table.
     */
    @Test
    public void testGet_existingTable_correctVersionedTableMetadataReturned() {
        final String existingTable = UUID.randomUUID().toString();
        final VersionedTableMetadata versionedTableMetadata = new VersionedTableMetadata();
        final TableMetadata tableMetadata = new TableMetadata();

        assertDoesNotThrow(() -> tableMetadata.put(existingTable, versionedTableMetadata));
        VersionedTableMetadata fetchedMetadata = assertDoesNotThrow(() -> tableMetadata.get(existingTable));
        assertEquals(versionedTableMetadata, fetchedMetadata);
    }

    @Test
    public void testRename() {
        TableMetadata tableMetadata = new TableMetadata();
        VersionedTableMetadata versionedTableMetadata = new VersionedTableMetadata();
        String tableName = "table";
        String nextName = "table2";
        assertDoesNotThrow(() -> tableMetadata.put(tableName, versionedTableMetadata));
        assertDoesNotThrow(() -> tableMetadata.rename(tableName, nextName));

        assertFalse(tableMetadata.has(tableName));
        assertTrue(tableMetadata.has(nextName));
    }
}