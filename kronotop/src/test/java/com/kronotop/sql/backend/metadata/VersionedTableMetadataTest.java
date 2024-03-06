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

package com.kronotop.sql.backend.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * This class contains tests for the put method of the VersionedTableMetadata class.
 * The put method is responsible for adding a specific version's metadata to the object.
 * These tests ensure that this method works as expected under a variety of conditions.
 */
public class VersionedTableMetadataTest {

    private final String versionStampFirst = "AAAHIvm0lWMAAAAA";
    private final String versionstampSecond = "AAAHKUwFqL8AAAAA";

    /**
     * This test validates the case when a new version is being added.
     * The expected behavior is that the method should simply add the new version without any problems.
     */
    @Test
    void testPutNewVersion() {
        VersionedTableMetadata vtm = new VersionedTableMetadata();
        assertDoesNotThrow(() -> vtm.put(versionStampFirst, null));
    }

    /**
     * This test validates the case when an existing version is added.
     * The expected behavior is that the method should throw a TableVersionAlreadyExistsException.
     */
    @Test
    void testPutExistingVersion() {
        VersionedTableMetadata vtm = new VersionedTableMetadata();
        assertDoesNotThrow(() -> vtm.put(versionStampFirst, null));
        assertThrows(TableVersionAlreadyExistsException.class, () -> vtm.put(versionStampFirst, null));
    }

    /**
     * This test checks if the latest version is updated after adding the metadata.
     * The expected behavior is that the latest version should be updated to the new version after it is added.
     */
    @Test
    void testLatestVersionUpdate() {
        VersionedTableMetadata vtm = new VersionedTableMetadata();
        assertDoesNotThrow(() -> vtm.put(versionStampFirst, null));
        assertNull(vtm.getLatest());
        assertDoesNotThrow(() -> vtm.put(versionstampSecond, null));
        assertNull(vtm.getLatest());
    }

    /**
     * The testGet method tests the get method in the
     * VersionedTableMetadata class.
     */
    @Test
    public void testGet() {
        VersionedTableMetadata tableMetadata = new VersionedTableMetadata();

        //Test case#1: check get when version does not exist.
        assertNull(tableMetadata.get(versionStampFirst));

        //Test case#2: check get when version exists.
        try {
            tableMetadata.put(versionstampSecond, null);
        } catch (TableVersionAlreadyExistsException e) {
            fail("Unexpected exception occurred");
        }

        assertNull(tableMetadata.get(versionstampSecond));

        //Test case#3: check get when version is null.
        assertNull(tableMetadata.get(null));
    }
}