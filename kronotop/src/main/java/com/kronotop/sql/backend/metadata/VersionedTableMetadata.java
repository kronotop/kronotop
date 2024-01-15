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

import com.kronotop.sql.KronotopTable;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The VersionedTableMetadata class represents the metadata for a versioned table in a database.
 * It provides methods to add metadata for a specific version, retrieve the metadata for a version, and retrieve the latest metadata.
 */
public class VersionedTableMetadata {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, KronotopTable> versions = new HashMap<>();
    private final SortedSet<String> sorted = new TreeSet<>();
    private final AtomicReference<KronotopTable> latest = new AtomicReference<>();

    /**
     * Adds metadata for a specific version to the VersionedTableMetadata object.
     * If the version already exists, a TableVersionAlreadyExistsException is thrown.
     * The latest version is updated after adding the metadata.
     *
     * @param version  The version string to associate with the metadata.
     * @param metadata The metadata to be added for the specified version.
     * @throws TableVersionAlreadyExistsException If the version already exists in the table's metadata.
     */
    public void put(String version, KronotopTable metadata) throws TableVersionAlreadyExistsException {
        lock.writeLock().lock();
        try {
            if (versions.containsKey(version)) {
                throw new TableVersionAlreadyExistsException("Version already exists");
            }
            versions.put(version, metadata);
            sorted.add(version);
            String latestVersion = sorted.last();
            latest.set(versions.get(latestVersion));
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Retrieves the metadata for a specific version from the VersionedTableMetadata object.
     * If the version does not exist, null is returned.
     *
     * @param version The version string to retrieve the metadata for.
     * @return The metadata for the specified version, or null if the version does not exist.
     */
    public KronotopTable get(String version) {
        lock.readLock().lock();
        try {
            return versions.get(version);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Retrieves the latest metadata from the VersionedTableMetadata object.
     *
     * @return The latest metadata, or null if no metadata has been added yet.
     */
    public KronotopTable getLatest() {
        return latest.get();
    }
}
