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

package com.kronotop.core;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.JNIUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The FoundationDBFactory class provides a static method for creating a new FoundationDB Database instance.
 */
public class FoundationDBFactory {
    private static final int DEFAULT_FDB_API_VERSION = 510;
    private static final Logger LOGGER = LoggerFactory.getLogger(FoundationDBFactory.class);
    private static volatile Database database;
    private static volatile boolean isClosed;

    /**
     * Creates a new FoundationDB Database instance based on the provided configuration.
     *
     * @param config the configuration object containing the necessary parameters for establishing a connection to the database
     * @return a new FoundationDB Database instance
     * @throws FDBException             if there is an error accessing the FoundationDB database
     * @throws IllegalArgumentException if the API version specified in the configuration is zero
     */
    public synchronized static Database newDatabase(Config config) throws FDBException {
        if (isClosed) {
            throw new IllegalStateException("Database is already closed");
        }

        // We already created a FoundationDB connection. Let's reuse it.
        if (database != null) {
            return database;
        }

        int apiVersion = DEFAULT_FDB_API_VERSION;
        try {
            apiVersion = config.getInt("foundationdb.apiversion");
        } catch (ConfigException.Missing e) {
            LOGGER.warn(String.format(
                    "foundationdb.apiversion is missing. Setting the default version: %d", DEFAULT_FDB_API_VERSION)
            );
        }

        // Validate API Version here
        if (apiVersion == 0) {
            throw new IllegalArgumentException("FoundationDB API Version cannot be zero");
        }

        configureFDBLibraryPaths(config);

        // TODO: Add network options.
        FDB fdb = FDB.selectAPIVersion(apiVersion);
        fdb.disableShutdownHook();
        FoundationDBFactory.database = fdb.open();
        return database;
    }

    /**
     * Closes the FoundationDB connection.
     * <p>
     * This method closes the FoundationDB connection if it is open and sets the 'isClosed' flag to true.
     * It is synchronized to ensure thread safety.
     */
    public synchronized static void closeDatabase() {
        if (isClosed || database == null) {
            return;
        }
        try {
            database.close();
        } finally {
            isClosed = true;
        }
    }

    /**
     * Configures the FoundationDB library paths based on the provided configuration.
     *
     * @param config the configuration object containing the necessary paths for FoundationDB libraries
     */
    private static void configureFDBLibraryPaths(Config config) {
        if (System.getProperty("FDB_LIBRARY_PATH_FDB_C") == null) {
            if (config.hasPath("foundationdb.fdbc")) {
                String fdbc = config.getString("foundationdb.fdbc");
                System.setProperty("FDB_LIBRARY_PATH_FDB_C", fdbc);
            }
        }

        // We couldn't find libfdb_c. Let's try to make a guess.
        if (System.getProperty("FDB_LIBRARY_PATH_FDB_C") == null) {
            String operatingSystemName = System.getProperty("os.name").toLowerCase();
            if (operatingSystemName.startsWith("linux")) {
                System.setProperty("FDB_LIBRARY_PATH_FDB_C", "/usr/lib/libfdb_c.so");
            } else if (operatingSystemName.startsWith("mac") || operatingSystemName.startsWith("darwin")) {
                System.setProperty("FDB_LIBRARY_PATH_FDB_C", "/usr/local/lib/libfdb_c.dylib");
            }
        }

        if (System.getProperty("FDB_LIBRARY_PATH_FDB_JAVA") == null) {
            if (config.hasPath("foundationdb.fdbjava")) {
                String fdbjava = config.getString("foundationdb.fdbjava");
                System.setProperty("FDB_LIBRARY_PATH_FDB_JAVA", fdbjava);
            }
        }
    }
}
