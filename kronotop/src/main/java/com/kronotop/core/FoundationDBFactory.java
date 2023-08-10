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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBFactory {
    private static final int DEFAULT_FDB_API_VERSION = 510;
    private static final Logger logger = LoggerFactory.getLogger(FoundationDBFactory.class);

    public static Database newDatabase(Config config) throws FDBException {
        int apiVersion = DEFAULT_FDB_API_VERSION;
        try {
            apiVersion = config.getInt("foundationdb.apiversion");
        } catch (ConfigException.Missing e) {
            logger.warn(String.format(
                    "foundationdb.apiversion is missing. Setting the default version: %d", DEFAULT_FDB_API_VERSION)
            );
        }

        // Validate API Version here
        if (apiVersion == 0) {
            throw new IllegalArgumentException("FoundationDB API Version cannot be zero");
        }

        if (config.hasPath("foundationdb.fdbc")) {
            String fdbc = config.getString("foundationdb.fdbc");
            System.setProperty("FDB_LIBRARY_PATH_FDB_C", fdbc);
        }

        if (config.hasPath("foundationdb.fdbjava")) {
            String fdbjava = config.getString("foundationdb.fdbjava");
            System.setProperty("FDB_LIBRARY_PATH_FDB_JAVA", fdbjava);
        }

        // TODO: Add network options.
        FDB fdb = FDB.selectAPIVersion(apiVersion);
        fdb.disableShutdownHook();
        return fdb.open();
    }
}
