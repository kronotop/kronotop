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

package com.kronotop;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.server.ClusterNotInitializedException;

/**
 * Manages cluster metadata version persistence in FoundationDB.
 */
public class MetadataVersion {
    public static final String CURRENT = "1.0.0";

    private static final String METADATA_VERSION = "VERSION";

    /**
     * Reads the metadata version from FoundationDB.
     *
     * @param context the Kronotop context
     * @param tr      the FoundationDB transaction
     * @return the metadata version string
     * @throws ClusterNotInitializedException if the version is not found
     */
    public static String read(Context context, Transaction tr) {
        KronotopDirectoryNode directory = KronotopDirectory.kronotop().cluster(context.getClusterName()).metadata();
        DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr.snapshot(), directory.toList()).join();
        byte[] key = subspace.pack(Tuple.from(METADATA_VERSION));
        byte[] value = tr.snapshot().get(key).join();
        if (value == null) {
            throw new ClusterNotInitializedException();
        }
        return new String(value);
    }

    /**
     * Writes the metadata version to FoundationDB.
     * <p>
     * This method should only be used by migration scripts and the cluster initialization command
     *
     * @param context the Kronotop context
     * @param tr      the FoundationDB transaction
     * @param version the version string to write
     */
    public static void write(Context context, Transaction tr, String version) {
        KronotopDirectoryNode directory = KronotopDirectory.kronotop().cluster(context.getClusterName()).metadata();
        DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, directory.toList()).join();
        byte[] key = subspace.pack(Tuple.from(METADATA_VERSION));
        tr.set(key, version.getBytes());
    }
}
