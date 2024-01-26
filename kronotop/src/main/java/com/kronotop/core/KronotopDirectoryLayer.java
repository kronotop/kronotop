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
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.redis.storage.LogicalDatabase;
import com.kronotop.redis.storage.persistence.DataStructure;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The KronotopDirectoryLayer class provides methods to open or create directory subspaces within the cluster.
 */
public class KronotopDirectoryLayer {
    private final String clusterName;
    private final Database database;
    private final HashMap<Integer, DataStructureSubpath> datastructures = new HashMap<>();
    private final ConcurrentHashMap<String, DirectorySubspace> directorySubspaceCache = new ConcurrentHashMap<>();

    public KronotopDirectoryLayer(Database database, String clusterName) {
        this.database = database;
        this.clusterName = clusterName;
    }

    private List<String> getDataStructureSubpath(int shardId, DataStructure dataStructure) {
        return DirectoryLayout.Builder.
                clusterName(clusterName).
                internal().
                redis().
                persistence().
                logicalDatabase(LogicalDatabase.NAME).
                shardId(Integer.toString(shardId)).
                dataStructure(dataStructure.name().toLowerCase()).
                asList();
    }

    private synchronized List<String> getDirectorySubpath(int shardId, DataStructure dataStructure) {
        DataStructureSubpath dataStructureSubpath = datastructures.compute(shardId, (id, value) -> {
            if (value == null) {
                DataStructureSubpath newValue = new DataStructureSubpath();
                List<String> subpath = getDataStructureSubpath(id, dataStructure);
                newValue.put(dataStructure, subpath);
                return newValue;
            }
            value.computeIfAbsent(dataStructure, (ds) -> getDataStructureSubpath(id, ds));
            return value;
        });
        return dataStructureSubpath.get(dataStructure);
    }

    /**
     * Creates or opens a directory subspace based on the given shard ID and data structure type.
     *
     * @param shardId       The ID of the shard.
     * @param dataStructure The type of the data structure.
     * @return The directory subspace.
     */
    public DirectorySubspace openDataStructure(int shardId, DataStructure dataStructure) {
        return openDirectorySubspace(getDirectorySubpath(shardId, dataStructure));
    }

    /**
     * Creates or opens a directory subspace based on the given shard ID and data structure type.
     *
     * @param shardId       The ID of the shard.
     * @param dataStructure The type of the data structure.
     * @return The directory subspace.
     */
    public DirectorySubspace createOrOpenDataStructure(int shardId, DataStructure dataStructure) {
        return createOrOpenDirectorySubspace(getDirectorySubpath(shardId, dataStructure));
    }

    /**
     * Creates or opens a directory subspace based on the given subpath.
     *
     * @param subpath The list of strings representing the subpath of the directory subspace.
     * @return The directory subspace.
     */
    public DirectorySubspace openDirectorySubspace(List<String> subpath) {
        String key = String.join(".", subpath);
        return directorySubspaceCache.computeIfAbsent(key, (k) -> database.run(tr -> DirectoryLayer.getDefault().open(tr, subpath).join()));
    }

    /**
     * Creates or opens a directory subspace based on the given subpath.
     *
     * @param subpath The list of strings representing the subpath of the directory subspace.
     * @return The directory subspace.
     */
    public DirectorySubspace createOrOpenDirectorySubspace(List<String> subpath) {
        String key = String.join(".", subpath);
        return directorySubspaceCache.computeIfAbsent(key, (k) -> database.run(tr -> DirectoryLayer.getDefault().createOrOpen(tr, subpath).join()));
    }

    private static class DataStructureSubpath extends HashMap<DataStructure, List<String>> {
    }
}
