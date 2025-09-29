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

import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.directory.KronotopDirectory;

import java.util.List;

public class IndexTaskUtil {
    /**
     * Creates or opens a DirectorySubspace for index maintenance tasks based on the specified context and shard ID.
     * This method generates the directory layout using the Kronotop directory structure,
     * retrieves the FoundationDB database from the provided context, and operates on the subspace.
     *
     * @param context the context associated with the Kronotop instance, providing cluster and FoundationDB database access
     * @param shardId the ID of the shard for which the subspace will be created or opened
     * @return the DirectorySubspace corresponding to the specified IndexTask's subspace
     */
    public static DirectorySubspace createOrOpenTasksSubspace(Context context, int shardId) {
        List<String> layout = KronotopDirectory.
                kronotop().cluster(context.getClusterName()).metadata().
                shards().bucket().shard(shardId).maintenance().
                index().tasks().toList();
        return context.getFoundationDB().run(tr -> DirectoryLayer.getDefault().createOrOpen(tr, layout).join());
    }
}
