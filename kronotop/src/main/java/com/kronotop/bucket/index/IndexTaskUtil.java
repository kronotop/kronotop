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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.directory.KronotopDirectory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
        return context.getDirectorySubspaceCache().get(layout);
    }

    public static void modifyTaskCounter(Context context, Transaction tr, Versionstamp taskId, int delta) {
        List<String> layout = KronotopDirectory.
                kronotop().cluster(context.getClusterName()).metadata().
                buckets().maintenance().index().counter().toList();
        DirectorySubspace subspace = context.getDirectorySubspaceCache().get(layout);
        byte[] data = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(delta).array();
        byte[] key = subspace.pack(taskId);
        tr.mutate(MutationType.ADD, key, data);
    }

    public static int readTaskCounter(Context context, Versionstamp taskId) {
        List<String> layout = KronotopDirectory.
                kronotop().cluster(context.getClusterName()).metadata().
                buckets().maintenance().index().counter().toList();
        DirectorySubspace subspace = context.getDirectorySubspaceCache().get(layout);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] key = subspace.pack(taskId);
            byte[] data = tr.get(key).join();
            return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
        }
    }
}
