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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;

import java.util.concurrent.CompletionException;

public class VolumeMetadataMaintainer {

    public static KronotopDirectoryNode getShardVolumeDirectory(Context context, int shardId) {
        return KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                volumes().
                volume(Integer.toString(shardId));
    }

    /**
     * Creates the directory subspace of the shard volume in the given transaction.
     *
     * @param tr the transaction to create the directory in. The caller commits it.
     * @return the directory subspace of the shard volume
     * @throws VolumeAlreadyExistsException if the volume already exists
     */
    public static DirectorySubspace createVolumeSubspace(Context context, Transaction tr, int shardId) {
        KronotopDirectoryNode directory = getShardVolumeDirectory(context, shardId);
        try {
            return context.getDirectoryLayer().create(tr, directory.toList()).join();
        } catch (CompletionException exp) {
            if (exp.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new VolumeAlreadyExistsException(VolumeNames.format(shardId));
            }
            throw exp;
        }
    }
}
