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

package com.kronotop.cluster;

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.internal.DirectorySubspaceCache;

public class ClusterUtil {
    private static final byte[] LITTLE_ENDIAN_ONE = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian

    /**
     * Signals a topology change to all members that watch the cluster topology key.
     *
     * @param context the context of the Kronotop instance
     * @param tr      the transaction that carries the signal; members see it after the commit
     */
    public static void signalTopologyChange(Context context, Transaction tr) {
        DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
        byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_TOPOLOGY_CHANGED));
        tr.mutate(MutationType.ADD, key, LITTLE_ENDIAN_ONE);
    }

}
