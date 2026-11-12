/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.cluster;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.transaction.TransactionUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterUtilTest extends BaseStandaloneInstanceTest {

    private int readTopologyCounter() {
        DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
        byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_TOPOLOGY_CHANGED));
        byte[] value = TransactionUtil.execute(context, tr -> tr.get(key).join());
        if (value == null) {
            return 0;
        }
        return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    @Test
    void shouldIncrementTopologyCounterWhenTopologyChangeSignaled() {
        // Behavior: The cluster topology counter is read, then signalTopologyChange is called in a transaction and the
        // transaction is committed. A second read returns exactly the first value plus one.
        int initialValue = readTopologyCounter();

        TransactionUtil.executeThenCommit(context, tr -> {
            ClusterUtil.signalTopologyChange(context, tr);
            return null;
        });

        assertEquals(initialValue + 1, readTopologyCounter());
    }
}
