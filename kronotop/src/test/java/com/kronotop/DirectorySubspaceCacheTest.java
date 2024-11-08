/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.cluster.sharding.ShardKind;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DirectorySubspaceCacheTest extends BaseClusterTest {

    @Test
    public void test_CLUSTER_METADATA() {
        KronotopTestInstance instance = getInstances().getFirst();
        DirectorySubspaceCache cache = new DirectorySubspaceCache(
                instance.getContext().getClusterName(),
                instance.getContext().getFoundationDB()
        );
        DirectorySubspace subspace = assertDoesNotThrow(() -> cache.get(DirectorySubspaceCache.Key.CLUSTER_METADATA));
        assertNotNull(subspace);
    }

    @Test
    public void test_REDIS_SHARD() {
        KronotopTestInstance instance = getInstances().getFirst();
        DirectorySubspaceCache cache = new DirectorySubspaceCache(
                instance.getContext().getClusterName(),
                instance.getContext().getFoundationDB()
        );
        DirectorySubspace subspace = assertDoesNotThrow(() -> cache.get(ShardKind.REDIS, 1));
        assertNotNull(subspace);
    }
}