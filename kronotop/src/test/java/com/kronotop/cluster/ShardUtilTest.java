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

import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.directory.Shards;
import com.kronotop.transaction.TransactionUtil;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

class ShardUtilTest extends BaseStandaloneInstanceTest {

    private Shards.Shard shardDirectory(int shardId) {
        return KronotopDirectory.kronotop().cluster(context.getClusterName()).metadata().shards().shard(shardId);
    }

    private boolean directoryExists(KronotopDirectoryNode directory) {
        return TransactionUtil.execute(context, tr -> context.getDirectoryLayer().exists(tr, directory.toList()).join());
    }

    @Test
    void shouldCreateShard() {
        // Behavior: On a cluster initialized with NUM_SHARDS shards, createShard is called for the unused id
        // NUM_SHARDS and the transaction is committed. The returned subspace is the directory of the new shard.
        // The shard is stored with the status INOPERABLE, and its index task directory and its last seen versions
        // directory exist.
        DirectorySubspace shardSubspace = TransactionUtil.executeThenCommit(context, tr -> ShardUtil.createShard(context, tr, NUM_SHARDS));
        assertEquals(shardDirectory(NUM_SHARDS).toList(), shardSubspace.getPath());

        ShardStatus status = TransactionUtil.execute(context, tr -> ShardUtil.getShardStatus(context, tr, NUM_SHARDS));
        assertEquals(ShardStatus.INOPERABLE, status);

        assertTrue(directoryExists(shardDirectory(NUM_SHARDS).buckets().maintenance().index().tasks()));
        assertTrue(directoryExists(shardDirectory(NUM_SHARDS).buckets().lastSeenVersions()));
    }

    @Test
    void shouldThrowWhenShardAlreadyExists() {
        // Behavior: On a cluster initialized with NUM_SHARDS shards, createShard is called for shard 0, which
        // already exists. The call fails with a CompletionException caused by DirectoryAlreadyExistsException.
        CompletionException exception = assertThrows(CompletionException.class,
                () -> TransactionUtil.execute(context, tr -> ShardUtil.createShard(context, tr, 0)));
        assertInstanceOf(DirectoryAlreadyExistsException.class, exception.getCause());
    }
}
