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

package com.kronotop.stash.storage.impl;

import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.stash.storage.ShardInoperableException;
import com.kronotop.stash.storage.ShardReadOnlyException;
import com.kronotop.stash.storage.StashShard;
import com.kronotop.stash.storage.syncer.VolumeSyncQueue;
import com.kronotop.stash.storage.syncer.impl.OnHeapVolumeSyncQueue;
import com.kronotop.stash.storage.syncer.jobs.VolumeSyncJob;

/**
 * A volume synchronization queue specifically tailored to work with Stash shards.
 * <p>
 * This class inherits from {@link OnHeapVolumeSyncQueue} and implements the {@link VolumeSyncQueue} interface.
 * It ensures that synchronization jobs are only added to the queue if the shard's status allows it,
 * i.e., the shard must not be in a read-only or inoperable state.
 */
public class StashShardVolumeSyncQueue extends OnHeapVolumeSyncQueue implements VolumeSyncQueue {
    private final StashShard shard;

    public StashShardVolumeSyncQueue(StashShard shard) {
        this.shard = shard;
    }

    private void checkShardStatus() {
        if (shard.status().equals(ShardStatus.READONLY)) {
            throw new ShardReadOnlyException(shard.id());
        }
        if (shard.status().equals(ShardStatus.INOPERABLE)) {
            throw new ShardInoperableException(shard.id());
        }
    }

    @Override
    public void add(VolumeSyncJob job) {
        checkShardStatus();
        super.add(job);
    }
}
