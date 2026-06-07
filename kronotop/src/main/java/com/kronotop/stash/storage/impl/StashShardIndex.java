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
import com.kronotop.stash.storage.index.Index;
import com.kronotop.stash.storage.index.impl.IndexImpl;

public class StashShardIndex extends IndexImpl implements Index {
    private final StashShard shard;

    public StashShardIndex(int id, StashShard shard) {
        super(id);
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
    public void add(String key) {
        checkShardStatus();
        super.add(key);
    }

    @Override
    public void remove(String key) {
        checkShardStatus();
        super.remove(key);
    }
}
