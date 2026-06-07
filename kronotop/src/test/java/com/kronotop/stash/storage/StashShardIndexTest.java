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

package com.kronotop.stash.storage;

import com.kronotop.stash.storage.impl.OnHeapStashShardImpl;
import com.kronotop.stash.storage.impl.StashShardIndex;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class StashShardIndexTest extends BaseStorageTest {
    @Test
    public void test_add() {
        StashShard shard = new OnHeapStashShardImpl(context, 0);
        makeAllShardsReadOnly();
        StashShardIndex index = new StashShardIndex(0, shard);
        assertThrows(ShardReadOnlyException.class, () -> index.add("foo"));
    }

    @Test
    public void test_remove() {
        StashShard shard = new OnHeapStashShardImpl(context, 0);
        makeAllShardsReadOnly();
        StashShardIndex index = new StashShardIndex(0, shard);
        assertThrows(ShardReadOnlyException.class, () -> index.remove("foo"));
    }
}
