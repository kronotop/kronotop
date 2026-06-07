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

import com.kronotop.cluster.sharding.ShardKind;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardRegistryTest {

    @Test
    void shouldReturnOnlyBucketWhenStashDisabled() {
        // Behavior: getShardKinds() excludes STASH when stash.enabled is false.
        ShardRegistry registry = new ShardRegistry(
                ConfigFactory.parseMap(Map.of("stash.enabled", false, "stash.shards", 1)),
                "test-cluster"
        );
        assertEquals(Set.of(ShardKind.BUCKET), registry.getShardKinds());
    }

    @Test
    void shouldReturnBothKindsWhenStashEnabled() {
        // Behavior: getShardKinds() includes both STASH and BUCKET when stash.enabled is true.
        ShardRegistry registry = new ShardRegistry(
                ConfigFactory.parseMap(Map.of("stash.enabled", true, "stash.shards", 3)),
                "test-cluster"
        );
        assertEquals(Set.of(ShardKind.STASH, ShardKind.BUCKET), registry.getShardKinds());
    }

    @Test
    void shouldReturnEmptyStashShardIdsWhenDisabled() {
        // Behavior: getShardIds(STASH) returns empty list when stash is disabled.
        ShardRegistry registry = new ShardRegistry(
                ConfigFactory.parseMap(Map.of("stash.enabled", false, "stash.shards", 1)),
                "test-cluster"
        );
        assertTrue(registry.getShardIds(ShardKind.STASH).isEmpty());
    }

    @Test
    void shouldReturnContiguousStashShardIdsWhenEnabled() {
        // Behavior: getShardIds(STASH) returns contiguous 0..N-1 shard IDs when stash is enabled.
        ShardRegistry registry = new ShardRegistry(
                ConfigFactory.parseMap(Map.of("stash.enabled", true, "stash.shards", 3)),
                "test-cluster"
        );
        assertEquals(List.of(0, 1, 2), registry.getShardIds(ShardKind.STASH));
    }
}
