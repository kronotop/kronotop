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

package com.kronotop.internal;

import com.apple.foundationdb.directory.DirectoryLayer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KronotopDirectoryLayerTest {

    @Test
    void shouldReturnDefaultLayerWhenRootIsAbsent() {
        // Behavior: without directory.root the global default layer is used, leaving production keyspace unchanged.
        Config config = ConfigFactory.empty();
        assertSame(DirectoryLayer.getDefault(), KronotopDirectoryLayer.fromConfig(config));
    }

    @Test
    void shouldReturnScopedLayerWhenRootIsPresent() {
        // Behavior: directory.root switches to a layer scoped under a dedicated prefix, distinct from the default.
        Config config = ConfigFactory.parseString("directory.root = test-cluster");
        DirectoryLayer layer = KronotopDirectoryLayer.fromConfig(config);
        assertNotEquals(DirectoryLayer.getDefault(), layer);
    }

    @Test
    void shouldIsolateLayersAcrossDifferentRoots() {
        // Behavior: different roots yield layers with disjoint node/content subspaces, so they share no FDB keys.
        DirectoryLayer a = KronotopDirectoryLayer.scoped("cluster-a");
        DirectoryLayer b = KronotopDirectoryLayer.scoped("cluster-b");
        assertNotEquals(a, b);
    }

    @Test
    void shouldBeDeterministicForSameRoot() {
        // Behavior: the prefix is a pure function of the root, so the cached layer and any context-less rebuild match.
        assertEquals(KronotopDirectoryLayer.scoped("cluster-a"), KronotopDirectoryLayer.scoped("cluster-a"));
    }
}
