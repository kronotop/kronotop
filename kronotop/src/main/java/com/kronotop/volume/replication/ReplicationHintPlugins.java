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

package com.kronotop.volume.replication;

import com.apple.foundationdb.tuple.Versionstamp;

import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registry of replication hint plugins keyed by hint kind.
 *
 * <p>Owned by {@link ReplicationService} and shared by all replication stages.
 * Plugins are registered at startup. There is no unregister.</p>
 */
public class ReplicationHintPlugins {
    private final Map<ReplicationHintKind, List<ReplicationHintPlugin>> plugins = new EnumMap<>(ReplicationHintKind.class);

    public void register(ReplicationHintKind kind, ReplicationHintPlugin plugin) {
        plugins.computeIfAbsent(kind, k -> new CopyOnWriteArrayList<>()).add(plugin);
    }

    public void dispatch(ReplicationHintEntry hint, Versionstamp addVs, ByteBuffer document) {
        List<ReplicationHintPlugin> list = plugins.get(hint.kind());
        if (list == null) {
            return;
        }
        for (ReplicationHintPlugin plugin : list) {
            plugin.process(hint, addVs, document);
        }
    }
}
