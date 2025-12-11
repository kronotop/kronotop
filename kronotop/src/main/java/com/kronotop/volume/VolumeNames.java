/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume;

import com.kronotop.cluster.sharding.ShardKind;

public final class VolumeNames {
    private VolumeNames() {
    }

    public static String format(ShardKind shardKind, int shardId) {
        return String.format("%s-shard-%d", shardKind, shardId).toLowerCase();
    }

    public static Parsed parse(String name) {
        if (name == null) {
            throw new IllegalArgumentException("name cannot be null");
        }

        String[] parts = name.split("-");
        if (parts.length != 3) {
            throw new IllegalArgumentException("invalid volume name: " + name);
        }

        if (!parts[1].equals("shard")) {
            throw new IllegalArgumentException("invalid volume name: " + name);
        }

        ShardKind kind;
        try {
            kind = ShardKind.valueOf(parts[0].toUpperCase());
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid shard kind: " + parts[0], e);
        }

        int shardId;
        try {
            shardId = Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid shard id: " + parts[2], e);
        }

        return new Parsed(kind, shardId);
    }

    public record Parsed(ShardKind shardKind, int shardId) {
    }
}
