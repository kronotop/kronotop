/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume.handlers.protocol;

public enum VolumeAdminSubcommand {
    LIST("list"),
    DESCRIBE("describe"),
    SET_STATUS("set-status"),
    VACUUM("vacuum"),
    STOP_VACUUM("stop-vacuum"),
    CLEANUP_ORPHAN_FILES("cleanup-orphan-files"),
    MARK_STALE_PREFIXES("mark-stale-prefixes"),
    LIST_SEGMENTS("list-segments"),
    START_REPLICATION("start-replication"),
    STOP_REPLICATION("stop-replication"),
    PRUNE_CHANGELOG("prune-changelog");

    private final String value;

    VolumeAdminSubcommand(String value) {
        this.value = value;
    }

    public static VolumeAdminSubcommand valueOfSubcommand(String command) {
        for (VolumeAdminSubcommand value : values()) {
            if (value.value.equalsIgnoreCase(command)) {
                return value;
            }
        }
        throw new IllegalArgumentException();
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
