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

package com.kronotop.cluster.handlers;

public enum KrAdminSubcommand {
    INITIALIZE_CLUSTER("initialize-cluster"),
    DESCRIBE_CLUSTER("describe-cluster"),
    LIST_MEMBERS("list-members"),
    SET_MEMBER_STATUS("set-member-status"),
    FIND_MEMBER("find-member"),
    REMOVE_MEMBER("remove-member"),
    LIST_SILENT_MEMBERS("list-silent-members"),
    ROUTE("route"),
    SET_SHARD_STATUS("set-shard-status"),
    DESCRIBE_SHARD("describe-shard"),
    SYNC_STANDBY("sync-standby");

    private final String value;

    KrAdminSubcommand(String value) {
        this.value = value;
    }

    public static KrAdminSubcommand valueOfSubcommand(String command) {
        for (KrAdminSubcommand value : values()) {
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
