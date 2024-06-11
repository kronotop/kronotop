/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.cluster.sharding;

import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.kronotop.cluster.coordinator.tasks.AssignShardTask;
import com.kronotop.cluster.coordinator.tasks.BaseTask;
import com.kronotop.cluster.coordinator.tasks.ReassignShardTask;
import com.kronotop.network.Address;

import java.util.HashMap;

/**
 * Represents metadata for a shard, including status, owner, and tasks.
 */
public class ShardMetadata {
    private final HashMap<String, Task> tasks = new HashMap<>();
    private ShardOwner owner;

    ShardMetadata() {
    }

    public ShardMetadata(Address address, Versionstamp processId) {
        this.owner = new ShardOwner(address, processId);
    }

    public ShardOwner getOwner() {
        return owner;
    }

    public void setOwner(ShardOwner owner) {
        this.owner = owner;
    }

    public HashMap<String, Task> getTasks() {
        return tasks;
    }

    @Override
    public String toString() {
        return String.format(
                "ShardMetadata {owner=%s}",
                owner
        );
    }

    public static class Task {
        private AssignShardTask assignShardTask;
        private ReassignShardTask reassignShardTask;
        @JsonIgnore
        private BaseTask baseTask;

        Task() {
        }

        public Task(AssignShardTask assignShardTask) {
            this.assignShardTask = assignShardTask;
        }

        public Task(ReassignShardTask reassignShardTask) {
            this.reassignShardTask = reassignShardTask;
        }

        public synchronized BaseTask getBaseTask() {
            if (baseTask != null) {
                return baseTask;
            }

            if (assignShardTask != null) {
                baseTask = assignShardTask;
            } else if (reassignShardTask != null) {
                baseTask = reassignShardTask;
            }
            return baseTask;
        }

        public ReassignShardTask getReassignShardTask() {
            return reassignShardTask;
        }

        public AssignShardTask getAssignShardTask() {
            return assignShardTask;
        }
    }
}
