/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.core.cluster.sharding;

import com.kronotop.core.cluster.coordinator.tasks.AssignShardTask;
import com.kronotop.core.cluster.coordinator.tasks.BaseTask;
import com.kronotop.core.cluster.coordinator.tasks.ReassignShardTask;
import com.kronotop.core.network.Address;

import java.util.HashMap;

public class ShardMetadata {
    private final HashMap<String, Task> tasks = new HashMap<>();
    private ShardStatus status = ShardStatus.INOPERABLE;
    private ShardOwner owner;

    ShardMetadata() {
    }

    public ShardMetadata(Address address, long processId) {
        this.owner = new ShardOwner(address, processId);
    }

    public ShardStatus getStatus() {
        return status;
    }

    public void setStatus(ShardStatus status) {
        this.status = status;
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
                "ShardMetadata {owner=%s status=%s}",
                owner,
                status
        );
    }

    public static class Task {
        private AssignShardTask assignShardTask;
        private ReassignShardTask reassignShardTask;
        private BaseTask baseTask;

        Task() {
        }

        public Task(AssignShardTask assignShardTask) {
            this.assignShardTask = assignShardTask;
            this.baseTask = this.assignShardTask;
        }

        public Task(ReassignShardTask reassignShardTask) {
            this.reassignShardTask = reassignShardTask;
            this.baseTask = reassignShardTask;
        }

        public BaseTask getBaseTask() {
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
