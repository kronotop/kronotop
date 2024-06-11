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

package com.kronotop.cluster.coordinator.tasks;

import java.time.Instant;
import java.util.UUID;

/**
 * The BaseTask class represents a base task with common attributes such as taskId, shardId, type, and createdAt.
 */
public class BaseTask {
    private String taskId;
    private int shardId;
    private TaskType type;
    private long createdAt;

    BaseTask() {
    }

    public BaseTask(TaskType type, int shardId, long createdAt) {
        this.taskId = UUID.randomUUID().toString();
        this.type = type;
        this.shardId = shardId;
        this.createdAt = createdAt;
    }

    public BaseTask(TaskType type, int shardId) {
        this(type, shardId, Instant.now().toEpochMilli());
    }

    public String getTaskId() {
        return taskId;
    }

    public TaskType getType() {
        return type;
    }

    public int getShardId() {
        return shardId;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    @Override
    public String toString() {
        return String.format("Task {shardId: %d taskId: %s type: %s}", shardId, taskId, type);
    }
}
