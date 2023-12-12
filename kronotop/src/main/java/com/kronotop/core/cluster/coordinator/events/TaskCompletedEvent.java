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

package com.kronotop.core.cluster.coordinator.events;

import com.kronotop.core.cluster.coordinator.tasks.BaseTask;

public class TaskCompletedEvent extends BaseCoordinatorEvent {
    private String taskId;

    TaskCompletedEvent() {
    }

    public TaskCompletedEvent(BaseTask baseTask) {
        super(CoordinatorEventType.TASK_COMPLETED, baseTask.getShardId());
        this.taskId = baseTask.getTaskId();
    }

    public String getTaskId() {
        return taskId;
    }

    @Override
    public String toString() {
        return String.format("TaskCompletedEvent {shardId: %d taskId: %s type: %s}", getShardId(), taskId, getType());
    }
}
