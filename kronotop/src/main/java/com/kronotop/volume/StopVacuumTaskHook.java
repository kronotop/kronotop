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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.kronotop.cluster.RoutingEventHook;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.task.Task;
import com.kronotop.task.TaskNotFoundException;
import com.kronotop.task.TaskService;

public class StopVacuumTaskHook implements RoutingEventHook {
    private final VolumeService service;

    public StopVacuumTaskHook(VolumeService service) {
        this.service = service;
    }

    private Task getTask(String taskName) {
        TaskService taskService = service.getContext().getService(TaskService.NAME);
        try {
            return taskService.getTask(taskName);
        } catch (IllegalArgumentException e) {
            // not found
            return null;
        }
    }

    @Override
    public void run(ShardKind shardKind, int shardId) {
        String name = VolumeConfigGenerator.volumeName(shardKind, shardId);
        Volume volume = service.findVolume(name);
        if (service.hasVolumeOwnership(volume)) {
            // Current owner, no need to stop it.
            return;
        }
        try (Transaction tr = service.getContext().getFoundationDB().createTransaction()) {
            VacuumMetadata vacuumMetadata = VacuumMetadata.load(tr, volume.getConfig().subspace());
            if (vacuumMetadata == null) {
                // No vacuum task found for this volume.
                return;
            }
            // Stop the task
            TaskService taskService = service.getContext().getService(TaskService.NAME);
            try {
                // TODO: Do we need to remove VacuumMetadata?
                Task task = taskService.getTask(vacuumMetadata.getTaskName());
                if (task == null) {
                    // No local task found
                    return;
                }
                task.shutdown();
                task.awaitCompletion();
            } catch (TaskNotFoundException e) {
                // Ignore
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
