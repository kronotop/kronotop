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

import com.kronotop.cluster.RoutingEventHook;
import com.kronotop.cluster.sharding.ShardKind;

public class SubmitVacuumTaskHook implements RoutingEventHook {
    private final VolumeService service;

    public SubmitVacuumTaskHook(VolumeService service) {
        this.service = service;
    }

    @Override
    public void run(ShardKind shardKind, int shardId) {
        String name = VolumeNames.format(shardKind, shardId);
        try {
            Volume volume = service.findVolume(name);
            service.submitVacuumTaskIfAny(volume);
        } catch (VolumeNotOpenException ignored) {
            // The volume is not open, so there cannot be a running Vacuum task.
        }
    }
}
