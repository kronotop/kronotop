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

package com.kronotop.volume;

import com.kronotop.Context;
import com.kronotop.common.KronotopException;
import com.kronotop.task.Task;

import java.io.IOException;

public class VacuumTask implements Task {
    private final Context context;
    private final String name;
    private Vacuum vacuum;
    private volatile boolean shutdown;

    public VacuumTask(Context context, String name) {
        this.context = context;
        this.name = name;
    }

    @Override
    public String name() {
        return "vacuum:" + name;
    }

    @Override
    public void run() {
        VolumeService service = context.getService(VolumeService.NAME);
        try {
            Volume volume = service.findVolume(name);
            vacuum = new Vacuum(context, volume);
            // Blocking call
            vacuum.start();
        } catch (ClosedVolumeException | VolumeNotOpenException | IOException e) {
            throw new KronotopException(e);
        }
    }

    @Override
    public void shutdown() {
        if (shutdown) {
            return;
        }
        shutdown = true;
        vacuum.stop();
    }
}