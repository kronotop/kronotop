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

/**
 * Represents a task for performing a vacuuming operation on a specified volume.
 * The task ensures that the garbage ratio within the volume does not exceed
 * the allowed threshold.
 */
public class VacuumTask implements Task {
    private final Context context;
    private final String name;
    private final double allowedGarbageRatio;
    private Vacuum vacuum;
    private volatile boolean completed;
    private volatile boolean shutdown;

    public VacuumTask(Context context, String name, double allowedGarbageRatio) {
        this.context = context;
        this.name = name;
        this.allowedGarbageRatio = allowedGarbageRatio;
    }

    @Override
    public String name() {
        return "vacuum:" + name;
    }

    @Override
    public boolean isCompleted() {
        return completed;
    }

    /**
     * Executes the vacuuming task for a specific volume. This method retrieves a
     * VolumeService from the context and uses it to find and initialize the target volume.
     * It then creates and starts a Vacuum instance to perform the vacuuming operation as
     * a blocking call. Upon successful execution, it updates the task status to completed.
     * <p>
     * If an exception occurs during the process (e.g., ClosedVolumeException,
     * VolumeNotOpenException, or IOException), it wraps and rethrows the exception as
     * a KronotopException.
     *
     * @throws KronotopException if an error occurs during the vacuuming operation
     */
    @Override
    public void run() {
        VolumeService service = context.getService(VolumeService.NAME);
        try {
            Volume volume = service.findVolume(name);
            vacuum = new Vacuum(context, volume, allowedGarbageRatio);
            // Blocking call
            vacuum.start();
        } catch (ClosedVolumeException | VolumeNotOpenException | IOException e) {
            throw new KronotopException(e);
        }
        completed = true;
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
