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
import com.kronotop.Context;
import com.kronotop.common.KronotopException;
import com.kronotop.task.BaseTask;
import com.kronotop.task.Task;
import com.kronotop.task.TaskStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a task for performing a vacuuming operation on a specified volume.
 * The task ensures that the garbage ratio within the volume does not exceed
 * the allowed threshold.
 */
public class VacuumTask extends BaseTask implements Task {
    private static final Logger LOGGER = LoggerFactory.getLogger(VacuumTask.class);
    private final Context context;
    private final Volume volume;
    private final VacuumMetadata vacuumMetadata;
    private final CountDownLatch latch = new CountDownLatch(1);
    private Vacuum vacuum;
    private volatile boolean shutdown;

    public VacuumTask(Context context, Volume volume, VacuumMetadata vacuumMetadata) {
        this.context = context;
        this.volume = volume;
        this.vacuumMetadata = vacuumMetadata;
    }

    @Override
    public String name() {
        return vacuumMetadata.getTaskName();
    }

    @Override
    public boolean isCompleted() {
        return latch.getCount() == 0;
    }

    public void complete() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumMetadata.remove(tr, volume.getConfig().subspace());
            tr.commit().join();
        }
        // removed from FDB, mark it as completed.
        latch.countDown();
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
    public void task() {
        try {
            vacuum = new Vacuum(context, volume, vacuumMetadata);
            // Blocking call
            List<String> files = vacuum.start();
            for (String file : files) {
                LOGGER.debug("Cleaned up stale data file: {} on volume '{}'", file, volume.getConfig().name());
            }
            if (!shutdown) {
                // Only complete the task if Vacuum.start processed all entries without interruption.
                complete();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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

    @Override
    public void awaitCompletion() throws InterruptedException {
        latch.await();
    }
}
