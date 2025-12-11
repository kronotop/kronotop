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

import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.task.BaseTask;
import com.kronotop.task.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class VacuumTask extends BaseTask implements Task {
    private static final Logger LOGGER = LoggerFactory.getLogger(VacuumTask.class);
    private final Context context;
    private final Volume volume;
    private final VacuumMetadata vacuumMetadata;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final Vacuum vacuum;
    private volatile boolean shutdown;

    public VacuumTask(Context context, Volume volume, VacuumMetadata vacuumMetadata) {
        this.context = context;
        this.volume = volume;
        this.vacuumMetadata = vacuumMetadata;
        this.vacuum = new Vacuum(context, volume, vacuumMetadata);
    }

    @Override
    public String name() {
        return vacuumMetadata.getTaskName();
    }

    @Override
    public boolean isCompleted() {
        return latch.getCount() == 0;
    }

    @Override
    public void complete() {
        TransactionUtils.executeThenCommit(context, (tr) -> {
            VacuumMetadata.remove(tr, volume.getConfig().subspace());
            return null;
        });
    }

    @Override
    public void task() {
        try {
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
        } finally {
            latch.countDown();
        }
    }

    @Override
    public void shutdown() {
        shutdown = true;
        vacuum.stop();
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                LOGGER.warn("{} cannot be stopped gracefully", name());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KronotopException("Operation was interrupted while waiting", e);
        }
    }
}
