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
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class VacuumTaskTest extends BaseVolumeIntegrationTest {

    private VacuumTask prepareTestEnv() throws IOException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        VacuumMetadata vacuumMetadata = new VacuumMetadata(volume.getConfig().name(), 0);
        return new VacuumTask(context, volume, vacuumMetadata);
    }

    @Test
    void shouldCompleteVacuumTask() throws IOException {
        VacuumTask task = prepareTestEnv();
        try {
            task.run();
            assertTrue(task.isCompleted());
        } finally {
            task.shutdown();
        }
    }

    @Test
    void shouldCompleteImmediatelyWhenNoSegmentsExist() {
        // No data appended - volume has no segments
        VacuumMetadata vacuumMetadata = new VacuumMetadata(volume.getConfig().name(), 0);
        VacuumTask task = new VacuumTask(context, volume, vacuumMetadata);

        task.run();

        assertTrue(task.isCompleted());
    }

    @Test
    void shouldStopGracefullyOnShutdown() throws IOException {
        VacuumTask task = prepareTestEnv();

        Thread taskThread = new Thread(task);
        taskThread.start();

        // Shutdown should complete gracefully without throwing
        assertDoesNotThrow(task::shutdown);

        // Task has stopped running
        assertTrue(task.isCompleted());
    }
}