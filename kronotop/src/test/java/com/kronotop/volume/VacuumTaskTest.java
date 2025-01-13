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
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VacuumTaskTest extends BaseVolumeIntegrationTest {

    private VacuumTask prepareTestEnv() throws IOException {
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Session session = new Session(tr, prefix);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        long readVersion = getReadVersion();
        VacuumMetadata vacuumMetadata = new VacuumMetadata(volume.getConfig().name(), readVersion, 0);
        return new VacuumTask(context, volume, vacuumMetadata);
    }

    @Test
    void test_VacuumTask() throws IOException {
        VacuumTask task = prepareTestEnv();
        try {
            task.run();
            assertTrue(task.isCompleted());
        } finally {
            task.shutdown();
        }
    }

    @Test
    void test_VacuumTask_awaitTermination() throws IOException {
        VacuumTask task = prepareTestEnv();

        CountDownLatch latch = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            // awaitTermination must be called before
            task.run();
        });

        await().atMost(Duration.ofSeconds(15)).until(() -> {
            latch.countDown();
            task.awaitTermination();
            return task.isCompleted();
        });
    }
}