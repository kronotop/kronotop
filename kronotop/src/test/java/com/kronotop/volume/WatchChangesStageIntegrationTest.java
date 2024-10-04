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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.awaitility.Awaitility.await;

public class WatchChangesStageIntegrationTest extends BaseNetworkedVolumeTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(WatchChangesStageIntegrationTest.class);

    @TempDir
    private Path standbyVolumeDataDir;

    private Replication newReplication() {
        final Host source;
        final Versionstamp jobId = ReplicationJob.newJob(database, volume.getConfig().subspace(), context.getMember());
        try (Transaction tr = database.createTransaction()) {
            VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, volume.getConfig().subspace());
            source = volumeMetadata.getOwner();
        }

        Host destination = new Host(Role.STANDBY, context.getMember());
        ReplicationConfig config = new ReplicationConfig(
                source,
                destination,
                volume.getConfig().subspace(),
                jobId,
                volume.getConfig().name(),
                volume.getConfig().segmentSize(),
                standbyVolumeDataDir.toString(),
                true
        );
        return new Replication(context, config);
    }

    private Versionstamp[] appendKeys(int number) throws IOException {
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(10);
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            AppendResult result = volume.append(session, entries);
            tr.commit().join();

            LOGGER.info("Successfully appended {} keys", number);
            return result.getVersionstampedKeys();
        }
    }

    private Volume standbyVolume() throws IOException {
        VolumeConfig standbyVolumeConfig = new VolumeConfig(
                volume.getConfig().subspace(),
                volume.getConfig().name(),
                standbyVolumeDataDir.toString(),
                volume.getConfig().segmentSize(),
                volume.getConfig().allowedGarbageRatio()
        );
        return new Volume(context, standbyVolumeConfig);
    }

    private boolean checkAppendedEntries(Versionstamp[] versionstampedKeys, Volume standbyVolume) throws IOException {
        for (Versionstamp versionstampedKey : versionstampedKeys) {
            try {
                ByteBuffer buf = volume.get(versionstampedKey);
                if (buf == null) {
                    return false;
                }
                ByteBuffer replicaBuf = standbyVolume.get(versionstampedKey);
                if (!Arrays.equals(buf.array(), replicaBuf.array())) {
                    return false;
                }
            } catch (SegmentNotFoundException e) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void test_watch_changes_stage() throws IOException {
        Replication replication = newReplication();
        Volume standbyVolume = standbyVolume();
        try {
            replication.start();

            await().atMost(10, TimeUnit.SECONDS).until(() -> replication.getActiveStageRunner() != null);
            WatchChangesStageRunner watchChangesStageRunner = (WatchChangesStageRunner) replication.getActiveStageRunner();
            await().atMost(10, TimeUnit.SECONDS).until(watchChangesStageRunner::isWatching);

            Versionstamp[] versionstampedKeys = appendKeys(10);
            await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(versionstampedKeys, standbyVolume));
        } finally {
            replication.stop();
        }
    }

    @Test
    public void test_watch_changes_stage_concurrently_appending_keys() throws IOException, InterruptedException {
        Replication replication = newReplication();
        Volume standbyVolume = standbyVolume();
        try {
            replication.start();

            await().atMost(10, TimeUnit.SECONDS).until(() -> replication.getActiveStageRunner() != null);
            WatchChangesStageRunner watchChangesStageRunner = (WatchChangesStageRunner) replication.getActiveStageRunner();
            await().atMost(10, TimeUnit.SECONDS).until(watchChangesStageRunner::isWatching);

            CountDownLatch latch = new CountDownLatch(10);
            List<Versionstamp> versionstampedKeys = new ArrayList<>();
            Lock lock = new ReentrantLock();
            try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
                AppendKeysRunnable appendKeysRunnable = new AppendKeysRunnable(versionstampedKeys, lock, latch);
                for (int i = 0; i < 10; i++) {
                    executorService.submit(appendKeysRunnable);
                }
            }
            latch.await();

            lock.lock();
            try {
                Versionstamp[] result = versionstampedKeys.toArray(Versionstamp[]::new);
                await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(result, standbyVolume));
            } finally {
                lock.unlock();
            }
        } finally {
            replication.stop();
        }
    }

    private class AppendKeysRunnable implements Runnable {
        private final Lock lock;
        private final List<Versionstamp> versionstampedKeys;
        private final CountDownLatch latch;

        AppendKeysRunnable(List<Versionstamp> versionstampedKeys, Lock lock, CountDownLatch latch) {
            this.lock = lock;
            this.versionstampedKeys = versionstampedKeys;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                Versionstamp[] result = appendKeys(10);
                lock.lock();
                try {
                    versionstampedKeys.addAll(Arrays.stream(result).toList());
                } finally {
                    lock.unlock();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                latch.countDown();
            }
        }
    }
}
