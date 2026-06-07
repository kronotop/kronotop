/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.volume.segment.SegmentAnalysis;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class VacuumWatchDogTest extends BaseVolumeIntegrationTest {

    private final List<CountDownLatch> cleanupLatches = new ArrayList<>();

    @AfterEach
    void releaseBlockingEvacuators() {
        for (CountDownLatch latch : cleanupLatches) {
            latch.countDown();
        }
        cleanupLatches.clear();
    }

    private long fillAndSealSegmentWithGarbage(Prefix prefix) throws IOException {
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            appendResult = volume.append(session, getEntries(10));
            tr.commit().join();
        }
        Versionstamp[] keys = appendResult.getVersionstampedKeys();

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

        List<SegmentAnalysis> analyses = volume.analyze();
        assertTrue(analyses.size() >= 2);
        long sealedSegmentId = analyses.getFirst().segmentId();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            DeleteResult deleteResult = volume.delete(session, keys);
            tr.commit().join();
            deleteResult.complete();
        }

        return sealedSegmentId;
    }

    @Test
    void shouldStartWorkerForSegmentAboveThreshold() throws Exception {
        // Behavior: WatchDog starts a worker for a sealed segment with garbage above threshold
        long sealedSegmentId = fillAndSealSegmentWithGarbage(stashVolumeSyncerPrefix);

        BlockingEvacuator evacuator = new BlockingEvacuator();
        cleanupLatches.add(evacuator.proceedLatch);

        VacuumWatchDog watchDog = new VacuumWatchDog(context, volume, 0.001f, 2, () -> evacuator);
        Thread watchDogThread = Thread.ofVirtual().start(watchDog);

        assertTrue(evacuator.enteredLatch.await(5, TimeUnit.SECONDS));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());
            VacuumSegmentMetadata metadata = VacuumSegmentMetadataUtil.load(tr, volumeSubspace, sealedSegmentId);
            assertNotNull(metadata);
            assertEquals(VacuumMetadataStatus.EVACUATING, metadata.status());
        }

        evacuator.proceedLatch.countDown();
        watchDog.stop();
        watchDogThread.join(10_000);
        assertTrue(watchDog.isStopped());
    }

    @Test
    void shouldExitImmediatelyWhenNoSegmentsExceedThreshold() throws Exception {
        // Behavior: all segments below threshold, WatchDog exits with no workers started
        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            for (int i = 1; i <= numIterations; i++) {
                volume.append(session, randomBytes((int) bufferSize));
            }
            tr.commit().join();
        }

        List<SegmentAnalysis> analyses = volume.analyze();
        assertTrue(analyses.size() >= 2);
        long sealedSegmentId = analyses.getFirst().segmentId();

        VacuumWatchDog watchDog = new VacuumWatchDog(context, volume, 99.0f, 2, NoOpEvacuator::new);
        Thread watchDogThread = Thread.ofVirtual().start(watchDog);
        watchDogThread.join(5_000);

        assertTrue(watchDog.isStopped());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());
            VacuumSegmentMetadata segmentMetadata = VacuumSegmentMetadataUtil.load(tr, volumeSubspace, sealedSegmentId);
            assertNull(segmentMetadata);

            VacuumMetadata vacuumMetadata = VacuumMetadataUtil.load(tr, volumeSubspace);
            assertNotNull(vacuumMetadata);
            assertEquals(VacuumResult.NO_WORK, vacuumMetadata.result());
            assertEquals(0, vacuumMetadata.segmentsProcessed());
        }
    }

    @Test
    void shouldSkipWritableSegment() throws Exception {
        // Behavior: WatchDog skips the writable segment even if it has garbage
        AppendResult appendResult;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            appendResult = volume.append(session, getEntries(10));
            tr.commit().join();
        }
        Versionstamp[] keys = appendResult.getVersionstampedKeys();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, stashVolumeSyncerPrefix);
            DeleteResult deleteResult = volume.delete(session, keys);
            tr.commit().join();
            deleteResult.complete();
        }

        List<SegmentAnalysis> analyses = volume.analyze();
        assertEquals(1, analyses.size());

        VacuumWatchDog watchDog = new VacuumWatchDog(context, volume, 0.001f, 2, NoOpEvacuator::new);
        Thread watchDogThread = Thread.ofVirtual().start(watchDog);
        watchDogThread.join(5_000);

        assertTrue(watchDog.isStopped());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());
            List<VacuumSegmentMetadata> segmentMetadata = VacuumSegmentMetadataUtil.loadAll(tr, volumeSubspace);
            assertTrue(segmentMetadata.isEmpty());

            VacuumMetadata vacuumMetadata = VacuumMetadataUtil.load(tr, volumeSubspace);
            assertNotNull(vacuumMetadata);
            assertEquals(VacuumResult.NO_WORK, vacuumMetadata.result());
            assertEquals(0, vacuumMetadata.segmentsProcessed());
        }
    }

    @Test
    void shouldBlockUntilRunFinishesOnStop() throws Exception {
        // Behavior: stop() blocks until run() fully exits
        fillAndSealSegmentWithGarbage(stashVolumeSyncerPrefix);

        BlockingEvacuator evacuator = new BlockingEvacuator();
        cleanupLatches.add(evacuator.proceedLatch);

        VacuumWatchDog watchDog = new VacuumWatchDog(context, volume, 0.001f, 1, () -> evacuator);
        Thread.ofVirtual().start(watchDog);

        assertTrue(evacuator.enteredLatch.await(5, TimeUnit.SECONDS));

        Thread stopThread = Thread.ofVirtual().start(watchDog::stop);

        evacuator.proceedLatch.countDown();
        stopThread.join(10_000);

        assertFalse(stopThread.isAlive());
        assertTrue(watchDog.isStopped());
    }

    @Test
    void shouldHandleStopBeforeRunStarts() throws Exception {
        // Behavior: stop() called before run() starts does not hang
        VacuumWatchDog watchDog = new VacuumWatchDog(context, volume, 0.001f, 1, NoOpEvacuator::new);

        Thread stopThread = Thread.ofVirtual().start(watchDog::stop);

        Thread.ofVirtual().start(watchDog);

        stopThread.join(5_000);
        assertFalse(stopThread.isAlive());
        assertTrue(watchDog.isStopped());
    }

    @Test
    void shouldNotLeaveOrphanedWorkersAfterStop() throws Exception {
        // Behavior: after stop() returns, no unsupervised workers remain running
        long sealedSegmentId = fillAndSealSegmentWithGarbage(stashVolumeSyncerPrefix);

        BlockingEvacuator evacuator = new BlockingEvacuator();
        cleanupLatches.add(evacuator.proceedLatch);

        VacuumWatchDog watchDog = new VacuumWatchDog(context, volume, 0.001f, 1, () -> evacuator);
        Thread.ofVirtual().start(watchDog);

        assertTrue(evacuator.enteredLatch.await(5, TimeUnit.SECONDS));

        Thread stopThread = Thread.ofVirtual().start(watchDog::stop);

        evacuator.proceedLatch.countDown();
        stopThread.join(10_000);

        assertTrue(watchDog.isStopped());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());
            VacuumSegmentMetadata metadata = VacuumSegmentMetadataUtil.load(tr, volumeSubspace, sealedSegmentId);
            assertNotNull(metadata);
            assertEquals(VacuumMetadataStatus.STOPPED, metadata.status());
        }
    }

    @Test
    void shouldLimitConcurrentWorkers() throws Exception {
        // Behavior: WatchDog runs at most maxWorkers workers concurrently
        fillAndSealSegmentWithGarbage(stashVolumeSyncerPrefix);
        fillAndSealSegmentWithGarbage(stashVolumeSyncerPrefix);

        BlockingEvacuator evacuator = new BlockingEvacuator();
        cleanupLatches.add(evacuator.proceedLatch);

        VacuumWatchDog watchDog = new VacuumWatchDog(context, volume, 0.001f, 1, () -> evacuator);
        Thread.ofVirtual().start(watchDog);

        assertTrue(evacuator.enteredLatch.await(5, TimeUnit.SECONDS));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());
            List<VacuumSegmentMetadata> allMetadata = VacuumSegmentMetadataUtil.loadAll(tr, volumeSubspace);
            assertEquals(1, allMetadata.size());
        }

        evacuator.proceedLatch.countDown();
        watchDog.stop();
        assertTrue(watchDog.isStopped());
    }

    @Test
    void shouldStartAndStopThroughVolumeApi() throws Exception {
        // Behavior: vacuumStart/vacuumStop manages the full watchdog lifecycle
        fillAndSealSegmentWithGarbage(stashVolumeSyncerPrefix);

        BlockingEvacuator evacuator = new BlockingEvacuator();
        cleanupLatches.add(evacuator.proceedLatch);

        volume.vacuumStart(0.001f, () -> evacuator);

        assertTrue(evacuator.enteredLatch.await(5, TimeUnit.SECONDS));

        VacuumStatusResult statusBeforeStop = volume.vacuumStatus();
        assertTrue(statusBeforeStop.active());

        Thread stopThread = Thread.ofVirtual().start(() -> volume.vacuumStop());

        evacuator.proceedLatch.countDown();
        stopThread.join(10_000);

        VacuumStatusResult statusAfterStop = volume.vacuumStatus();
        assertFalse(statusAfterStop.active());
        assertNotNull(statusAfterStop.metadata());
        assertEquals(VacuumResult.STOPPED, statusAfterStop.metadata().result());
        assertTrue(statusAfterStop.metadata().segmentsProcessed() > 0);

        volume.vacuumDrop();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSubspace volumeSubspace = new VolumeSubspace(volume.getConfig().subspace());
            assertFalse(VacuumMetadataUtil.exists(tr, volumeSubspace));
        }
    }

    @Test
    void shouldRejectDoubleStart() throws Exception {
        // Behavior: vacuumStart throws when vacuum is already running
        fillAndSealSegmentWithGarbage(stashVolumeSyncerPrefix);

        BlockingEvacuator evacuator = new BlockingEvacuator();
        cleanupLatches.add(evacuator.proceedLatch);

        volume.vacuumStart(0.001f, () -> evacuator);

        assertTrue(evacuator.enteredLatch.await(5, TimeUnit.SECONDS));

        assertThrows(KronotopException.class, () -> volume.vacuumStart(0.001f, NoOpEvacuator::new));

        evacuator.proceedLatch.countDown();
        volume.vacuumStop();
    }

    @Test
    void shouldRejectStopWhenNotRunning() {
        // Behavior: vacuumStop throws when no vacuum is active
        assertThrows(KronotopException.class, () -> volume.vacuumStop());
    }

    @Test
    void shouldRejectDropWhenNoMetadataExists() {
        // Behavior: vacuumDrop throws when no vacuum metadata exists
        assertThrows(KronotopException.class, () -> volume.vacuumDrop());
    }

    @Test
    void shouldRejectStatusWhenNoMetadataExists() {
        // Behavior: vacuumStatus throws when no vacuum metadata exists
        assertThrows(KronotopException.class, () -> volume.vacuumStatus());
    }

    static class NoOpEvacuator implements EntryEvacuator {
        @Override
        public boolean evacuate(Context context, Transaction tr, EntryMetadata metadata, Versionstamp versionstamp) {
            return true;
        }
    }

    static class BlockingEvacuator implements EntryEvacuator {
        final CountDownLatch enteredLatch = new CountDownLatch(1);
        final CountDownLatch proceedLatch = new CountDownLatch(1);

        @Override
        public boolean evacuate(Context context, Transaction tr, EntryMetadata metadata, Versionstamp versionstamp) {
            enteredLatch.countDown();
            try {
                proceedLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return true;
        }
    }
}
