/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.volume.segment.SegmentAnalysis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

class VacuumWatchDog implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(VacuumWatchDog.class);

    private final Context context;
    private final Volume volume;
    private final float garbageThreshold;
    private final int maxWorkers;
    private final Supplier<EntryEvacuator> evacuatorFactory;
    private final List<VacuumWorker> activeWorkers = new ArrayList<>();
    private final LinkedBlockingQueue<VacuumWorker> completionQueue = new LinkedBlockingQueue<>();
    private final CountDownLatch runCompletionLatch = new CountDownLatch(1);
    private volatile boolean stopped;

    VacuumWatchDog(Context context, Volume volume, float garbageThreshold, int maxWorkers, Supplier<EntryEvacuator> evacuatorFactory) {
        this.context = context;
        this.volume = volume;
        this.garbageThreshold = garbageThreshold;
        this.maxWorkers = maxWorkers;
        this.evacuatorFactory = evacuatorFactory;
    }

    private Set<Long> getActiveSegmentIds() {
        Set<Long> ids = new HashSet<>();
        for (VacuumWorker worker : activeWorkers) {
            ids.add(worker.getSegmentId());
        }
        return ids;
    }

    private boolean startWorker(long segmentId) {
        EntryEvacuator evacuator = evacuatorFactory.get();
        VacuumWorker worker = new VacuumWorker(context, volume, segmentId, evacuator, completionQueue);
        synchronized (this) {
            if (stopped) {
                return false;
            }
            Thread.ofVirtual().name("kr.vacuum-worker-" + segmentId).start(worker);
            activeWorkers.add(worker);
        }
        return true;
    }

    private void awaitAnyCompletion() {
        while (!isStopped()) {
            try {
                VacuumWorker completed = completionQueue.poll(1, TimeUnit.SECONDS);
                if (completed != null) {
                    synchronized (this) {
                        activeWorkers.remove(completed);
                    }
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void run() {
        long startedAt = System.currentTimeMillis();
        int segmentsProcessed = 0;
        try {
            while (!isStopped()) {
                long writableSegmentId = volume.getWritableSegmentId();
                List<SegmentAnalysis> analyses = volume.analyze();
                Set<Long> activeSegmentIds = getActiveSegmentIds();

                for (SegmentAnalysis analysis : analyses) {
                    if (isStopped()) {
                        break;
                    }
                    if (analysis.segmentId() == writableSegmentId) {
                        continue;
                    }
                    if (activeSegmentIds.contains(analysis.segmentId())) {
                        continue;
                    }
                    if (analysis.garbagePercentage() < garbageThreshold) {
                        continue;
                    }
                    if (activeWorkers.size() >= maxWorkers) {
                        break;
                    }

                    LOGGER.info("Segment {} has {}% garbage, exceeds threshold {}%",
                            analysis.segmentId(), analysis.garbagePercentage(), garbageThreshold);
                    if (!startWorker(analysis.segmentId())) {
                        break;
                    }
                    segmentsProcessed++;
                }

                if (activeWorkers.isEmpty()) {
                    LOGGER.info("No segments exceed garbage threshold {}%, vacuum complete", garbageThreshold);
                    break;
                }

                awaitAnyCompletion();
            }
        } finally {
            boolean externallyStopped = stopped;
            stopped = true;
            try {
                saveVacuumStats(startedAt, segmentsProcessed, externallyStopped);
            } catch (Exception e) {
                LOGGER.error("Failed to save vacuum stats for volume {}", volume, e);
            }
            runCompletionLatch.countDown();
        }
    }

    private void saveVacuumStats(long startedAt, int segmentsProcessed, boolean externallyStopped) {
        VacuumResult result;
        if (segmentsProcessed == 0) {
            result = VacuumResult.NO_WORK;
        } else if (externallyStopped) {
            result = VacuumResult.STOPPED;
        } else {
            result = VacuumResult.COMPLETED;
        }
        long completedAt = System.currentTimeMillis();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumMetadataUtil.save(tr, volume.getSubspace(), startedAt, completedAt, result, segmentsProcessed);
            tr.commit().join();
        }
    }

    private void stopAll(List<VacuumWorker> workers) {
        for (VacuumWorker worker : workers) {
            worker.stop();
        }
        for (VacuumWorker worker : workers) {
            try {
                worker.waitUntilComplete();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void stop() {
        List<VacuumWorker> workers;
        synchronized (this) {
            stopped = true;
            workers = new ArrayList<>(activeWorkers);
        }
        stopAll(workers);
        try {
            runCompletionLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    boolean isStopped() {
        return stopped;
    }

    static class VacuumWorker implements Runnable {
        private static final Logger WORKER_LOGGER = LoggerFactory.getLogger(VacuumWorker.class);

        private final Context context;
        private final Volume volume;
        private final long segmentId;
        private final EntryEvacuator evacuator;
        private final LinkedBlockingQueue<VacuumWorker> completionQueue;
        private final VacuumContext vacuumContext = new VacuumContext();
        private final CountDownLatch completionLatch = new CountDownLatch(1);

        VacuumWorker(Context context, Volume volume, long segmentId, EntryEvacuator evacuator, LinkedBlockingQueue<VacuumWorker> completionQueue) {
            this.context = context;
            this.volume = volume;
            this.segmentId = segmentId;
            this.evacuator = evacuator;
            this.completionQueue = completionQueue;
        }

        @Override
        public void run() {
            try {
                if (vacuumContext.isStopped()) {
                    return;
                }
                volume.vacuum(vacuumContext, segmentId, evacuator);
            } catch (IOException e) {
                WORKER_LOGGER.error("Vacuum failed for segment {}", segmentId, e);
            } finally {
                try {
                    markStopped();
                } catch (Exception e) {
                    WORKER_LOGGER.error("Failed to mark segment {} as stopped", segmentId, e);
                }
                completionLatch.countDown();
                completionQueue.add(this);
            }
        }

        private void markStopped() {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VacuumMetadataStatus status = VacuumSegmentMetadataUtil.readStatus(tr, volume.getSubspace(), segmentId);
                if (status != VacuumMetadataStatus.COMPLETED) {
                    VacuumSegmentMetadataUtil.setStatus(tr, volume.getSubspace(), segmentId, VacuumMetadataStatus.STOPPED);
                    tr.commit().join();
                }
            } catch (IllegalStateException e) {
                // No metadata found — segment was never analyzed, nothing to mark
            }
        }

        void stop() {
            vacuumContext.stop();
        }

        void waitUntilComplete() throws InterruptedException {
            completionLatch.await();
        }

        long getSegmentId() {
            return segmentId;
        }
    }
}
