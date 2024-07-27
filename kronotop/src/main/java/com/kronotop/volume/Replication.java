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
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.KeyWatcher;
import com.kronotop.VersionstampUtils;
import com.kronotop.journal.JournalName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import static com.kronotop.volume.Prefixes.VOLUME_CDC_TRIGGER_PREFIX;

public class Replication {
    private static final Logger LOGGER = LoggerFactory.getLogger(Replication.class);
    private final Context context;
    private final ReplicationConfig config;
    private final ReplicationContext replicationContext;
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private volatile boolean started = false;
    private volatile boolean stopped = false;

    public Replication(Context context, ReplicationConfig config) {
        this.context = context;
        this.config = config;
        this.replicationContext = new ReplicationContext();
    }

    public void start() throws IOException {
        if (started) {
            throw new IllegalStateException("Replication is already started");
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            if (config.cdcOnly()) {
                replicationContext.changeDataCaptureFuture().set(replicationContext.executor().submit(new ChangeDataCaptureStageRunner(this)));
            } else {
                if (ReplicationJob.load(tr, config).isSnapshotCompleted()) {
                    replicationContext.changeDataCaptureFuture().set(replicationContext.executor().submit(new ChangeDataCaptureStageRunner(this)));
                } else {
                    replicationContext.snapshotFuture().set(replicationContext.executor().submit(new SnapshotStageRunnable(context, config)));
                }
            }
        }

        started = true;
        LOGGER.info("Replication job: {} started", VersionstampUtils.base64Encode(config.jobId()));
    }

    public ReplicationContext getContext() {
        return replicationContext;
    }

    public void stop() throws IOException {
        if (!started) {
            throw new IllegalStateException("Replication is not started");
        }

        // Unwatch the CDC trigger, if it exists.
        keyWatcher.unwatch(config.subspace().pack(Tuple.from(VOLUME_CDC_TRIGGER_PREFIX)));

        stopped = true;
        try {
            replicationContext.executor().close();
        } finally {
            started = false;
        }
        LOGGER.info("Replication job: {} stopped", VersionstampUtils.base64Encode(config.jobId()));
    }

    private static class ChangeDataCaptureStageRunner implements Runnable {
        private static final Logger LOGGER = LoggerFactory.getLogger(ChangeDataCaptureStageRunner.class);
        private final Replication replication;
        private final Context context;
        private final ReplicationConfig config;
        private final KeyWatcher keyWatcher = new KeyWatcher();

        public ChangeDataCaptureStageRunner(Replication replication) {
            this.replication = replication;
            this.context = replication.context;
            this.config = replication.config;
        }

        private void fetchChanges() {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReplicationJob replicationJob = ReplicationJob.load(tr, config);
                System.out.println(replicationJob.getLatestSegmentId());
                System.out.println(Arrays.toString(replicationJob.getLatestVersionstampedKey()));
            }
        }

        private void watchChanges() {
            while (!replication.stopped) {
                try (Transaction tr = replication.context.getFoundationDB().createTransaction()) {
                    CompletableFuture<Void> watcher = keyWatcher.watch(tr, config.subspace().pack(Tuple.from(VOLUME_CDC_TRIGGER_PREFIX)));
                    tr.commit().join();

                    try {
                        // fetch events here
                        LOGGER.info("Fetching the latest segment logs before watching");
                        fetchChanges();
                        watcher.join();
                    } catch (CancellationException e) {
                        LOGGER.info("CDC watcher has been cancelled on Volume: {}", config.volumeName());
                        return;
                    }
                    // fetch events here
                    LOGGER.info("Triggered, fetching the latest segment logs");
                    fetchChanges();
                } catch (Exception e) {
                    LOGGER.error("Error while watching segment log: {}", JournalName.clusterEvents(), e);
                }
            }
        }

        private ReplicationJob startChangeDataCapture() {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReplicationJob replicationJob = ReplicationJob.compute(tr, config, (job) -> {
                    if (!job.getSnapshots().isEmpty()) {
                        Map.Entry<Long, Snapshot> entry = job.getSnapshots().lastEntry();
                        Snapshot snapshot = entry.getValue();
                        job.setLatestSegmentId(snapshot.getSegmentId());
                        job.setLatestVersionstampedKey(snapshot.getEnd());
                    }
                });
                tr.commit().join();
                return replicationJob;
            }
        }

        @Override
        public void run() {
            if (replication.stopped) {
                return;
            }

            LOGGER.info("ReplicationJob: {}, CDC stage has started", VersionstampUtils.base64Encode(replication.config.jobId()));
            try {
                //replication.semaphore.acquire();
                startChangeDataCapture();
                watchChanges();
            } catch (Exception e) {
                LOGGER.error("ReplicationJob: {}, CDC stage has failed", VersionstampUtils.base64Encode(replication.config.jobId()), e);
            } finally {
                //replication.semaphore.release();
            }
        }
    }
}
