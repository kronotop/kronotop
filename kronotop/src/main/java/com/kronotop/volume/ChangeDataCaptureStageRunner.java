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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import static com.kronotop.volume.Prefixes.VOLUME_CDC_TRIGGER_PREFIX;

public class ChangeDataCaptureStageRunner extends ReplicationStageRunner implements StageRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeDataCaptureStageRunner.class);
    private final KeyWatcher keyWatcher = new KeyWatcher();

    public ChangeDataCaptureStageRunner(Context context, ReplicationConfig config) {
        super(context, config);
    }

    public String name() {
        return "ChangeDataCapture";
    }

    private void fetchChanges() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationJob replicationJob = ReplicationJob.load(tr, config);
            System.out.println(replicationJob.getLatestSegmentId());
            System.out.println(Arrays.toString(replicationJob.getLatestVersionstampedKey()));
        }
    }

    private void watchChanges() {
        while (!isStopped()) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
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
        if (isStopped()) {
            return;
        }

        LOGGER.info("ReplicationJob: {}, CDC stage has started", VersionstampUtils.base64Encode(config.jobId()));
        try {
            //replication.semaphore.acquire();
            startChangeDataCapture();
            watchChanges();
        } catch (Exception e) {
            LOGGER.error("ReplicationJob: {}, CDC stage has failed", VersionstampUtils.base64Encode(config.jobId()), e);
        } finally {
            //replication.semaphore.release();
        }
    }
}