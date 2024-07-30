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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.KeyWatcher;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.journal.JournalName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import static com.kronotop.volume.Prefixes.VOLUME_CDC_TRIGGER_PREFIX;

public class ChangeDataCaptureStageRunner extends ReplicationStageRunner implements StageRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeDataCaptureStageRunner.class);
    private final KeyWatcher keyWatcher = new KeyWatcher();

    public ChangeDataCaptureStageRunner(Context context, ReplicationConfig config, StatefulInternalConnection<byte[], byte[]> connection) {
        super(context, config, connection);
    }

    public String name() {
        return "ChangeDataCapture";
    }

    private IterationResult iterateSegmentLogEntries(Transaction tr, long segmentId, Versionstamp key) throws IOException, NotEnoughSpaceException {
        Segment segment = openSegments.get(segmentId);
        if (segment == null) {
            SegmentConfig segmentConfig = new SegmentConfig(segmentId, config.rootPath(), config.segmentSize());
            segment = new Segment(segmentConfig);
            openSegments.put(segmentId, segment);
        }

        // (begin, ...)
        VersionstampedKeySelector begin = null;
        if (key != null) {
            begin = VersionstampedKeySelector.firstGreaterThan(key);
            // There is no difference between firstGreaterThan and firstGreaterOrEqual. firstGreaterThan still returns the
            // begin-key. I don't understand why but calling add(1) fixes the problem.
            begin = begin.add(1);
        }

        return iterate(tr, segment, begin, null, MAXIMUM_BATCH_SIZE);
    }

    private void fetchChanges() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationJob.compute(tr, config, (job) -> {
                Versionstamp key = null;
                if (job.getLatestVersionstampedKey() != null) {
                    key = Versionstamp.fromBytes(job.getLatestVersionstampedKey());
                }

                IterationResult iterationResult;
                try {
                    iterationResult = iterateSegmentLogEntries(tr, job.getLatestSegmentId(), key);
                } catch (IOException | NotEnoughSpaceException e) {
                    throw new RuntimeException(e);
                }
                if (iterationResult.processedKeys() == 0) {
                    // TODO: Find a new segment id, if there is any.
                    return;
                }
                job.setLatestVersionstampedKey(iterationResult.latestKey().getBytes());
            });

            tr.commit().join();
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
                LOGGER.error("Error while watching segment log: {}", config.volumeName(), e);
            }
        }
    }

    private void discoverStartingPoint() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationJob.compute(tr, config, (job) -> {
                if (!job.getSnapshots().isEmpty()) {
                    Map.Entry<Long, Snapshot> entry = job.getSnapshots().lastEntry();
                    Snapshot snapshot = entry.getValue();
                    job.setLatestSegmentId(snapshot.getSegmentId());
                    job.setLatestVersionstampedKey(snapshot.getEnd());
                }
            });
            tr.commit().join();
        }
    }

    @Override
    public void stop() {
        keyWatcher.unwatch(config.subspace().pack(Tuple.from(VOLUME_CDC_TRIGGER_PREFIX)));
        super.stop();
    }

    @Override
    public void run() {
        if (isStopped()) {
            return;
        }

        try {
            discoverStartingPoint();
            watchChanges();
        } catch (Exception e) {
            LOGGER.atError().setMessage("{} stage has failed, jobId = {}").
                    addArgument(name()).
                    addArgument(config.stringifyJobId()).
                    setCause(e).
                    log();
        }
    }
}