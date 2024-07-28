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
import com.kronotop.Context;
import com.kronotop.VersionstampUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class Replication {
    private static final Logger LOGGER = LoggerFactory.getLogger(Replication.class);
    private final Context context;
    private final ReplicationConfig config;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicReference<StageRunner> activeStageRunner = new AtomicReference<>();
    private volatile boolean started = false;
    private volatile boolean stopped = false;

    public Replication(Context context, ReplicationConfig config) {
        this.context = context;
        this.config = config;
    }

    private void runStages(List<StageRunner> stageRunners) {
        for (StageRunner stageRunner : stageRunners) {
            if (stopped) {
                return;
            }
            LOGGER.atInfo().
                    setMessage("{} state is about to be started, jobId = {}").
                    addArgument(stageRunner.name()).
                    addArgument(config.stringifyJobId()).
                    log();
            activeStageRunner.set(stageRunner);
            try {
                stageRunner.run();
            } finally {
                stageRunner.stop();
            }
        }
    }

    public void start() throws IOException {
        if (started) {
            throw new IllegalStateException("Replication is already started");
        }

        started = true;

        executor.submit(() -> {
            List<StageRunner> runners = new ArrayList<>();

            if (config.cdcOnly()) {
                StageRunner changeDataCaptureStageRunner = new ChangeDataCaptureStageRunner(context, config);
                runners.add(changeDataCaptureStageRunner);
            } else {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    if (ReplicationJob.load(tr, config).isSnapshotCompleted()) {
                        StageRunner changeDataCaptureStageRunner = new ChangeDataCaptureStageRunner(context, config);
                        runners.add(changeDataCaptureStageRunner);
                    } else {
                        StageRunner snapshotStageRunner = new SnapshotStageRunner(context, config);
                        runners.add(snapshotStageRunner);

                        StageRunner changeDataCaptureStageRunner = new ChangeDataCaptureStageRunner(context, config);
                        runners.add(changeDataCaptureStageRunner);
                    }
                }
            }

            runStages(runners);
        });

        LOGGER.info("Replication job: {} started", VersionstampUtils.base64Encode(config.jobId()));
    }

    public void stop() {
        if (!started) {
            throw new IllegalStateException("Replication is not started");
        }

        stopped = true;

        StageRunner stageRunner = activeStageRunner.get();
        if (stageRunner != null) {
            LOGGER.info("Stopping stage {}", VersionstampUtils.base64Encode(config.jobId()));
            stageRunner.stop();
        }

        executor.shutdown();

        LOGGER.info("Replication job: {} stopped", VersionstampUtils.base64Encode(config.jobId()));
    }
}
