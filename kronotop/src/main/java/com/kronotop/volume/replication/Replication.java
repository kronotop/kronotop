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

package com.kronotop.volume.replication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.client.InternalClient;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.volume.VolumeConfig;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.ByteArrayCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class Replication {
    private static final Logger LOGGER = LoggerFactory.getLogger(Replication.class);
    protected final StatefulInternalConnection<byte[], byte[]> connection;
    private final Context context;
    private final Versionstamp slotId;
    private final VolumeConfig volumeConfig;
    private final ReplicationConfig config;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicReference<StageRunner> activeStageRunner = new AtomicReference<>();
    private final RedisClient client;
    private volatile boolean started = false;
    private volatile boolean stopped = false;

    public Replication(Context context, Versionstamp slotId, ReplicationConfig config) {
        this.context = context;
        this.config = config;
        this.slotId = slotId;
        this.volumeConfig = config.volumeConfig();

        RoutingService routing = context.getService(RoutingService.NAME);
        Route route = routing.findRoute(config.shardKind(), config.shardId());
        if (route == null) {
            throw new IllegalArgumentException("Route not found: " + config.shardKind() + " " + config.shardId());
        }

        Member member = route.primary();
        this.client = RedisClient.create(
                String.format("redis://%s:%d", member.getInternalAddress().getHost(), member.getInternalAddress().getPort())
        );
        this.connection = InternalClient.connect(client, ByteArrayCodec.INSTANCE);
    }

    private void runStages(List<StageRunner> stageRunners) {
        for (StageRunner stageRunner : stageRunners) {
            if (stopped) {
                break;
            }
            activeStageRunner.set(stageRunner);
            try {
                stageRunner.run();
            } finally {
                stageRunner.stop();
            }
        }
    }

    public synchronized Future<?> start() throws IOException {
        if (started) {
            throw new IllegalStateException("Replication is already started");
        }

        stopped = false;
        started = true;

        ReplicationContext replicationContext = new ReplicationContext(slotId, config, volumeConfig, connection);
        return executor.submit(() -> {
            List<StageRunner> runners = new ArrayList<>();

            if (config.initialStage().equals(ReplicationStage.STREAMING)) {
                StageRunner changeDataCaptureStageRunner = new StreamingStageRunner(context, replicationContext);
                runners.add(changeDataCaptureStageRunner);
            } else {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    if (ReplicationSlot.load(tr, config, slotId).isSnapshotCompleted()) {
                        StageRunner changeDataCaptureStageRunner = new StreamingStageRunner(context, replicationContext);
                        runners.add(changeDataCaptureStageRunner);
                    } else {
                        StageRunner snapshotStageRunner = new SnapshotStageRunner(context, replicationContext);
                        runners.add(snapshotStageRunner);

                        StageRunner changeDataCaptureStageRunner = new StreamingStageRunner(context, replicationContext);
                        runners.add(changeDataCaptureStageRunner);
                    }
                }
            }

            runStages(runners);
        });
    }

    public StageRunner getActiveStageRunner() {
        return activeStageRunner.get();
    }

    public synchronized void stop() {
        if (!started) {
            throw new IllegalStateException("Replication is not started");
        }

        if (stopped) {
            throw new IllegalStateException("Replication is already stopped");
        }

        stopped = true;

        StageRunner stageRunner = activeStageRunner.get();
        if (stageRunner != null) {
            stageRunner.stop();
        }

        executor.shutdown();
        client.shutdown();

        LOGGER.atDebug().setMessage("Replication has stopped, slotId = {}")
                .addArgument(ReplicationMetadata.stringifySlotId(slotId))
                .log();
    }
}
