/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume.segrep;

import com.kronotop.Context;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.cluster.sharding.ShardKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ReplicationWatchDog implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationWatchDog.class);

    private final Context context;
    private final ExecutorService executor;
    private final ShardKind shardKind;
    private final int shardId;
    private final Path destination;
    private StatefulInternalConnection<byte[], byte[]> connection;
    private volatile boolean shutdown;

    public ReplicationWatchDog(Context context, ShardKind shardKind, int shardId, String destination) {
        this.context = context;
        this.shardKind = shardKind;
        this.shardId = shardId;
        this.executor = Executors.newSingleThreadExecutor();

        try {
            this.destination = Files.createDirectories(Path.of(destination));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void run() {

    }

    public void shutdown() {
        shutdown = true;
        executor.shutdownNow();

        try {
            if (!executor.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.debug("Replication executor did not terminate within timeout");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
