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

import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.volume.VolumeConfigGenerator;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
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
    private final String volumeName;
    private final DirectorySubspace subspace;

    private final Retry transactionWithRetry;
    private StatefulInternalConnection<byte[], byte[]> connection;
    private volatile boolean shutdown;

    public ReplicationWatchDog(Context context, ShardKind shardKind, int shardId, String destination) {
        this.context = context;
        this.shardKind = shardKind;
        this.shardId = shardId;
        this.executor = Executors.newSingleThreadExecutor();
        this.transactionWithRetry = TransactionUtils.transactionWithRetryConfig(10, Duration.ofMillis(100));

        try {
            this.destination = Files.createDirectories(Path.of(destination));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        this.volumeName = VolumeConfigGenerator.volumeName(shardKind, shardId);
        this.subspace = openStandbySubspace();
    }

    private DirectorySubspace openStandbySubspace() {
        KronotopDirectoryNode node = KronotopDirectory.kronotop().
                cluster(context.getClusterName()).
                metadata().
                volumes().
                bucket().
                volume(volumeName).
                standby(context.getMember().getId());

        return transactionWithRetry.executeSupplier(() -> TransactionUtils.executeThenCommit(context,
                tr -> DirectoryLayer.getDefault().createOrOpen(tr, node.toList()).join()
        ));
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
