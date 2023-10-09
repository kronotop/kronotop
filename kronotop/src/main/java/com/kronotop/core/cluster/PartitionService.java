/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.core.cluster;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.common.resp.RESPError;
import com.kronotop.core.Context;
import com.kronotop.core.KronotopService;
import com.kronotop.redis.storage.Partition;
import com.kronotop.redis.storage.impl.OnHeapPartitionImpl;
import com.kronotop.redis.storage.persistence.DataStructure;
import com.kronotop.redis.storage.persistence.PartitionLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PartitionService implements KronotopService {
    public static final String NAME = "Partition";
    private static final Logger logger = LoggerFactory.getLogger(PartitionService.class);
    private final Context context;
    private final ClusterService clusterService;
    private final ExecutorService executor;
    private volatile boolean isShutdown;

    public PartitionService(Context context) {
        this.context = context;
        this.clusterService = context.getService(ClusterService.NAME);
        this.executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("kr.partition-migration-%d").build()
        );
    }

    @Override
    public String getName() {
        return NAME;
    }

    public void start() {
        executor.submit(new PartitionQueueListener());
        logger.info("Partition Service has been started");
    }

    @Override
    public Context getContext() {
        return context;
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(6, TimeUnit.SECONDS)) {
                logger.warn(String.format("%s cannot be stopped gracefully", NAME));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private class PartitionQueueListener implements Runnable {

        @Override
        public void run() {
            while (!isShutdown) {
                try {
                    PartitionEvent partitionEvent = clusterService.getPartitionEventQueue().take();
                    Partition partition = context.getLogicalDatabase().getPartitions().computeIfAbsent(
                            partitionEvent.getPartitionId(), (k) -> new OnHeapPartitionImpl(partitionEvent.getPartitionId())
                    );

                    if (partition.getPartitionMetadata().isWritable()) {
                        // Already initialized
                        continue;
                    }

                    while (true) {
                        try (Transaction tr = context.getFoundationDB().createTransaction()) {
                            PartitionLoader partitionLoader = new PartitionLoader(context, partition);
                            for (DataStructure dataStructure : DataStructure.values()) {
                                partitionLoader.load(tr, dataStructure);
                            }
                            break;
                        } catch (CompletionException e) {
                            if (e.getCause() instanceof FDBException) {
                                String message = RESPError.decapitalize(e.getCause().getMessage());
                                if (message.equalsIgnoreCase(RESPError.TRANSACTION_TOO_OLD_MESSAGE)) {
                                    continue;
                                }
                            }
                            throw e;
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}
