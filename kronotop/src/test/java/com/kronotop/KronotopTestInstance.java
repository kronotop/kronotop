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

package com.kronotop;

import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.cluster.MembershipService;
import com.kronotop.core.cluster.coordinator.Route;
import com.kronotop.instance.KronotopInstance;
import com.kronotop.redis.storage.Shard;
import com.kronotop.server.resp.Router;
import com.kronotop.server.resp3.RedisArrayAggregator;
import com.kronotop.server.resp3.RedisBulkStringAggregator;
import com.kronotop.server.resp3.RedisDecoder;
import com.kronotop.server.resp3.RedisMapAggregator;
import com.typesafe.config.Config;
import io.netty.channel.embedded.EmbeddedChannel;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * KronotopTestInstance is a class that extends KronotopInstance and represents a standalone instance of
 * Kronotop for testing.
 */
public class KronotopTestInstance extends KronotopInstance {
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    private final Object clusterOperable = new Object();
    private EmbeddedChannel channel;

    public KronotopTestInstance(Config config) {
        super(config);
    }

    public EmbeddedChannel newChannel() {
        return new EmbeddedChannel(
                new RedisDecoder(false),
                new RedisBulkStringAggregator(),
                new RedisArrayAggregator(),
                new RedisMapAggregator(),
                new Router(super.context, super.handlers)
        );
    }

    public EmbeddedChannel getChannel() {
        return channel;
    }

    /**
     * Starts the Kronotop instance for testing.
     *
     * <p>
     * This method performs the following steps:
     * <p>
     * 1. Calls the start method of the super class.
     * 2. Creates a new CheckClusterStatus object and adds it to the executor.
     * 3. Waits until the clusterOperable object is notified.
     * 4. Creates a new channel using the newChannel method.
     * </p>
     *
     * @throws UnknownHostException if the host address is unknown
     * @throws InterruptedException if the thread is interrupted
     */
    @Override
    public void start() throws UnknownHostException, InterruptedException {
        super.start();
        CheckClusterStatus checkClusterStatus = new CheckClusterStatus();
        executor.execute(checkClusterStatus);
        synchronized (clusterOperable) {
            clusterOperable.wait();
        }
        channel = newChannel();
    }

    /**
     * Cleans up the test cluster by removing the corresponding directory in the FoundationDB database.
     */
    private void cleanupTestCluster() {
        context.getFoundationDB().run(tr -> {
            List<String> subpath = DirectoryLayout.Builder.clusterName(context.getClusterName()).asList();
            return DirectoryLayer.getDefault().removeIfExists(tr, subpath).join();
        });
    }

    @Override
    public void shutdown() {
        try {
            super.shutdown();
            executor.shutdownNow();
            channel.finishAndReleaseAll();
        } finally {
            cleanupTestCluster();
        }
    }

    /**
     * CheckClusterStatus is a private class that implements the Runnable interface.
     * It is used to check the status of the cluster by iterating through each shard and performing necessary checks.
     * If the shard is not operable, read-only, or if the shard or route is null, the execution is scheduled to retry after 20 milliseconds.
     * Once all the shards have been checked and no issues are found, it notifies the clusterOperable object.
     */
    private class CheckClusterStatus implements Runnable {
        @Override
        public void run() {
            int numberOfShards = context.getConfig().getInt("cluster.number_of_shards");
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                MembershipService membershipService = context.getService(MembershipService.NAME);
                Route route = membershipService.getRoutingTable().getRoute(shardId);
                if (route == null) {
                    executor.schedule(this, 20, TimeUnit.MILLISECONDS);
                    return;
                }
                if (!route.getMember().equals(context.getMember())) {
                    // Belong to another member
                    continue;
                }
                Shard shard = context.getLogicalDatabase().getShards().get(shardId);
                if (shard == null) {
                    executor.schedule(this, 20, TimeUnit.MILLISECONDS);
                    return;
                }
                if (!shard.isOperable() || shard.isReadOnly()) {
                    executor.schedule(this, 20, TimeUnit.MILLISECONDS);
                    return;
                }
            }
            // This instance is now operable.
            synchronized (clusterOperable) {
                clusterOperable.notifyAll();
            }
        }
    }
}
