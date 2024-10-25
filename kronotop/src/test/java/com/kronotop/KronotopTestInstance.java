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

package com.kronotop;

import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.cluster.MembershipService;
import com.kronotop.cluster.RouteLegacy;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.commandbuilder.redis.RedisCommandBuilder;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.instance.KronotopInstance;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.client.protocol.ClientMessage;
import com.kronotop.redis.handlers.connection.protocol.PingMessage;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.server.*;
import com.kronotop.server.resp3.*;
import com.typesafe.config.Config;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * KronotopTestInstance is a class that extends KronotopInstance and represents a standalone instance of
 * Kronotop for testing.
 */
public class KronotopTestInstance extends KronotopInstance {
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    private final Set<String> duplicatedCommands = new HashSet<>(List.of(
            ClientMessage.COMMAND,
            PingMessage.COMMAND
    ));
    private final Object clusterOperable = new Object();
    private final boolean runWithTCPServer;
    private EmbeddedChannel channel;

    public KronotopTestInstance(Config config) {
        this(config, false);
    }

    public KronotopTestInstance(Config config, boolean runWithTCPServer) {
        super(config);
        this.runWithTCPServer = runWithTCPServer;
    }

    private CommandHandlerRegistry mergeCommandHandlerRegistries() {
        CommandHandlerRegistry mergedRegistry = new CommandHandlerRegistry();
        for (ServerKind kind : ServerKind.values()) {
            CommandHandlerRegistry registry = super.context.getHandlers(kind);
            for (String command : registry.getCommands()) {
                Handler handler = registry.get(command);

                try {
                    mergedRegistry.handlerMethod(command, handler);
                } catch (CommandAlreadyRegisteredException e) {
                    if (!duplicatedCommands.contains(command)) {
                        throw e;
                    }
                }
            }
        }
        return mergedRegistry;
    }

    public EmbeddedChannel newChannel() {
        return new EmbeddedChannel(
                new RedisDecoder(false),
                new RedisBulkStringAggregator(),
                new RedisArrayAggregator(),
                new RedisMapAggregator(),
                new Router(super.context, mergeCommandHandlerRegistries())
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
        if (runWithTCPServer) {
            RESPServer server = new NioRESPServer(context, mergeCommandHandlerRegistries());
            context.registerService(server.getName(), server);
            server.start(member.getExternalAddress());
        }
        // TODO: CLUSTER-REFACTORING
        //CheckClusterStatus checkClusterStatus = new CheckClusterStatus();
        //executor.execute(checkClusterStatus);
        //synchronized (clusterOperable) {
        //    clusterOperable.wait();
        //}
        channel = newChannel();
        initializeTestCluster();

        String namespace = config.getString("default_namespace");
        NamespaceUtils.createOrOpen(context.getFoundationDB(), context.getClusterName(), namespace);
    }

    private void initializeTestCluster() {
        {
            RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
            boolean authRequired = config.hasPath("auth.users") || config.hasPath("auth.requirepass");
            if (authRequired) {
                ByteBuf buf = Unpooled.buffer();
                cmd.auth("devuser", "devpass").encode(buf);

                channel.writeInbound(buf);
                Object msg = channel.readOutbound();
                assertInstanceOf(SimpleStringRedisMessage.class, msg);
                SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
                assertEquals(Response.OK, actualMessage.content());
            }
        }

        {
            KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            cmd.initializeCluster().encode(buf);
            channel.writeInbound(buf);

            Object raw = channel.readOutbound();
            if (raw instanceof SimpleStringRedisMessage message) {
                assertEquals(Response.OK, message.content());
            } else if (raw instanceof ErrorRedisMessage message) {
                if (message.content().equals("ERR cluster has already been initialized")) {
                    // It's okay.
                    return;
                }
                fail(message.content());
            }

            MembershipService service = context.getService(MembershipService.NAME);
            await().atMost(5000, TimeUnit.MILLISECONDS).until(service::isClusterInitialized);
        }
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

    public void shutdownWithoutCleanup() {
        super.shutdown();
        executor.shutdownNow();
        channel.finishAndReleaseAll();
    }

    @Override
    public void shutdown() {
        try {
            shutdownWithoutCleanup();
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
        private final MembershipService membershipService;
        private final RedisService redisService;

        public CheckClusterStatus() {
            this.membershipService = context.getService(MembershipService.NAME);
            this.redisService = context.getService(RedisService.NAME);
        }

        @Override
        public void run() {
            // TODO: Cant see the exception traceback here. This may lead to critical and subtle issues.
            int numberOfShards = context.getConfig().getInt("redis.shards");
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                RouteLegacy route = membershipService.getRoutingTableLegacy().getRoute(shardId);
                if (route == null) {
                    executor.schedule(this, 20, TimeUnit.MILLISECONDS);
                    return;
                }
                if (!route.getMember().equals(context.getMember())) {
                    // Belong to another member
                    continue;
                }

                RedisShard shard = redisService.getServiceContext().shards().get(shardId);
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
