/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RouteKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.commandbuilder.redis.RedisCommandBuilder;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.instance.KronotopInstance;
import com.kronotop.network.Address;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.client.protocol.ClientMessage;
import com.kronotop.redis.handlers.cluster.protocol.ClusterMessage;
import com.kronotop.redis.handlers.connection.protocol.HelloMessage;
import com.kronotop.redis.handlers.connection.protocol.PingMessage;
import com.kronotop.redis.handlers.protocol.InfoMessage;
import com.kronotop.redis.server.protocol.CommandMessage;
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
            PingMessage.COMMAND,
            CommandMessage.COMMAND,
            ClusterMessage.COMMAND,
            InfoMessage.COMMAND,
            HelloMessage.COMMAND
    ));
    private final boolean initialize; // The default is true. It's useful for integration tests that test some failure conditions.
    private final boolean runWithTCPServer; // the default is false
    private EmbeddedChannel channel;

    public KronotopTestInstance(Config config) {
        this(config, false, true);
    }

    public KronotopTestInstance(Config config, boolean runWithTCPServer) {
        this(config, runWithTCPServer, true);
    }

    public KronotopTestInstance(Config config, boolean runWithTCPServer, boolean initialize) {
        super(config);
        this.runWithTCPServer = runWithTCPServer;
        this.initialize = initialize;
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

    private void startNioRESPServer(String name, Address address) throws InterruptedException {
        RESPServer server = new NioRESPServer(context, mergeCommandHandlerRegistries());
        context.registerService(name, server);
        server.start(address);
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
            startNioRESPServer("IntegrationTestInternal-TCPServer", member.getInternalAddress());
            startNioRESPServer("IntegrationTestExternal-TCPServer", member.getExternalAddress());
        }

        channel = newChannel();

        if (initialize) {
            initialize();
        }

        String namespace = config.getString("default_namespace");
        NamespaceUtils.createOrOpen(context, namespace);
    }

    private void authenticateIfRequired() {
        // Authenticate the connection - channel
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

    private boolean initializeCluster(KrAdminCommandBuilder<String, String> cmd) {
        ByteBuf buf = Unpooled.buffer();
        cmd.initializeCluster().encode(buf);
        channel.writeInbound(buf);

        Object raw = channel.readOutbound();
        if (raw instanceof SimpleStringRedisMessage message) {
            assertEquals(Response.OK, message.content());
        } else if (raw instanceof ErrorRedisMessage message) {
            if (message.content().equals("ERR cluster has already been initialized")) {
                // It's okay, already initialized by the first call of addNewInstance method.
                return true;
            } else {
                fail(message.content());
            }
        }
        return false;
    }

    private void setPrimaryOwnersOfShards(KrAdminCommandBuilder<String, String> cmd) {
        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", RouteKind.PRIMARY.name(), ShardKind.REDIS.name(), context.getMember().getId()).encode(buf);
        channel.writeInbound(buf);

        Object raw = channel.readOutbound();
        if (raw instanceof SimpleStringRedisMessage message) {
            assertEquals(Response.OK, message.content());
        } else if (raw instanceof ErrorRedisMessage message) {
            fail(message.content());
        }
    }

    private void setShardsReadWrite(KrAdminCommandBuilder<String, String> cmd) {
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus("REDIS", "READWRITE").encode(buf);
        channel.writeInbound(buf);

        Object raw = channel.readOutbound();
        if (raw instanceof SimpleStringRedisMessage message) {
            assertEquals(Response.OK, message.content());
        } else if (raw instanceof ErrorRedisMessage message) {
            fail(message.content());
        }
    }

    private void initialize() {
        authenticateIfRequired();

        KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        boolean clusterAlreadyInitialized = initializeCluster(cmd);

        if (clusterAlreadyInitialized) {
            // The cluster has already been initialized, no need to run the rest of the procedure.
            return;
        }

        RoutingService routing = context.getService(RoutingService.NAME);
        await().atMost(5000, TimeUnit.MILLISECONDS).until(routing::isClusterInitialized);

        setPrimaryOwnersOfShards(cmd);
        await().atMost(5, TimeUnit.SECONDS).until(this::areAllOwnedRedisShardsOperable);

        setShardsReadWrite(cmd);
        await().atMost(5, TimeUnit.SECONDS).until(this::areAllRedisShardsWritable);
    }

    private boolean areAllRedisShardsWritable() {
        RoutingService routing = context.getService(RoutingService.NAME);
        int shards = context.getConfig().getInt("redis.shards");
        for (int shardId = 0; shardId < shards; shardId++) {
            Route route = routing.findRoute(ShardKind.REDIS, shardId);
            if (route == null) {
                return false;
            }
            if (!route.shardStatus().equals(ShardStatus.READWRITE)) {
                return false;
            }
        }
        return true;
    }

    private boolean areAllOwnedRedisShardsOperable() {
        RoutingService routing = context.getService(RoutingService.NAME);
        RedisService redis = context.getService(RedisService.NAME);
        int shards = context.getConfig().getInt("redis.shards");
        for (int shardId = 0; shardId < shards; shardId++) {
            Route route = routing.findRoute(ShardKind.REDIS, shardId);
            if (route == null) {
                return false;
            }
            if (!route.primary().equals(context.getMember())) {
                // Not belong to this member
                continue;
            }
            RedisShard shard = redis.getServiceContext().shards().get(shardId);
            if (shard == null) {
                return false;
            }
            if (!shard.operable()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Cleans up the test cluster by removing the corresponding directory in the FoundationDB database.
     */
    public void cleanupTestCluster() {
        context.getFoundationDB().run(tr -> {
            List<String> subpath = KronotopDirectory.kronotop().cluster(context.getClusterName()).toList();
            return DirectoryLayer.getDefault().removeIfExists(tr, subpath).join();
        });
    }

    public void shutdownWithoutCleanup() {
        super.shutdown();
        executor.shutdown();
        try {
            executor.awaitTermination(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
}
