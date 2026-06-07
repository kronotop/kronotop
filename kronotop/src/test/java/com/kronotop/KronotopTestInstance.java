/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.kronotop.bucket.BucketService;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RouteKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.Shard;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.commands.KrAdminCommandBuilder;
import com.kronotop.commands.redis.RedisCommandBuilder;
import com.kronotop.core.handlers.client.protocol.ClientMessage;
import com.kronotop.core.handlers.connection.protocol.EchoMessage;
import com.kronotop.core.handlers.connection.protocol.HelloMessage;
import com.kronotop.core.handlers.connection.protocol.PingMessage;
import com.kronotop.core.handlers.protocol.InfoMessage;
import com.kronotop.core.handlers.server.protocol.CommandMessage;
import com.kronotop.core.handlers.session.protocol.SessionAttributeMessage;
import com.kronotop.core.handlers.session.protocol.SessionCloseMessage;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.instance.KronotopInstance;
import com.kronotop.namespace.NamespaceAlreadyExistsException;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.network.Address;
import com.kronotop.server.*;
import com.kronotop.server.resp3.*;
import com.kronotop.stash.StashService;
import com.kronotop.stash.handlers.cluster.protocol.ClusterMessage;
import com.typesafe.config.Config;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.Attribute;

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
            HelloMessage.COMMAND,
            EchoMessage.COMMAND,
            SessionAttributeMessage.COMMAND,
            SessionCloseMessage.COMMAND
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
                HandlerEntry entry = registry.get(command);

                try {
                    mergedRegistry.register(
                            entry.commandType(),
                            entry.handler(),
                            entry.minimumParameterCount(),
                            entry.maximumParameterCount()
                    );
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
                new KronotopChannelDuplexHandler(super.context, mergeCommandHandlerRegistries(), ServerKind.EXTERNAL)
        );
    }

    public EmbeddedChannel getChannel() {
        return channel;
    }

    private void startNioRESPServer(String name, Address address) throws InterruptedException {
        NettyConfig nettyConfig = NettyConfig.fromConfig(config, ServerKind.INTERNAL);
        RESPServer server = new NioRESPServer(context, mergeCommandHandlerRegistries(), TLSConfig.disabled(), nettyConfig, ServerKind.INTERNAL);
        context.registerService(name, server);
        server.start(address);
    }

    /**
     * Starts the Kronotop test instance.
     * <p>
     * This method extends the behavior of the superclass's start method by adding specific startup
     * tasks needed for the test instance. It performs the following operations:
     * <p>
     * 1. Invokes the superclass's start method to initialize core functionalities.
     * 2. If the test instance is configured to run with a TCP server, it starts two Nio RESP servers:
     * - One for the internal address of the member.
     * - One for the external address of the member.
     * 3. Creates a new communication channel using the {@link #newChannel()} method.
     * 4. If initialization is required, it invokes the {@link #initialize()} method to initialize the
     * cluster-related state and requirements.
     * 5. Creates or opens a default namespace based on the provided configuration.
     *
     * @throws UnknownHostException if an unknown host address issue occurs during startup.
     * @throws InterruptedException if the thread is interrupted during the server startup or other processes.
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
        try {
            NamespaceUtil.create(context, namespace);
        } catch (NamespaceAlreadyExistsException ignore) {
        }
    }

    private void authenticateIfRequired() {
        // Authenticate the connection - channel
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        boolean authRequired = config.hasPath("auth.users") || config.hasPath("auth.requirepass");
        if (authRequired) {
            ByteBuf buf = Unpooled.buffer();
            cmd.auth("devuser", "devpass").encode(buf);

            Object msg = BaseTest.runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    private boolean initializeCluster(KrAdminCommandBuilder<String, String> cmd) {
        ByteBuf buf = Unpooled.buffer();
        cmd.initializeCluster().encode(buf);

        Object raw = BaseTest.runCommand(channel, buf);
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

    private void setPrimaryOwnersOfShards(KrAdminCommandBuilder<String, String> cmd, ShardKind shardKind) {
        ByteBuf buf = Unpooled.buffer();
        cmd.route("SET", RouteKind.PRIMARY.name(), shardKind.name(), context.getMember().getId()).encode(buf);

        Object raw = BaseTest.runCommand(channel, buf);
        if (raw instanceof SimpleStringRedisMessage message) {
            assertEquals(Response.OK, message.content());
        } else if (raw instanceof ErrorRedisMessage message) {
            fail(message.content());
        }
    }

    private void setShardsReadWrite(KrAdminCommandBuilder<String, String> cmd, ShardKind shardKind) {
        ByteBuf buf = Unpooled.buffer();
        cmd.setShardStatus(shardKind.name(), "READWRITE").encode(buf);

        Object raw = BaseTest.runCommand(channel, buf);
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

        // Create namespace tombstones directory
        List<String> path = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                namespaceTombstones().
                toList();
        context.getFoundationDB().run(tr ->
                context.getDirectoryLayer().createOrOpen(tr, path).join()
        );

        await().atMost(5000, TimeUnit.MILLISECONDS).until(() -> {
            Attribute<Boolean> clusterInitialized = context.getMemberAttributes().attr(MemberAttributes.CLUSTER_INITIALIZED);
            return clusterInitialized.get() != null && clusterInitialized.get();
        });

        setPrimaryOwnersOfShards(cmd, ShardKind.STASH);
        await().atMost(5, TimeUnit.SECONDS).until(() -> areAllOwnedShardsOperable(StashService.NAME));

        setPrimaryOwnersOfShards(cmd, ShardKind.BUCKET);
        await().atMost(5, TimeUnit.SECONDS).until(() -> areAllOwnedShardsOperable(BucketService.NAME));

        setShardsReadWrite(cmd, ShardKind.STASH);
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            List<Integer> shardIds = context.getShardRegistry().getShardIds(ShardKind.STASH);
            return areAllShardsWritable(ShardKind.STASH, shardIds);
        });

        setShardsReadWrite(cmd, ShardKind.BUCKET);
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            List<Integer> shardIds = context.getShardRegistry().getShardIds(ShardKind.BUCKET);
            return areAllShardsWritable(ShardKind.BUCKET, shardIds);
        });
    }

    /**
     * Determines if all shards of the specified kind are in a writable state.
     *
     * @param shardKind the type of shard to check, represented by the {@code ShardKind} enum.
     * @param shardIds  the list of shard IDs to examine.
     * @return {@code true} if all shards are in the writable state; otherwise {@code false}.
     */
    private boolean areAllShardsWritable(ShardKind shardKind, List<Integer> shardIds) {
        RoutingService routing = context.getService(RoutingService.NAME);
        for (int shardId : shardIds) {
            Route route = routing.findRoute(shardKind, shardId);
            if (route == null) {
                return false;
            }
            if (!route.shardStatus().equals(ShardStatus.READWRITE)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if all shards owned by the current member for a specified service are operable.
     * A shard is considered operable if it is assigned to the current member and is in a valid operational state.
     *
     * @param serviceName the name of the service whose owned shard operability status is to be checked.
     * @return {@code true} if all owned shards are operable; {@code false} otherwise.
     */
    private boolean areAllOwnedShardsOperable(String serviceName) {
        RoutingService routing = context.getService(RoutingService.NAME);
        ShardOwnerService<Shard> service = context.getService(serviceName);
        assert service != null;
        int shards = context.getConfig().getInt("stash.shards");
        for (int shardId = 0; shardId < shards; shardId++) {
            Route route = routing.findRoute(ShardKind.STASH, shardId);
            if (route == null) {
                return false;
            }
            if (!route.primary().equals(context.getMember())) {
                // Not belong to this member
                continue;
            }
            Shard shard = service.getServiceContext().shards().get(shardId);
            if (shard == null) {
                return false;
            }
            if (!shard.isOperable()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Cleans up the test cluster by removing the corresponding directory in the FoundationDB database.
     */
    public void cleanupTestCluster() {
        // start() may fail before the context is initialized; in that case there is nothing to clean
        // up, and dereferencing the null context here would mask the real startup exception.
        if (context == null) {
            return;
        }
        context.getFoundationDB().run(tr -> {
            List<String> subpath = KronotopDirectory.kronotop().cluster(context.getClusterName()).toList();
            return context.getDirectoryLayer().removeIfExists(tr, subpath).join();
        });
    }

    public void shutdownWithoutCleanup() {
        super.shutdown();
        executor.shutdown();
        try {
            executor.awaitTermination(15, TimeUnit.SECONDS);
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException(exp);
        } finally {
            if (channel != null) {
                channel.finishAndReleaseAll();
            }
        }
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
