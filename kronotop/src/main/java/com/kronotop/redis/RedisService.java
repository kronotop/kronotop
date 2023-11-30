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

package com.kronotop.redis;

import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import com.kronotop.core.KronotopService;
import com.kronotop.core.cluster.MembershipService;
import com.kronotop.core.cluster.coordinator.Route;
import com.kronotop.core.watcher.Watcher;
import com.kronotop.redis.cluster.ClusterHandler;
import com.kronotop.redis.connection.AuthHandler;
import com.kronotop.redis.connection.HelloHandler;
import com.kronotop.redis.connection.PingHandler;
import com.kronotop.redis.connection.SelectHandler;
import com.kronotop.redis.generic.*;
import com.kronotop.redis.hash.*;
import com.kronotop.redis.server.CommandHandler;
import com.kronotop.redis.server.FlushAllHandler;
import com.kronotop.redis.server.FlushDBHandler;
import com.kronotop.redis.storage.LogicalDatabase;
import com.kronotop.redis.storage.Shard;
import com.kronotop.redis.storage.ShardMaintenanceTask;
import com.kronotop.redis.storage.impl.OnHeapShardImpl;
import com.kronotop.redis.storage.persistence.Persistence;
import com.kronotop.redis.string.*;
import com.kronotop.redis.transactions.*;
import com.kronotop.server.resp.*;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.Commands;
import io.lettuce.core.cluster.SlotHash;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class RedisService implements KronotopService {
    public static final String REDIS_VERSION = "7.0.8";
    public static final String NAME = "Redis";
    private static final Logger logger = LoggerFactory.getLogger(RedisService.class);
    public final int NUM_HASH_SLOTS = 16384;
    private final Handlers handlers;
    private final Context context;
    private final Map<Integer, Integer> hashSlots;
    private final Watcher watcher;
    private final MembershipService membershipService;
    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryBuilder().setNameFormat("kr.redis-service-%d").build()
    );
    private final int numberOfShards;

    public RedisService(Context context, Handlers handlers) throws CommandAlreadyRegisteredException {
        this.handlers = handlers;
        this.context = context;
        this.watcher = context.getService(Watcher.NAME);
        this.numberOfShards = context.getConfig().getInt("cluster.number_of_shards");
        this.membershipService = context.getService(MembershipService.NAME);
        this.hashSlots = distributeHashSlots();

        registerHandler(new PingHandler());
        registerHandler(new AuthHandler(this));
        registerHandler(new InfoHandler(this));
        registerHandler(new SetHandler(this));
        registerHandler(new GetHandler(this));
        registerHandler(new SelectHandler(this));
        registerHandler(new DelHandler(this));
        registerHandler(new CommandHandler(this));
        registerHandler(new IncrByHandler(this));
        registerHandler(new DecrByHandler(this));
        registerHandler(new IncrHandler(this));
        registerHandler(new DecrHandler(this));
        registerHandler(new MSetHandler(this));
        registerHandler(new MGetHandler(this));
        registerHandler(new GetDelHandler(this));
        registerHandler(new AppendHandler(this));
        registerHandler(new ExistsHandler(this));
        registerHandler(new IncrByFloatHandler(this));
        registerHandler(new StrlenHandler(this));
        registerHandler(new SetRangeHandler(this));
        registerHandler(new GetRangeHandler(this));
        registerHandler(new GetSetHandler(this));
        registerHandler(new SubstrHandler(this));
        registerHandler(new SetNXHandler(this));
        registerHandler(new MSetNXHandler(this));
        registerHandler(new TypeHandler(this));
        registerHandler(new RenameHandler(this));
        registerHandler(new RenameNXHandler(this));
        registerHandler(new RandomKeyHandler(this));
        registerHandler(new ScanHandler(this));
        registerHandler(new MultiHandler(this));
        registerHandler(new DiscardHandler(this));
        registerHandler(new ExecHandler(this));
        registerHandler(new WatchHandler(this));
        registerHandler(new UnwatchHandler(this));
        registerHandler(new FlushDBHandler(this));
        registerHandler(new FlushAllHandler(this));
        registerHandler(new HelloHandler(this));
        registerHandler(new HSetHandler(this));
        registerHandler(new HGetHandler(this));
        registerHandler(new HDelHandler(this));
        registerHandler(new HKeysHandler(this));
        registerHandler(new HLenHandler(this));
        registerHandler(new HValsHandler(this));
        registerHandler(new HGetAllHandler(this));
        registerHandler(new HRandFieldHandler(this));
        registerHandler(new HIncrByHandler(this));
        registerHandler(new HIncrByFloatHandler(this));
        registerHandler(new HExistsHandler(this));
        registerHandler(new HStrlenHandler(this));
        registerHandler(new HSetNXHandler(this));
        registerHandler(new HMGetHandler(this));
        registerHandler(new HMSetHandler(this));
        registerHandler(new ClusterHandler(this));
    }

    public void start() {
        int numPersistenceWorkers = context.getConfig().getInt("persistence.num_workers");
        int period = context.getConfig().getInt("persistence.period");
        for (int id = 0; id < numPersistenceWorkers; id++) {
            ShardMaintenanceTask shardMaintenanceTask = new ShardMaintenanceTask(context, id);
            scheduledExecutorService.scheduleAtFixedRate(shardMaintenanceTask, 0, period, TimeUnit.SECONDS);
        }
    }

    private Map<Integer, Integer> distributeHashSlots() {
        HashMap<Integer, Integer> result = new HashMap<>();
        int hashSlotsPerShard = NUM_HASH_SLOTS / numberOfShards;
        int counter = 1;
        int partId = 0;
        for (int hashSlot = 0; hashSlot < NUM_HASH_SLOTS; hashSlot++) {
            if (counter >= hashSlotsPerShard) {
                counter = 1;
                partId++;
                if (partId >= numberOfShards) {
                    partId = 0;
                }
            }
            result.put(hashSlot, partId);
            counter++;
        }
        return Collections.unmodifiableMap(result);
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    private void registerHandler(Handler... handlers) throws CommandAlreadyRegisteredException {
        for (Handler handler : handlers) {
            Commands commands = handler.getClass().getAnnotation(Commands.class);
            if (commands != null) {
                for (Command command : commands.value()) {
                    this.handlers.register(command.value().toUpperCase(), handler);
                }
            } else {
                Command command = handler.getClass().getAnnotation(Command.class);
                this.handlers.register(command.value().toUpperCase(), handler);
            }
        }
    }

    public Shard getShard(Integer shardId) {
        return context.getLogicalDatabase().getShards().compute(shardId, (k, shard) -> {
            if (shard != null) {
                return shard;
            }
            Route route = membershipService.getRoutingTable().getRoute(shardId);
            if (route == null) {
                throw new KronotopException(RESPError.ERR, String.format("ShardId %d isn't owned by any member yet", shardId));
            }
            if (!route.getMember().equals(context.getMember())) {
                // This should not be happened at production but just for safety.
                throw new KronotopException(String.format("ShardId: %d not belong to this node: %s", shardId, context.getMember()));
            }
            shard = new OnHeapShardImpl(shardId);
            shard.setOwner(context.getMember());
            return shard;
        });
    }

    public void clearLogicalDatabase() {
        context.getFoundationDB().run(tr -> {
            DirectoryLayer directoryLayer = new DirectoryLayer();
            String subspaceName = DirectoryLayout.Builder.
                    clusterName(context.getClusterName()).
                    internal().
                    redis().
                    persistence().
                    logicalDatabase(LogicalDatabase.NAME).
                    toString();
            CompletableFuture<Void> future = directoryLayer.remove(tr, Arrays.asList(subspaceName.split("\\.")));
            try {
                future.join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof NoSuchDirectoryException) {
                    return null;
                }
                throw new RuntimeException(e);
            }
            return null;
        });
        for (Shard shard : context.getLogicalDatabase().getShards().values()) {
            shard.clear();
            shard.getPersistenceQueue().clear();
        }
    }

    public void cleanupRedisTransaction(ChannelHandlerContext ctx) {
        watcher.cleanupChannelHandlerContext(ctx);
        ctx.channel().attr(ChannelAttributes.REDIS_MULTI).set(false);
        ctx.channel().attr(ChannelAttributes.REDIS_MULTI_DISCARDED).set(false);
        Attribute<List<Request>> queuedCommands = ctx.channel().attr(ChannelAttributes.QUEUED_COMMANDS);
        for (Request cmd : queuedCommands.get()) {
            ReferenceCountUtil.release(cmd.getRedisMessage());
        }
        queuedCommands.set(new ArrayList<>());
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Context getContext() {
        return context;
    }

    public Watcher getWatcher() {
        return watcher;
    }

    private void drainPersistenceQueues() {
        logger.info("Draining persistence queue of Redis logical database");
        for (Shard shard : context.getLogicalDatabase().getShards().values()) {
            if (shard.getPersistenceQueue().size() > 0) {
                Persistence persistence = new Persistence(context, shard);
                while (!persistence.isQueueEmpty()) {
                    persistence.run();
                }
            }
        }
    }

    @Override
    public void shutdown() {
        scheduledExecutorService.shutdownNow();
        try {
            boolean result = scheduledExecutorService.awaitTermination(6, TimeUnit.SECONDS);
            if (result) {
                logger.debug("Persistence executor has been terminated");
            } else {
                logger.debug("Persistence executor has been terminated due to the timeout");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        drainPersistenceQueues();
    }

    public MembershipService getClusterService() {
        return membershipService;
    }

    private Shard resolveKeyInternal(int slot) {
        int shardId = getHashSlots().get(slot);
        Route route = getClusterService().getRoutingTable().getRoute(shardId);
        if (route == null) {
            throw new KronotopException(RESPError.ERR, String.format("slot %d isn't owned by any member yet", slot));
        }
        if (!route.getMember().equals(getContext().getMember())) {
            throw new KronotopException(
                    RESPError.MOVED,
                    String.format("%d %s:%d", slot, route.getMember().getAddress().getHost(), route.getMember().getAddress().getPort())
            );
        }
        return getShard(shardId);
    }

    public Shard resolveKey(String key) {
        int slot = SlotHash.getSlot(key);
        return resolveKeyInternal(slot);
    }

    public Shard resolveKeys(List<String> keys) {
        Integer latestSlot = null;
        for (String key : keys) {
            int currentSlot = SlotHash.getSlot(key);
            if (latestSlot != null && latestSlot != currentSlot) {
                throw new KronotopException(RESPError.CROSSSLOT, RESPError.CROSSSLOT_MESSAGE);
            }
            latestSlot = currentSlot;
        }
        if (latestSlot == null) {
            throw new NullPointerException("slot cannot be calculated for the given set of keys");
        }
        return resolveKeyInternal(latestSlot);
    }

    public Map<Integer, Integer> getHashSlots() {
        return hashSlots;
    }
}
