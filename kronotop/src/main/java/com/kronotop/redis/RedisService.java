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

package com.kronotop.redis;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.ServiceContext;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MembershipService;
import com.kronotop.cluster.coordinator.Coordinator;
import com.kronotop.cluster.coordinator.CoordinatorService;
import com.kronotop.cluster.coordinator.Route;
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.redis.cluster.ClusterHandler;
import com.kronotop.redis.connection.AuthHandler;
import com.kronotop.redis.connection.HelloHandler;
import com.kronotop.redis.connection.PingHandler;
import com.kronotop.redis.connection.SelectHandler;
import com.kronotop.redis.generic.*;
import com.kronotop.redis.hash.*;
import com.kronotop.redis.management.coordinator.RedisCoordinator;
import com.kronotop.redis.server.CommandHandler;
import com.kronotop.redis.server.FlushAllHandler;
import com.kronotop.redis.server.FlushDBHandler;
import com.kronotop.redis.storage.*;
import com.kronotop.redis.storage.impl.OnHeapRedisShardImpl;
import com.kronotop.redis.string.*;
import com.kronotop.redis.transactions.*;
import com.kronotop.server.*;
import com.kronotop.watcher.Watcher;
import com.typesafe.config.Config;
import io.lettuce.core.cluster.SlotHash;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RedisService extends CommandHandlerService implements KronotopService {
    public static final String REDIS_VERSION = "7.4.0";
    public static final String NAME = "Redis";
    public static final String LogicalDatabase = "0";
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisService.class);
    public final int NUM_HASH_SLOTS = 16384;
    private final ServiceContext<RedisShard> serviceContext;
    private final Map<Integer, Integer> hashSlots;
    private final Watcher watcher;
    private final MembershipService membership;
    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Thread.ofVirtual().name("kr.redis-service", 0L).factory()
    );
    private final boolean isVolumeSyncEnabled;
    private final int numberOfShards;
    private final List<VolumeSyncWorker> volumeSyncWorkers = new ArrayList<>();

    public RedisService(Context context) throws CommandAlreadyRegisteredException {
        super(context);
        this.watcher = context.getService(Watcher.NAME);
        this.numberOfShards = context.getConfig().getInt("redis.shards");
        this.isVolumeSyncEnabled = context.getConfig().getBoolean("redis.volume_syncer.enabled");
        this.membership = context.getService(MembershipService.NAME);
        this.hashSlots = distributeHashSlots();
        this.serviceContext = context.getServiceContext(NAME);

        RedisCoordinator coordinator = new RedisCoordinator(context);
        if (isVolumeSyncEnabled) {
            CoordinatorService coordinatorService = context.getService(CoordinatorService.NAME);
            coordinatorService.register(Coordinator.Service.REDIS, coordinator);
        }

        // TODO: CLUSTER-REFACTORING
        for (int i = 0; i < this.numberOfShards; i++) {
            RedisShard redisShard = new OnHeapRedisShardImpl(context, i);
            redisShard.setOperable(true);
            this.serviceContext.shards().put(i, redisShard);
        }

        // TODO: CLUSTER-REFACTORING
        for (int i = 0; i < this.numberOfShards; i++) {
            RedisShard shard = this.serviceContext.shards().get(i);
            RedisShardLoader redisShardLoader = new RedisShardLoader(context, shard);
            redisShardLoader.load();
        }

        handlerMethod(ServerKind.EXTERNAL, new PingHandler());
        handlerMethod(ServerKind.EXTERNAL, new AuthHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new InfoHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new SetHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new GetHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new SelectHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new DelHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new CommandHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new IncrByHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new DecrByHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new IncrHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new DecrHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new MSetHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new MGetHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new GetDelHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new AppendHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ExistsHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new IncrByFloatHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new StrlenHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new SetRangeHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new GetRangeHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new GetSetHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new SubstrHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new SetNXHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new MSetNXHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new TypeHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new RenameHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new RenameNXHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new RandomKeyHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ScanHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new MultiHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new DiscardHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ExecHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new WatchHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new UnwatchHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new FlushDBHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new FlushAllHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HelloHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HSetHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HGetHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HDelHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HKeysHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HLenHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HValsHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HGetAllHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HRandFieldHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HIncrByHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HIncrByFloatHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HExistsHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HStrlenHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HSetNXHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HMGetHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new HMSetHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ClusterHandler(this));
    }

    public static void checkRedisValueKind(RedisValueContainer container, RedisValueKind kind) {
        if (container == null) {
            return;
        }
        if (!container.kind().equals(kind)) {
            throw new WrongTypeException();
        }
    }

    public boolean isVolumeSyncEnabled() {
        return isVolumeSyncEnabled;
    }

    public void start() {
        if (isVolumeSyncEnabled()) {
            Config redisConfig = context.getConfig().getConfig("redis");
            int numVolumeSyncWorkers = redisConfig.getInt("volume_syncer.workers");
            int period = redisConfig.getInt("volume_syncer.period");
            for (int workerId = 0; workerId < numVolumeSyncWorkers; workerId++) {
                VolumeSyncWorker worker = new VolumeSyncWorker(context, workerId);
                volumeSyncWorkers.add(worker);
                scheduledExecutorService.scheduleAtFixedRate(worker, 0, period, TimeUnit.MILLISECONDS);
            }
            LOGGER.info("Volume sync has been enabled");
        }
    }

    /**
     * This method is used to distribute the hash slots among the shards in a Kronotop cluster.
     *
     * @return a map where the key represents the hash slot and the value represents the shard ID.
     */
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

    /**
     * Retrieves the Shard with the specified shard ID.
     *
     * @param shardId the ID of the shard to retrieve
     * @return the Shard object with the specified shard ID
     * @throws KronotopException if the shard is not operable, not owned by any member yet or not usable yet
     */
    public RedisShard getShard(Integer shardId) {
        return serviceContext.shards().compute(shardId, (k, shard) -> {
            if (shard != null) {
                if (!shard.isOperable()) {
                    throw new KronotopException(String.format("ShardId: %s not operable", shardId));
                }
                return shard;
            }
            Route route = membership.getRoutingTable().getRoute(shardId);
            if (route == null) {
                throw new KronotopException(RESPError.ERR, String.format("ShardId: %d not owned by any member yet", shardId));
            }
            if (!route.getMember().equals(context.getMember())) {
                // This should not be happened at production but just for safety.
                throw new KronotopException(String.format("ShardId: %d not belong to this node: %s", shardId, context.getMember()));
            }
            throw new KronotopException(String.format("ShardId: %d not usable yet", shardId));
        });
    }

    /**
     * Clears the logical database by performing the following steps:
     * 1. Pauses all shard maintenance workers.
     * 2. Sets all shards in the logical database to be inoperable and read-only.
     * 3. Clears the data structures for each shard in the logical database by deleting all data in the corresponding directory subspaces.
     * 4. Clears the in-memory data for each shard in the logical database.
     * 5. Resumes all shard maintenance workers.
     * 6. Restores the shard operability and read-write state.
     *
     * @throws RuntimeException if there is an error during the process.
     */
    public void clearShards() {
        try {
            volumeSyncWorkers.forEach(VolumeSyncWorker::pause);

            // Stop all the operations on the owned shards
            serviceContext.shards().values().forEach(shard -> {
                shard.setOperable(false);
                shard.setReadOnly(true);
            });

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                serviceContext.shards().values().forEach(shard -> {
                    for (DataStructure dataStructure : DataStructure.values()) {
                        try {
                            DirectorySubspace shardSubspace = context.getDirectoryLayer().openDataStructure(shard.id(), dataStructure);
                            tr.clear(Range.startsWith(shardSubspace.pack()));
                        } catch (CompletionException e) {
                            if (e.getCause() instanceof NoSuchDirectoryException) {
                                return;
                            }
                            throw new RuntimeException(e);
                        }
                    }
                });
                tr.commit().join();
            }

            // Clear all in-memory data
            serviceContext.shards().values().forEach(shard -> {
                shard.storage().clear();
                shard.volumeSyncQueue().clear();
            });
        } finally {
            volumeSyncWorkers.forEach(VolumeSyncWorker::resume);
            // Make the shard operable again
            serviceContext.shards().values().forEach(shard -> {
                shard.setReadOnly(false);
                shard.setOperable(true);
            });
        }
    }

    /**
     * Cleans up the Redis transaction by releasing the resources and resetting the transaction state.
     *
     * @param ctx the channel handler context associated with the transaction
     */
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

    public ServiceContext<RedisShard> getServiceContext() {
        return serviceContext;
    }

    public Watcher getWatcher() {
        return watcher;
    }

    @Override
    public void shutdown() {
        volumeSyncWorkers.forEach((worker) -> {
            worker.drainVolumeSyncQueues();
            worker.shutdown();
        });

        try {
            scheduledExecutorService.shutdownNow();
            if (!scheduledExecutorService.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.warn("Redis scheduled executor service could not be stopped gracefully");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public MembershipService getClusterService() {
        return membership;
    }

    /**
     * Find the shard associated with the given slot.
     *
     * @param slot the slot to find the shard for
     * @return the Shard object associated with the given slot
     * @throws KronotopException if the slot is not owned by any member yet or not owned by the current member
     */
    private RedisShard findShard_internal(int slot) {
        int shardId = getHashSlots().get(slot);
        Route route = getClusterService().getRoutingTable().getRoute(shardId);
        if (route == null) {
            throw new KronotopException(RESPError.ERR, String.format("slot %d isn't owned by any member yet", slot));
        }
        if (!route.getMember().equals(getContext().getMember())) {
            throw new KronotopException(
                    RESPError.MOVED,
                    String.format("%d %s:%d", slot, route.getMember().getExternalAddress().getHost(), route.getMember().getExternalAddress().getPort())
            );
        }
        return getShard(shardId);
    }

    /**
     * Find the shard associated with the given key.
     *
     * @param key the key to find the shard for
     * @return the Shard object associated with the given key
     * @throws KronotopException if the key's slot is not owned by any member yet or not owned by the current member
     */
    public RedisShard findShard(String key) {
        int slot = SlotHash.getSlot(key);
        return findShard_internal(slot);
    }

    /**
     * Finds the shard associated with the given list of keys.
     *
     * @param keys the list of keys to find the shard for
     * @return the Shard object associated with the given keys
     * @throws KronotopException    if the shard is not operable, not owned by any member yet, or not usable yet
     * @throws NullPointerException if the slot cannot be calculated for the given set of keys
     */
    public RedisShard findShard(List<String> keys) {
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
        return findShard_internal(latestSlot);
    }

    /**
     * Retrieves a map of hash slots and their corresponding shard IDs.
     *
     * @return a map where the key represents the hash slot and the value represents the shard ID
     */
    public Map<Integer, Integer> getHashSlots() {
        return hashSlots;
    }

    /**
     * This method returns a list of SlotRange objects representing the ranges of hash slots in a distributed system.
     *
     * @return a list of SlotRange objects
     */
    public List<SlotRange> getSlotRanges() {
        List<SlotRange> ranges = new ArrayList<>();

        SlotRange currentRange = new SlotRange(0);
        Integer currentShardId = null;
        int latestHashSlot = 0;

        for (int hashSlot = 0; hashSlot < NUM_HASH_SLOTS; hashSlot++) {
            int shardId = getHashSlots().get(hashSlot);
            if (currentShardId != null && shardId != currentShardId) {
                currentRange.setEnd(hashSlot - 1);
                Member owner = getClusterService().getRoutingTable().getRoute(currentShardId).getMember();
                currentRange.setOwner(owner);
                currentRange.setShardId(currentShardId);
                ranges.add(currentRange);
                currentRange = new SlotRange(hashSlot);
            }
            currentShardId = shardId;
            latestHashSlot = hashSlot;
        }

        currentRange.setEnd(latestHashSlot);
        Member owner = getClusterService().getRoutingTable().getRoute(currentShardId).getMember();
        currentRange.setOwner(owner);
        ranges.add(currentRange);

        return ranges;
    }
}
