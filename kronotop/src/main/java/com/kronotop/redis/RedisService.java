/*
 * Copyright (c) 2023-2025 Kronotop
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

import com.apple.foundationdb.Transaction;
import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.ServiceContext;
import com.kronotop.cluster.*;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.redis.handlers.InfoHandler;
import com.kronotop.redis.handlers.client.ClientHandler;
import com.kronotop.redis.handlers.cluster.ClusterHandler;
import com.kronotop.redis.handlers.connection.AuthHandler;
import com.kronotop.redis.handlers.connection.HelloHandler;
import com.kronotop.redis.handlers.connection.PingHandler;
import com.kronotop.redis.handlers.connection.SelectHandler;
import com.kronotop.redis.handlers.generic.*;
import com.kronotop.redis.handlers.hash.*;
import com.kronotop.redis.handlers.string.*;
import com.kronotop.redis.handlers.transactions.*;
import com.kronotop.redis.server.CommandHandler;
import com.kronotop.redis.server.FlushAllHandler;
import com.kronotop.redis.server.FlushDBHandler;
import com.kronotop.redis.storage.*;
import com.kronotop.redis.storage.impl.OnHeapRedisShardImpl;
import com.kronotop.server.*;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.Session;
import com.kronotop.watcher.Watcher;
import io.lettuce.core.cluster.SlotHash;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
    private final RoutingService routing;
    private final MembershipService membership;
    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Thread.ofVirtual().name("kr.redis-service", 0L).factory()
    );
    private final int numberOfShards;
    private final List<VolumeSyncWorker> volumeSyncWorkers = new ArrayList<>();

    public RedisService(Context context) throws CommandAlreadyRegisteredException {
        super(context, NAME);

        this.watcher = context.getService(Watcher.NAME);
        this.numberOfShards = context.getConfig().getInt("redis.shards");
        this.routing = context.getService(RoutingService.NAME);
        this.membership = context.getService(MembershipService.NAME);
        this.hashSlots = distributeHashSlots();
        this.serviceContext = context.getServiceContext(NAME);

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
        handlerMethod(ServerKind.EXTERNAL, new ReadonlyHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ReadWriteHandler(this));
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
        handlerMethod(ServerKind.EXTERNAL, new ClientHandler(this));

        // Internals
        handlerMethod(ServerKind.INTERNAL, new HelloHandler(this));
        handlerMethod(ServerKind.INTERNAL, new ClientHandler(this));
        handlerMethod(ServerKind.INTERNAL, new PingHandler());
        handlerMethod(ServerKind.INTERNAL, new ClusterHandler(this));
        handlerMethod(ServerKind.INTERNAL, new InfoHandler(this));
        handlerMethod(ServerKind.INTERNAL, new CommandHandler(this));

        routing.registerHook(RoutingEventKind.LOAD_REDIS_SHARD, new LoadRedisShardHook());
    }

    /**
     * Checks if the Redis value in the provided container is of the specified kind. If the container is null,
     * the method returns without any action. If the container's kind does not match the specified kind,
     * a WrongTypeException is thrown.
     *
     * @param container the RedisValueContainer holding the Redis value whose kind is to be checked
     * @param kind      the expected RedisValueKind that the value should be
     * @throws WrongTypeException if the value kind in the container does not match the expected kind
     */
    public static void checkRedisValueKind(RedisValueContainer container, RedisValueKind kind) {
        if (container == null) {
            return;
        }
        if (!container.kind().equals(kind)) {
            throw new WrongTypeException();
        }
    }

    private void initializeVolumeSyncerWorkers() {
        int numVolumeSyncWorkers = context.getConfig().getInt("redis.volume_syncer.workers");
        int period = context.getConfig().getInt("redis.volume_syncer.period");
        for (int workerId = 0; workerId < numVolumeSyncWorkers; workerId++) {
            VolumeSyncWorker worker = new VolumeSyncWorker(context, workerId);
            volumeSyncWorkers.add(worker);
            scheduledExecutorService.scheduleAtFixedRate(worker, 0, period, TimeUnit.MILLISECONDS);
        }
    }

    private void createAndLoadRedisShard(int shardId) {
        serviceContext.shards().put(shardId, new OnHeapRedisShardImpl(context, shardId));
        RedisShard shard = serviceContext.shards().get(shardId);
        RedisShardLoader loader = new RedisShardLoader(context, shard);
        loader.load();
    }

    /**
     * Loads the Redis shard from the local disk based on the given shard ID.
     * It first attempts to find the route for the specified shard and then loads
     * the shard if the current member is either the primary or one of the standbys.
     *
     * @param sharId the ID of the shard to load from disk
     */
    private void loadRedisShardFromDisk(int sharId) {
        Route route = routing.findRoute(ShardKind.REDIS, sharId);
        if (route == null) {
            return;
        }
        if (route.primary().equals(context.getMember()) || route.standbys().contains(context.getMember())) {
            LOGGER.trace("Loading Redis Shard: {} from the local disk", sharId);
            // Primary or standby owner
            createAndLoadRedisShard(sharId);
        }
    }

    public void start() {
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            loadRedisShardFromDisk(shardId);
        }
        initializeVolumeSyncerWorkers();
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
    private RedisShard getShard(Integer shardId) {
        return serviceContext.shards().compute(shardId, (k, shard) -> {
            if (shard != null) {
                return shard;
            }

            // Check routing info to return a sensible error message.
            Route route = routing.findRoute(ShardKind.REDIS, shardId);
            if (route == null) {
                throw new KronotopException(String.format("shard id: %d not owned by any member yet", shardId));
            }

            if (!route.primary().equals(context.getMember())) {
                // This should not be happened at production but just for safety.
                throw new KronotopException(String.format("shard id: %d not belong to this node: %s", shardId, context.getMember()));
            }

            throw new KronotopException(String.format("shard id: %d not usable yet", shardId));
        });
    }

    /**
     * Clears all shards, pausing volume synchronization workers and
     * ensuring shards are set to read-only mode. This method clears
     * all in-memory data and commits the transaction to the database.
     * After clearing, it resumes the volume synchronization workers
     * and restores shard operability.
     */
    public void clearShards() {
        try {
            volumeSyncWorkers.forEach(VolumeSyncWorker::pause);

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                // Stop all the operations on the owned shards
                serviceContext.shards().values().forEach(shard -> {
                    ShardUtils.setShardStatus(context, tr, ShardKind.REDIS, ShardStatus.READONLY, shard.id());
                });
                membership.triggerClusterTopologyWatcher(tr);
                tr.commit().join();
            }

            // Clear all in-memory data
            serviceContext.shards().values().forEach(shard -> {
                shard.storage().clear();
                shard.volumeSyncQueue().clear();
            });

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Prefix prefix = new Prefix(context.getConfig().getString("redis.volume_syncer.prefix").getBytes());
                Session session = new Session(tr, prefix);
                serviceContext.shards().values().forEach(shard -> shard.volume().clearPrefix(session));
                tr.commit().join();
            }
        } finally {
            volumeSyncWorkers.forEach(VolumeSyncWorker::resume);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                // Stop all the operations on the owned shards
                serviceContext.shards().values().forEach(shard -> {
                    ShardUtils.setShardStatus(context, tr, ShardKind.REDIS, ShardStatus.READWRITE, shard.id());
                });
                membership.triggerClusterTopologyWatcher(tr);
                tr.commit().join();
            }
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

    /**
     * Finds a Redis shard by its ID and checks if it matches the desired shard status.
     * If the shard is in an inoperable status or does not have the desired status, an exception is thrown.
     *
     * @param route              the route containing the primary and standby members and their associated shard statuses
     * @param shardId            the ID of the shard to be found
     * @param desiredShardStatus the status that the shard is expected to have
     * @return the RedisShard object if the shard is found and its status matches the desired status
     * @throws KronotopException if the shard is in an inoperable status or does not match the desired status
     */
    private RedisShard findShardAndCheckStatus(Route route, int shardId, ShardStatus desiredShardStatus) {
        if (route.shardStatus().equals(ShardStatus.INOPERABLE)) {
            throw new KronotopException(String.format("shard id: %d is in %s status", shardId, ShardStatus.INOPERABLE));
        }

        if (ShardStatus.READONLY.equals(desiredShardStatus)) {
            // ShardStatus can be READWRITE or READONLY
            if (route.shardStatus().equals(ShardStatus.READWRITE) || route.shardStatus().equals(ShardStatus.READONLY)) {
                return getShard(shardId);
            }
        }

        if (ShardStatus.READWRITE.equals(desiredShardStatus)) {
            // ShardStatus can only be READWRITE
            if (route.shardStatus().equals(ShardStatus.READWRITE)) {
                return getShard(shardId);
            }
        }

        throw new KronotopException(String.format("shard id: %d is not in %s status", shardId, desiredShardStatus));
    }

    /**
     * Finds the route information for a Redis shard with the specified shard ID.
     *
     * @param shardId the ID of the shard for which to retrieve the route information
     * @return the Route object containing the primary and standby members associated with the specified shard ID
     * @throws ClusterNotInitializedException if the cluster has not been initialized yet
     * @throws KronotopException              if the shard ID is not owned by any member yet
     */
    private Route findRoute(int shardId) {
        if (!routing.isClusterInitialized()) {
            throw new ClusterNotInitializedException();
        }

        Route route = routing.findRoute(ShardKind.REDIS, shardId);
        if (route == null) {
            throw new KronotopException(String.format("shard id: %d not owned by any member yet", shardId));
        }
        return route;
    }

    /**
     * Find the shard associated with the given slot.
     *
     * @param slot the slot to find the shard for
     * @return the Shard object associated with the given slot
     * @throws KronotopException if the slot is not owned by any member yet or not owned by the current member
     */
    private RedisShard findShard_internal(int slot, ShardStatus desiredShardStatus) {
        int shardId = getHashSlots().get(slot);
        Route route = findRoute(shardId);
        if (!route.primary().equals(getContext().getMember())) {
            throw new KronotopException(
                    RESPError.MOVED,
                    String.format("%d %s:%d", slot, route.primary().getExternalAddress().getHost(), route.primary().getExternalAddress().getPort())
            );
        }
        return findShardAndCheckStatus(route, shardId, desiredShardStatus);
    }

    /**
     * Find the shard associated with the given key.
     *
     * @param key the key to find the shard for
     * @return the Shard object associated with the given key
     * @throws KronotopException if the key's slot is not owned by any member yet or not owned by the current member
     */
    public RedisShard findShard(String key, ShardStatus desiredShardStatus) {
        int slot = SlotHash.getSlot(key);
        return findShard_internal(slot, desiredShardStatus);
    }

    /**
     * Finds the shard associated with the given list of keys.
     *
     * @param keys the list of keys to find the shard for
     * @return the Shard object associated with the given keys
     * @throws KronotopException    if the shard is not operable, not owned by any member yet, or not usable yet
     * @throws NullPointerException if the slot cannot be calculated for the given set of keys
     */
    public RedisShard findShard(List<String> keys, ShardStatus desiredShardStatus) {
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
        return findShard_internal(latestSlot, desiredShardStatus);
    }

    /**
     * Finds and retrieves a Redis shard with a given shard ID and verifies its status.
     *
     * @param shardId            the ID of the shard to be located
     * @param desiredShardStatus the expected status of the shard; the method will ensure that
     *                           the shard matches this status before returning
     * @return the RedisShard object associated with the specified shard ID and desired status
     * @throws KronotopException if the shard is inoperable or if its status does not match
     *                           the desired status
     */
    public RedisShard findShard(int shardId, ShardStatus desiredShardStatus) {
        Route route = findRoute(shardId);
        return findShardAndCheckStatus(route, shardId, desiredShardStatus);
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
     * This method returns a list of SlotRange objects representing the ranges of hash slots.
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
                Route route = routing.findRoute(ShardKind.REDIS, currentShardId);
                currentRange.setPrimary(route.primary());
                currentRange.setStandbys(route.standbys());
                currentRange.setShardId(currentShardId);
                ranges.add(currentRange);
                currentRange = new SlotRange(hashSlot);
            }
            currentShardId = shardId;
            latestHashSlot = hashSlot;
        }

        currentRange.setEnd(latestHashSlot);
        Route route = routing.findRoute(ShardKind.REDIS, currentShardId);
        currentRange.setPrimary(route.primary());
        currentRange.setStandbys(route.standbys());
        ranges.add(currentRange);

        return ranges;
    }

    /**
     * LoadRedisShardHook is a private class that implements the RoutingEventHook interface.
     * It is responsible for handling the loading of Redis shards from disk when a routing event occurs.
     */
    private class LoadRedisShardHook implements RoutingEventHook {

        @Override
        public void run(ShardKind shardKind, int shardId) {
            final int finalShardId = shardId;
            Thread.ofVirtual().name("kr.redis.load-redis-shard").start(() -> {
                loadRedisShardFromDisk(finalShardId);
            });
        }
    }
}
