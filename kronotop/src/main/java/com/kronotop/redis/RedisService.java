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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
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
import com.kronotop.redis.storage.ShardMaintenanceWorker;
import com.kronotop.redis.storage.persistence.DataStructure;
import com.kronotop.redis.storage.persistence.Persistence;
import com.kronotop.redis.string.*;
import com.kronotop.redis.transactions.*;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.Commands;
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
    private final List<ShardMaintenanceWorker> shardMaintenanceWorkers = new ArrayList<>();

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
        for (int workerId = 0; workerId < numPersistenceWorkers; workerId++) {
            ShardMaintenanceWorker shardMaintenanceWorker = new ShardMaintenanceWorker(context, workerId);
            shardMaintenanceWorkers.add(shardMaintenanceWorker);
            scheduledExecutorService.scheduleAtFixedRate(shardMaintenanceWorker, 0, period, TimeUnit.SECONDS);
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

    /**
     * Retrieves the Shard with the specified shard ID.
     *
     * @param shardId the ID of the shard to retrieve
     * @return the Shard object with the specified shard ID
     * @throws KronotopException if the shard is not operable, not owned by any member yet or not usable yet
     */
    public Shard getShard(Integer shardId) {
        return context.getLogicalDatabase().getShards().compute(shardId, (k, shard) -> {
            if (shard != null) {
                if (!shard.isOperable()) {
                    throw new KronotopException(String.format("ShardId: %s not operable", shardId));
                }
                return shard;
            }
            Route route = membershipService.getRoutingTable().getRoute(shardId);
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
    public void clearLogicalDatabase() {
        try {
            shardMaintenanceWorkers.forEach(ShardMaintenanceWorker::pause);

            // Stop all the operations on the owned shards
            context.getLogicalDatabase().getShards().values().forEach(shard -> {
                shard.setOperable(false);
                shard.setReadOnly(true);
            });

            DirectoryLayer directoryLayer = new DirectoryLayer();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                context.getLogicalDatabase().getShards().values().forEach(shard -> {
                    for (DataStructure dataStructure : DataStructure.values()) {
                        List<String> subpath = DirectoryLayout.Builder.
                                clusterName(context.getClusterName()).
                                internal().
                                redis().
                                persistence().
                                logicalDatabase(LogicalDatabase.NAME).
                                shardId(shard.getId().toString()).
                                dataStructure(dataStructure.name().toLowerCase()).
                                asList();
                        try {
                            DirectorySubspace shardSubspace = directoryLayer.open(tr, subpath).join();
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
            context.getLogicalDatabase().getShards().values().forEach(shard -> {
                shard.clear();
                shard.getPersistenceQueue().clear();
            });
        } finally {
            shardMaintenanceWorkers.forEach(ShardMaintenanceWorker::unpause);
            // Make the shard operable again
            context.getLogicalDatabase().getShards().values().forEach(shard -> {
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

    public Watcher getWatcher() {
        return watcher;
    }

    /**
     * Drains the persistence queues of all shards in the logical database by running the persistence process for each key in the queues.
     * The persistence process retrieves the latest values for each key from the shard and persists them to the FoundationDB cluster.
     */
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

    /**
     * Find the shard associated with the given slot.
     *
     * @param slot the slot to find the shard for
     * @return the Shard object associated with the given slot
     * @throws KronotopException if the slot is not owned by any member yet or not owned by the current member
     */
    private Shard findShard_internal(int slot) {
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

    /**
     * Find the shard associated with the given key.
     *
     * @param key the key to find the shard for
     * @return the Shard object associated with the given key
     * @throws KronotopException if the key's slot is not owned by any member yet or not owned by the current member
     */
    public Shard findShard(String key) {
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
    public Shard findShard(List<String> keys) {
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

    public Map<Integer, Integer> getHashSlots() {
        return hashSlots;
    }
}
