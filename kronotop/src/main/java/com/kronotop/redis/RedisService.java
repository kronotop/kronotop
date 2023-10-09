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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import com.kronotop.core.KronotopService;
import com.kronotop.core.cluster.ClusterService;
import com.kronotop.core.cluster.Member;
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
import com.kronotop.redis.storage.Partition;
import com.kronotop.redis.storage.PartitionMaintenanceTask;
import com.kronotop.redis.storage.PartitionStatus;
import com.kronotop.redis.storage.impl.OnHeapPartitionImpl;
import com.kronotop.redis.storage.persistence.DataStructure;
import com.kronotop.redis.storage.persistence.PartitionLoader;
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
    private final ClusterService clusterService;
    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryBuilder().setNameFormat("kr.redis-service-%d").build()
    );
    private final int partitionCount;

    public RedisService(Context context, Handlers handlers) throws CommandAlreadyRegisteredException {
        this.handlers = handlers;
        this.context = context;
        this.watcher = context.getService(Watcher.NAME);
        this.partitionCount = context.getConfig().getInt("cluster.partition_count");
        this.clusterService = context.getService(ClusterService.NAME);
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
            PartitionMaintenanceTask partitionMaintenanceTask = new PartitionMaintenanceTask(context, id);
            scheduledExecutorService.scheduleAtFixedRate(partitionMaintenanceTask, 0, period, TimeUnit.SECONDS);
        }

        long start = System.nanoTime();
        logger.info("Loading Redis data structures");
        loadPartitionsFromFDB();
        long finish = System.nanoTime();
        logger.info("Redis data structures loaded in {} ms", TimeUnit.NANOSECONDS.toMillis(finish - start));
    }

    private void loadPartitionsFromFDB() {
        int currentPartitionId = 0;
        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                for (int partitionId = currentPartitionId; partitionId < partitionCount; partitionId++) {
                    Member owner = clusterService.getRoutingTable().getPartitionOwner(partitionId);
                    if (!owner.equals(context.getMember())) {
                        continue;
                    }

                    int finalPartitionId = partitionId;

                    Partition partition = context.getLogicalDatabase().getPartitions().compute(finalPartitionId,
                            (key, value) -> Objects.requireNonNullElseGet(
                                    value, () -> new OnHeapPartitionImpl(finalPartitionId))
                    );

                    PartitionLoader partitionLoader = new PartitionLoader(context, partition);
                    for (DataStructure dataStructure : DataStructure.values()) {
                        partitionLoader.load(tr, dataStructure);
                    }

                    // Make it operable
                    partition.getPartitionMetadata().setStatus(PartitionStatus.WRITABLE);
                    currentPartitionId = finalPartitionId;
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
    }

    private Map<Integer, Integer> distributeHashSlots() {
        HashMap<Integer, Integer> result = new HashMap<>();
        int hashSlotsPerPartition = NUM_HASH_SLOTS / partitionCount;
        int counter = 1;
        int partId = 0;
        for (int hashSlot = 0; hashSlot < NUM_HASH_SLOTS; hashSlot++) {
            if (counter >= hashSlotsPerPartition) {
                counter = 1;
                partId++;
                if (partId >= partitionCount) {
                    partId = 0;
                }
            }
            result.put(hashSlot, partId);
            counter++;
        }
        return Collections.unmodifiableMap(result);
    }

    public int getPartitionCount() {
        return partitionCount;
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

    public Partition getPartition(Integer partitionId) {
        return context.getLogicalDatabase().getPartitions().compute(partitionId, (k, partition) -> {
            if (partition != null) {
                return partition;
            }
            Member owner = clusterService.getRoutingTable().getPartitionOwner(partitionId);
            if (!owner.equals(context.getMember())) {
                // This should not be happened at production but just for safety.
                throw new KronotopException(String.format("PartitionId: %d not belong to this node: %s", partitionId, context.getMember()));
            }
            partition = new OnHeapPartitionImpl(partitionId);
            partition.setOwner(context.getMember());
            return partition;
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
        for (Partition partition : context.getLogicalDatabase().getPartitions().values()) {
            partition.clear();
            partition.getPersistenceQueue().clear();
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
        for (Partition partition : context.getLogicalDatabase().getPartitions().values()) {
            if (partition.getPersistenceQueue().size() > 0) {
                Persistence persistence = new Persistence(context, partition);
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

    public ClusterService getClusterService() {
        return clusterService;
    }

    private Partition resolveKeyInternal(ChannelHandlerContext context, int slot) {
        int partId = getHashSlots().get(slot);
        Member owner = getClusterService().getRoutingTable().getPartitionOwner(partId);
        if (!owner.equals(getContext().getMember())) {
            throw new KronotopException(
                    RESPError.MOVED,
                    String.format("%d %s:%d", slot, owner.getAddress().getHost(), owner.getAddress().getPort())
            );
        }
        return getPartition(partId);
    }

    public Partition resolveKey(ChannelHandlerContext context, String key) {
        int slot = SlotHash.getSlot(key);
        return resolveKeyInternal(context, slot);
    }

    public Partition resolveKeys(ChannelHandlerContext context, List<String> keys) {
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
        return resolveKeyInternal(context, latestSlot);
    }

    public Map<Integer, Integer> getHashSlots() {
        return hashSlots;
    }
}
