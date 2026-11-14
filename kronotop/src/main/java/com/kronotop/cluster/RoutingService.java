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

package com.kronotop.cluster;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.MemberAttributes;
import com.kronotop.cluster.handlers.KrAdminHandler;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.core.InfoCollector;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.internal.KeyWatcher;
import com.kronotop.metrics.InfoReport;
import com.kronotop.server.ServerKind;
import com.kronotop.task.TaskService;
import io.netty.util.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Keeps the in-memory routing table that maps each shard to its primary and standby members.
 */
public class RoutingService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Routing";
    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingService.class);

    private final ExecutorService executor;
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final MembershipService membership;
    private final AtomicReference<RoutingTable> routingTable = new AtomicReference<>(new RoutingTable());
    private final ConcurrentHashMap<RoutingEventKind, List<RoutingEventHook>> hooksByKind = new ConcurrentHashMap<>();

    private volatile boolean shutdown;

    public RoutingService(Context context) {
        super(context, NAME);

        this.membership = context.getService(MembershipService.NAME);

        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("kr.routing-%d").build();
        this.executor = Executors.newFixedThreadPool(2, factory);

        handlerMethod(ServerKind.INTERNAL, new KrAdminHandler(this));

        resumeCollectStalePrefixesTaskIfAny();
    }

    private void resumeCollectStalePrefixesTaskIfAny() {
        try (Transaction tr = context.getTransactionManager().create()) {
            KronotopDirectoryNode node = KronotopDirectory.
                    kronotop().
                    cluster(context.getClusterName()).
                    metadata().
                    tasks().
                    task(CollectStalePrefixesTask.NAME);
            try {
                DirectorySubspace subspace = context.getDirectoryLayer().open(tr, node.toList()).join();
                byte[] value = tr.get(subspace.pack(CollectStalePrefixesTask.METADATA_KEY.MEMBER_ID.name())).join();
                if (value == null) {
                    // This should not be happened
                    return;
                }
                String memberId = new String(value);
                if (context.getMember().getId().equals(memberId)) {
                    TaskService taskService = context.getService(TaskService.NAME);
                    CollectStalePrefixesTask task = new CollectStalePrefixesTask(context);
                    taskService.execute(task);
                    LOGGER.info("Resuming {}", CollectStalePrefixesTask.NAME);
                }
            } catch (CompletionException e) {
                if (e.getCause() instanceof NoSuchDirectoryException) {
                    // Ignore
                    return;
                }
                throw e;
            }
        }
    }

    /**
     * Registers a hook that runs when the given routing event happens on this member.
     *
     * @param kind the routing event that triggers the hook
     * @param hook the callback to run
     */
    public void registerHook(RoutingEventKind kind, RoutingEventHook hook) {
        hooksByKind.compute(kind, (ignored, value) -> {
            if (value == null) {
                value = new ArrayList<>();
            }
            value.add(hook);
            return value;
        });
    }

    @Override
    public void start() {
        if (!refreshClusterInitialized()) {
            executor.execute(new ClusterInitializationWatcher());
        } else {
            loadRoutingTableFromFoundationDB(true);
            executor.execute(new RoutingEventsWatcher());
        }
    }

    @Override
    public void shutdown() {
        shutdown = true;
        keyWatcher.unwatchAll();
        if (!ExecutorServiceUtil.shutdownNowThenAwaitTermination(executor)) {
            LOGGER.warn("Routing service cannot be stopped gracefully");
        }
    }

    /**
     * Returns the route of the given shard.
     *
     * @param shardId the shard id
     * @return the route with the primary and standby members, or null if the shard has no route yet
     */
    public Route findRoute(int shardId) {
        return routingTable.get().get(shardId);
    }

    @Override
    public void collectInfo(InfoCollector collector) {
        Member member = context.getMember();
        int primary = 0;
        int standby = 0;
        for (int shardId : context.getShardRegistry().getShardIds()) {
            Route route = findRoute(shardId);
            if (route == null) {
                continue;
            }
            if (member.equals(route.primary())) {
                primary++;
            } else if (route.standbys().contains(member)) {
                standby++;
            }
        }
        collector.put(InfoReport.KRONOTOP_SECTION, "primary_shards", primary);
        collector.put(InfoReport.KRONOTOP_SECTION, "standby_shards", standby);
    }

    private boolean isClusterInitialized_internal() {
        try (Transaction tr = context.getTransactionManager().create()) {
            DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
            byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_INITIALIZED));
            byte[] data = tr.get(key).join();
            if (data != null) {
                if (MembershipUtil.isTrue(data)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean refreshClusterInitialized() {
        boolean clusterInitialized = isClusterInitialized_internal();
        context.getMemberAttributes().attr(MemberAttributes.CLUSTER_INITIALIZED).set(clusterInitialized);
        return clusterInitialized;
    }

    private Set<Member> memberIdsToMembers(Transaction tr, Set<String> memberIds) {
        Set<Member> members = new HashSet<>();
        for (String memberId : memberIds) {
            Member member = membership.findMember(tr, memberId);
            members.add(member);
        }
        return members;
    }

    private Route loadRoute(Transaction tr, DirectorySubspace shardSubspace) {
        String primaryMemberId = MembershipUtil.loadPrimaryMemberId(tr, shardSubspace);
        if (primaryMemberId == null) {
            // No route set
            return null;
        }

        try {
            Member primary = membership.findMember(tr, primaryMemberId);
            ShardStatus shardStatus = ShardUtil.getShardStatus(tr, shardSubspace);

            Set<String> standbyIds = MembershipUtil.loadStandbyMemberIds(tr, shardSubspace);
            Set<Member> standbys = memberIdsToMembers(tr, standbyIds);

            return new Route(primary, standbys, shardStatus);
        } catch (MemberNotRegisteredException e) {
            LOGGER.error("Error while loading member", e);
        }

        return null;
    }

    private void loadRoute(Transaction tr, RoutingTable table, List<Integer> shardIds) {
        for (int shardId : shardIds) {
            DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(shardId);
            Route route = loadRoute(tr, shardSubspace);
            if (route != null) {
                table.set(shardId, route);
            }
        }
    }

    private void loadRoutingTableFromFoundationDB(boolean firstRun) {
        Attribute<Boolean> clusterInitialized = context.getMemberAttributes().attr(MemberAttributes.CLUSTER_INITIALIZED);
        if (clusterInitialized.get() == null || !clusterInitialized.get()) {
            return;
        }

        ShardRegistry shardRegistry = context.getShardRegistry();

        RoutingTable table = new RoutingTable();
        try (Transaction tr = context.getTransactionManager().create()) {
            shardRegistry.refresh(tr);
            loadRoute(tr, table, shardRegistry.getShardIds());
        }
        RoutingTable previous = routingTable.getAndSet(table);
        if (!firstRun) {
            changesBetweenRoutingTables(previous);
        }
    }

    private void runHooks(RoutingEventKind routingEventKind, int shardId) {
        List<RoutingEventHook> hooks = hooksByKind.get(routingEventKind);
        if (hooks == null) {
            return;
        }
        for (RoutingEventHook hook : hooks) {
            try {
                hook.run(shardId);
            } catch (Exception e) {
                LOGGER.error("Error while running hook for ShardId: {}", shardId, e);
            }
        }
    }

    private void changesBetweenRoutingTables(RoutingTable previous) {
        List<Integer> shardIds = context.getShardRegistry().getShardIds();
        RoutingTable current = routingTable.get();

        for (int shardId : shardIds) {
            Route currentRoute = current.get(shardId);
            if (currentRoute == null) {
                // Not assigned yet
                continue;
            }

            Route previousRoute = previous.get(shardId);
            if (previousRoute == null) {
                // Bootstrapping...
                if (currentRoute.primary().equals(context.getMember())) {
                    // Load the shard from local disk
                    runHooks(RoutingEventKind.INITIALIZE_SHARD, shardId);
                }
            }

            if (!currentRoute.standbys().isEmpty()) {
                if (previousRoute != null) {
                    if (currentRoute.standbys().contains(context.getMember())) {
                        // New assignment
                        if (!previousRoute.standbys().contains(context.getMember())) {
                            runHooks(RoutingEventKind.START_REPLICATION, shardId);
                        }
                    }
                } else {
                    // No previous root exists
                    if (currentRoute.standbys().contains(context.getMember())) {
                        // New assignment
                        runHooks(RoutingEventKind.START_REPLICATION, shardId);
                    }
                }
            }

            if (previousRoute != null) {
                if (!previousRoute.primary().equals(currentRoute.primary())) {
                    // Primary owner has changed
                    if (previousRoute.primary().equals(context.getMember())) {
                        runHooks(RoutingEventKind.HAND_OVER_SHARD_OWNERSHIP, shardId);
                    }

                    // Standbys should connect to the new primary owner
                    if (previousRoute.standbys().contains(context.getMember())) {
                        runHooks(RoutingEventKind.PRIMARY_OWNER_CHANGED, shardId);
                    }
                }
            }

            if (previousRoute != null) {
                if (previousRoute.standbys().contains(context.getMember())) {
                    if (!currentRoute.standbys().contains(context.getMember())) {
                        // Stop replication
                        runHooks(RoutingEventKind.STOP_REPLICATION, shardId);
                    }
                }
            }
        }
    }

    private class ClusterInitializationWatcher implements Runnable {

        @Override
        public void run() {
            if (shutdown) {
                return;
            }

            while (!shutdown) {
                DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
                byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_INITIALIZED));

                try (Transaction tr = context.getTransactionManager().create()) {
                    CompletableFuture<Void> watcher = keyWatcher.watch(tr, key);
                    tr.commit().join();
                    if (!refreshClusterInitialized()) {
                        try {
                            watcher.join();
                        } catch (CancellationException e) {
                            LOGGER.debug("Cluster initialization watcher has been cancelled");
                            return;
                        }
                        if (!refreshClusterInitialized()) {
                            continue;
                        }
                    }
                    keyWatcher.unwatch(key);
                    executor.execute(new RoutingEventsWatcher());
                    return;
                } catch (Exception exp) {
                    LOGGER.error("Error while waiting for cluster initialization", exp);
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                }
            }
        }
    }

    private class RoutingEventsWatcher implements Runnable {

        @Override
        public void run() {
            if (shutdown) {
                return;
            }

            DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
            byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_TOPOLOGY_CHANGED));
            while (!shutdown) {
                try (Transaction tr = context.getTransactionManager().create()) {
                    CompletableFuture<Void> watcher = keyWatcher.watch(tr, key);
                    tr.commit().join();

                    loadRoutingTableFromFoundationDB(false);

                    // Wait for routing table changes
                    try {
                        watcher.join();
                    } catch (CancellationException e) {
                        LOGGER.debug("Routing events watcher has been cancelled");
                        return;
                    }
                    LOGGER.debug("Routing events watcher has been triggered");
                    loadRoutingTableFromFoundationDB(false);
                } catch (Exception e) {
                    LOGGER.error("Error while waiting for routing events", e);
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                }
            }
        }
    }
}
