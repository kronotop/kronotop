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

package com.kronotop.cluster;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.*;
import com.kronotop.cluster.handlers.KrAdminHandler;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.internal.KeyWatcher;
import com.kronotop.server.ServerKind;
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
 * Manages shard routing and topology change detection for the Kronotop cluster.
 *
 * <p>This service maintains an in-memory routing table that maps shards to their primary
 * and standby members. It watches FoundationDB for topology changes and triggers event
 * hooks when routing assignments change.
 *
 * <p>Key responsibilities:
 * <ul>
 *   <li>Loading and caching the routing table from FoundationDB</li>
 *   <li>Watching for cluster initialization and topology changes</li>
 *   <li>Detecting routing changes and invoking registered hooks</li>
 *   <li>Providing route lookups for shard-to-member resolution</li>
 * </ul>
 *
 * <p>The service uses two background watchers:
 * <ul>
 *   <li>{@link ClusterInitializationWatcher} - waits for cluster initialization</li>
 *   <li>{@link RoutingEventsWatcher} - monitors topology changes after initialization</li>
 * </ul>
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
    }

    /**
     * Registers a hook to be invoked when the specified routing event occurs.
     *
     * @param kind the routing event type that triggers this hook
     * @param hook the callback to execute when the event occurs
     */
    public void registerHook(RoutingEventKind kind, RoutingEventHook hook) {
        hooksByKind.compute(kind, (k, value) -> {
            if (value == null) {
                value = new ArrayList<>();
            }
            value.add(hook);
            return value;
        });
    }

    /**
     * Starts the routing service by initializing the routing table and background watchers.
     *
     * <p>If the cluster is already initialized, loads the routing table immediately and
     * starts the {@link RoutingEventsWatcher}. Otherwise, starts the
     * {@link ClusterInitializationWatcher} to wait for cluster initialization.
     */
    public void start() {
        Attribute<Boolean> clusterInitialized = context.getMemberAttributes().attr(MemberAttributes.CLUSTER_INITIALIZED);
        clusterInitialized.set(isClusterInitialized_internal());
        if (!clusterInitialized.get()) {
            executor.execute(new ClusterInitializationWatcher());
        } else {
            loadRoutingTableFromFoundationDB(true);
            executor.execute(new RoutingEventsWatcher());
        }
    }

    /**
     * Shuts down the routing service, cancelling all watchers and releasing resources.
     */
    @Override
    public void shutdown() {
        shutdown = true;
        keyWatcher.unwatchAll();
        if (!ExecutorServiceUtil.shutdownNowThenAwaitTermination(executor)) {
            LOGGER.warn("Routing service cannot be stopped gracefully");
        }
    }

    /**
     * Finds and returns the route information for the specified shard based on its kind and ID.
     *
     * @param kind    the kind of the shard, represented by an instance of ShardKind
     * @param shardId the ID of the shard for which to retrieve the route information
     * @return the Route object containing the primary and standby members, or null if no route information is found
     */
    public Route findRoute(ShardKind kind, int shardId) {
        return routingTable.get().get(kind, shardId);
    }

    /**
     * Checks if the cluster has been initialized by verifying a specific key
     * in the FoundationDB cluster metadata subspace.
     *
     * @return true if the cluster is initialized, otherwise false.
     */
    private boolean isClusterInitialized_internal() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
            byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_INITIALIZED));
            byte[] data = tr.get(key).join();
            if (data != null) {
                if (MembershipUtils.isTrue(data)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Converts a set of member IDs to their corresponding Member objects.
     */
    private Set<Member> memberIdsToMembers(Set<String> memberIds) {
        Set<Member> members = new HashSet<>();
        for (String memberId : memberIds) {
            Member member = membership.findMember(memberId);
            members.add(member);
        }
        return members;
    }

    /**
     * Loads the shard routing information from the specified subspace within a transaction.
     *
     * @param tr            The transaction used to read from the database.
     * @param shardSubspace The specific directory subspace containing the route information.
     * @return A {@link Route} object containing the primary and standby members,
     * or null if no route information is found.
     */
    private Route loadRoute(Transaction tr, DirectorySubspace shardSubspace) {
        String primaryMemberId = MembershipUtils.loadPrimaryMemberId(tr, shardSubspace);
        if (primaryMemberId == null) {
            // No route set
            return null;
        }

        try {
            Member primary = membership.findMember(primaryMemberId);
            ShardStatus shardStatus = ShardUtils.getShardStatus(tr, shardSubspace);

            Set<String> standbyIds = MembershipUtils.loadStandbyMemberIds(tr, shardSubspace);
            Set<Member> standbys = memberIdsToMembers(standbyIds);

            return new Route(primary, standbys, shardStatus);
        } catch (MemberNotRegisteredException e) {
            LOGGER.error("Error while loading member", e);
        }

        return null;
    }

    /**
     * Loads the routing information for shards into the provided routing table within a transaction.
     *
     * @param tr             The transaction used for database operations.
     * @param table          The routing table to update with routing information.
     * @param shardKind      The type of shard (e.g., REDIS) to load routes for.
     * @param numberOfShards The total number of shards to process.
     */
    private void loadRoute(Transaction tr, RoutingTable table, ShardKind shardKind, int numberOfShards) {
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(shardKind, shardId);
            Route route = loadRoute(tr, shardSubspace);
            if (route != null) {
                table.set(shardKind, shardId, route);
            }
        }
    }

    /**
     * Loads the routing table from FoundationDB and detects routing changes.
     *
     * <p>Creates a new routing table, loads routes for all shard kinds (REDIS, BUCKET),
     * and atomically replaces the current table. On subsequent runs, compares with
     * the previous table to detect and trigger routing event hooks.
     *
     * @param firstRun true if this is the initial load (skip change detection)
     * @throws KronotopException if an unknown shard kind is encountered
     */
    private void loadRoutingTableFromFoundationDB(boolean firstRun) {
        Attribute<Boolean> clusterInitialized = context.getMemberAttributes().attr(MemberAttributes.CLUSTER_INITIALIZED);
        if (clusterInitialized.get() == null || !clusterInitialized.get()) {
            return;
        }
        RoutingTable table = new RoutingTable();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (ShardKind shardKind : ShardKind.values()) {
                String path;
                if (shardKind.equals(ShardKind.REDIS)) {
                    path = "redis.shards";
                } else if (shardKind.equals(ShardKind.BUCKET)) {
                    path = "bucket.shards";
                } else {
                    throw new KronotopException("Unknown shard kind: " + shardKind);
                }

                int numberOfShards = context.getConfig().getInt(path);
                loadRoute(tr, table, shardKind, numberOfShards);
            }
        }
        RoutingTable previous = routingTable.getAndSet(table);
        if (!firstRun) {
            changesBetweenRoutingTables(previous, ShardKind.REDIS);
            changesBetweenRoutingTables(previous, ShardKind.BUCKET);
        }
    }

    /**
     * Executes all registered hooks for the specified routing event.
     */
    private void runHooks(RoutingEventKind routingEventKind, ShardKind shardKind, int shardId) {
        List<RoutingEventHook> hooks = hooksByKind.get(routingEventKind);
        if (hooks == null) {
            return;
        }
        for (RoutingEventHook hook : hooks) {
            try {
                hook.run(shardKind, shardId);
            } catch (Exception e) {
                LOGGER.error("Error while running hook for ShardKind: {}, ShardId:{}", shardKind, shardId, e);
            }
        }
    }

    /**
     * Detects routing changes between the previous and current routing tables, invoking
     * appropriate hooks for this member based on assignment changes.
     *
     * <p>Detected events include:
     * <ul>
     *   <li>New primary assignment (triggers LOAD_REDIS_SHARD or INITIALIZE_BUCKET_SHARD)</li>
     *   <li>New standby assignment (triggers START_REPLICATION)</li>
     *   <li>Primary ownership change (triggers HAND_OVER_SHARD_OWNERSHIP, PRIMARY_OWNER_CHANGED)</li>
     *   <li>Standby removal (triggers STOP_REPLICATION)</li>
     * </ul>
     */
    private void changesBetweenRoutingTables(RoutingTable previous, ShardKind shardKind) {
        int shards;
        if (shardKind.equals(ShardKind.BUCKET)) {
            shards = context.getConfig().getInt("bucket.shards");
        } else if (shardKind.equals(ShardKind.REDIS)) {
            shards = context.getConfig().getInt("redis.shards");
        } else {
            throw new KronotopException("Unknown shard kind: " + shardKind);
        }

        RoutingTable current = routingTable.get();

        for (int shardId = 0; shardId < shards; shardId++) {
            Route currentRoute = current.get(shardKind, shardId);
            if (currentRoute == null) {
                // Not assigned yet
                continue;
            }

            Route previousRoute = previous.get(shardKind, shardId);
            if (previousRoute == null) {
                // Bootstrapping...
                if (currentRoute.primary().equals(context.getMember())) {
                    // Load the shard from local disk
                    if (shardKind.equals(ShardKind.REDIS)) {
                        runHooks(RoutingEventKind.LOAD_REDIS_SHARD, ShardKind.REDIS, shardId);
                    } else {
                        runHooks(RoutingEventKind.INITIALIZE_BUCKET_SHARD, ShardKind.BUCKET, shardId);
                    }
                }
            }

            if (!currentRoute.standbys().isEmpty()) {
                if (previousRoute != null) {
                    if (currentRoute.standbys().contains(context.getMember())) {
                        // New assignment
                        if (!previousRoute.standbys().contains(context.getMember())) {
                            runHooks(RoutingEventKind.START_REPLICATION, shardKind, shardId);
                        }
                    }
                } else {
                    // No previous root exists
                    if (currentRoute.standbys().contains(context.getMember())) {
                        // New assignment
                        runHooks(RoutingEventKind.START_REPLICATION, shardKind, shardId);
                    }
                }
            }

            if (previousRoute != null) {
                if (!previousRoute.primary().equals(currentRoute.primary())) {
                    // Primary owner has changed
                    if (previousRoute.primary().equals(context.getMember())) {
                        runHooks(RoutingEventKind.HAND_OVER_SHARD_OWNERSHIP, shardKind, shardId);
                    }

                    // Standbys should connect to the new primary owner
                    if (previousRoute.standbys().contains(context.getMember())) {
                        runHooks(RoutingEventKind.PRIMARY_OWNER_CHANGED, shardKind, shardId);
                    }
                }
            }

            if (previousRoute != null) {
                if (previousRoute.standbys().contains(context.getMember())) {
                    if (!currentRoute.standbys().contains(context.getMember())) {
                        // Stop replication
                        runHooks(RoutingEventKind.STOP_REPLICATION, shardKind, shardId);
                    }
                }
            }
        }
    }

    /**
     * Watches for cluster initialization by monitoring the CLUSTER_INITIALIZED key in FoundationDB.
     *
     * <p>Blocks until the cluster is initialized, then transitions to {@link RoutingEventsWatcher}.
     */
    private class ClusterInitializationWatcher implements Runnable {

        @Override
        public void run() {
            if (shutdown) {
                return;
            }

            boolean clusterInitialized;
            while (!shutdown) {
                DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
                byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_INITIALIZED));

                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    CompletableFuture<Void> watcher = keyWatcher.watch(tr, key);
                    tr.commit().join();
                    try {
                        clusterInitialized = isClusterInitialized_internal();
                        context.getMemberAttributes().attr(MemberAttributes.CLUSTER_INITIALIZED).set(clusterInitialized);
                        if (clusterInitialized) {
                            keyWatcher.unwatch(key);
                            return;
                        }
                        watcher.join();
                    } catch (CancellationException e) {
                        LOGGER.debug("Cluster initialization watcher has been cancelled");
                        return;
                    }
                    clusterInitialized = isClusterInitialized_internal();
                    context.getMemberAttributes().attr(MemberAttributes.CLUSTER_INITIALIZED).set(clusterInitialized);
                    if (clusterInitialized) {
                        executor.execute(new RoutingEventsWatcher());
                        return;
                    }
                } catch (Exception exp) {
                    LOGGER.error("Error while waiting for cluster initialization", exp);
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                }
            }
        }
    }

    /**
     * Watches for topology changes by monitoring the CLUSTER_TOPOLOGY_CHANGED key in FoundationDB.
     *
     * <p>When triggered, reloads the routing table and invokes hooks for any detected changes.
     * Runs continuously until shutdown.
     */
    private class RoutingEventsWatcher implements Runnable {

        @Override
        public void run() {
            if (shutdown) {
                return;
            }

            DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
            byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_TOPOLOGY_CHANGED));
            while (!shutdown) {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
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
