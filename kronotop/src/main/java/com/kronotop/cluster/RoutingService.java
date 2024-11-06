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

package com.kronotop.cluster;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.*;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.common.KronotopException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class RoutingService extends BaseKronotopService implements KronotopService {
    public static final String NAME = "Routing";
    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingService.class);

    private final ScheduledThreadPoolExecutor scheduler;
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final MembershipService membership;
    private final AtomicReference<RoutingTable> routingTable = new AtomicReference<>(new RoutingTable());

    private volatile boolean clusterInitialized;
    private volatile boolean isShutdown;

    public RoutingService(Context context) {
        super(context, NAME);

        this.membership = context.getService(MembershipService.NAME);

        ThreadFactory factory = Thread.ofVirtual().name("kr.routing").factory();
        this.scheduler = new ScheduledThreadPoolExecutor(1, factory);
    }

    public void start() {
        clusterInitialized = isClusterInitialized_internal();
        if (clusterInitialized) {
            loadRoutingTableFromFoundationDB();
        } else {
            scheduler.execute(new ClusterInitializationWatcher());
        }
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        try {
            keyWatcher.unwatchAll();
            scheduler.shutdownNow();
            if (!scheduler.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.warn("{} service cannot be stopped gracefully", NAME);
            }
        } catch (InterruptedException e) {
            throw new KronotopException(e);
        }
    }

    /**
     * Checks if the cluster has been initialized.
     *
     * @return true if the cluster is initialized, otherwise false.
     */
    public boolean isClusterInitialized() {
        return clusterInitialized;
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
            byte[] key = subspace.pack(Tuple.from(MembershipConstants.CLUSTER_INITIALIZED));
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
            ShardStatus shardStatus = MembershipUtils.loadShardStatus(tr, shardSubspace);
            Set<String> standbyIds = MembershipUtils.loadStandbyMemberIds(tr, shardSubspace);
            Set<Member> standbys = new HashSet<>();
            for (String standbyId : standbyIds) {
                Member standby = membership.findMember(standbyId);
                standbys.add(standby);
            }
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
     * Loads the routing table from FoundationDB.
     * <p>
     * This method initializes a new RoutingTable instance and populates it
     * with route information for various shard types supported in the system.
     * Currently, it supports only the REDIS shard kind. For each shard kind
     * (currently only REDIS), it reads configuration properties to determine
     * the number of shards and loads the route info for each shard into the routing table.
     * <p>
     * Transactions are used to ensure that the read operations from FoundationDB
     * are consistent. In case of encountering an unsupported shard kind,
     * the method throws a KronotopException.
     * <p>
     * This method is designed to be used internally within the RoutingService class.
     *
     * @throws KronotopException if an unknown shard kind is encountered
     */
    private void loadRoutingTableFromFoundationDB() {
        LOGGER.debug("Loading routing table from FoundationDB");
        RoutingTable table = new RoutingTable();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (ShardKind shardKind : ShardKind.values()) {
                if (shardKind.equals(ShardKind.REDIS)) {
                    int numberOfShards = context.getConfig().getInt("redis.shards");
                    loadRoute(tr, table, shardKind, numberOfShards);
                } else {
                    throw new KronotopException("Unknown shard kind: " + shardKind);
                }
            }
        }
        routingTable.set(table);
    }

    public void loadRoutingTableFromFoundationDB_Eagerly() {
        // TODO: CLUSTER-REFACTORING
        loadRoutingTableFromFoundationDB();
    }

    private class ClusterInitializationWatcher implements Runnable {

        @Override
        public void run() {
            if (isShutdown) {
                return;
            }

            DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
            byte[] key = subspace.pack(Tuple.from(MembershipConstants.CLUSTER_INITIALIZED));

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                CompletableFuture<Void> watcher = keyWatcher.watch(tr, key);
                tr.commit().join();
                try {
                    clusterInitialized = isClusterInitialized_internal();
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
            } catch (Exception e) {
                LOGGER.error("Error while waiting for cluster initialization", e);
            } finally {
                if (!isShutdown) {
                    if (clusterInitialized) {
                        loadRoutingTableFromFoundationDB();
                    } else {
                        // Try again
                        scheduler.execute(this);
                    }
                }
            }
        }
    }
}
