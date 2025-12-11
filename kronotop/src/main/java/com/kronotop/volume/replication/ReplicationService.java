/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.BaseKronotopService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingEventKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.VolumeConfigGenerator;
import com.kronotop.volume.VolumeNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages volume replication for standby nodes in a Kronotop cluster.
 *
 * <p>This service coordinates replication watchdogs that stream data from primary nodes
 * to standbys. It responds to routing events (slot creation, primary changes, stop requests)
 * and manages the lifecycle of each shard's replication process.</p>
 *
 * <p>Replication is automatically started when a node becomes a standby for a shard,
 * and can be suppressed or explicitly restarted as needed.</p>
 */
public class ReplicationService extends BaseKronotopService implements KronotopService {
    public static final String NAME = "Replication";
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationService.class);
    private final RoutingService routing;
    private final ExecutorService executor;

    private final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
    private final EnumMap<ShardKind, Map<Integer, ReplicationWatchDog>> replications = new EnumMap<>(ShardKind.class);
    private final EnumMap<ShardKind, Map<Integer, Boolean>> suppressionFlag = new EnumMap<>(ShardKind.class);

    public ReplicationService(Context context) {
        super(context, NAME);
        this.routing = context.getService(RoutingService.NAME);
        this.replications.put(ShardKind.REDIS, new HashMap<>());
        this.replications.put(ShardKind.BUCKET, new HashMap<>());
        this.suppressionFlag.put(ShardKind.REDIS, new HashMap<>());
        this.suppressionFlag.put(ShardKind.BUCKET, new HashMap<>());
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("kr.volume-replication-%d")
                .build();
        this.executor = Executors.newCachedThreadPool(factory);

        this.routing.registerHook(RoutingEventKind.START_REPLICATION, new StartReplicationHook(this));
        this.routing.registerHook(RoutingEventKind.STOP_REPLICATION, new StopReplicationHook(this));
        this.routing.registerHook(RoutingEventKind.PRIMARY_OWNER_CHANGED, new PrimaryOwnerChangedHook(this));
    }

    /**
     * Returns a snapshot of currently running replications grouped by shard kind.
     *
     * @return map of shard kinds to lists of active shard IDs
     */
    public EnumMap<ShardKind, List<Integer>> listActiveReplicationsByShard() {
        EnumMap<ShardKind, List<Integer>> snapshot = new EnumMap<>(ShardKind.class);

        for (var e : replications.entrySet()) {
            List<Integer> active = e.getValue().entrySet().stream()
                    .filter(it -> it.getValue().isRunning())
                    .map(Map.Entry::getKey)
                    .toList();

            if (!active.isEmpty()) {
                snapshot.put(e.getKey(), active);
            }
        }
        return snapshot;
    }

    /**
     * Starts replication for a shard if this node is a standby.
     *
     * @param shardKind the shard kind (REDIS or BUCKET)
     * @param shardId   the shard identifier
     * @param explicit  if true, clears the suppression flag; if false, respects existing suppression
     * @throws NoRouteFoundException             if no route exists for the shard
     * @throws ReplicationAlreadyExistsException if replication is already running for the shard
     */
    public void startReplication(ShardKind shardKind, int shardId, boolean explicit) {
        Route route = routing.findRoute(shardKind, shardId);
        if (route == null) {
            throw new NoRouteFoundException(
                    "No route found for " + shardKind + "-" + shardId
            );
        }
        if (route.standbys().contains(context.getMember())) {
            rwlock.writeLock().lock();
            try {
                if (!explicit) {
                    Boolean suppressed = suppressionFlag.get(shardKind).get(shardId);
                    if (Objects.requireNonNullElse(suppressed, false)) {
                        return;
                    }
                } else {
                    suppressionFlag.get(shardKind).remove(shardId);
                }
                ReplicationWatchDog watchDog = replications.get(shardKind).get(shardId);
                if (watchDog != null) {
                    throw new ReplicationAlreadyExistsException(
                            "A replication watchdog is already registered for " + shardKind + "-" + shardId
                    );
                }
                VolumeConfig config = new VolumeConfigGenerator(context, shardKind, shardId).volumeConfig();
                VolumeReplication replication = new VolumeReplication(context, shardKind, shardId, config.dataDir());
                watchDog = new ReplicationWatchDog(shardKind, shardId, replication);
                replications.get(shardKind).put(shardId, watchDog);
                executor.submit(watchDog);
            } finally {
                rwlock.writeLock().unlock();
            }
        }
    }

    // Exposed with package-private visibility solely for test support.
    // The retry logic in ReplicationWatchDog must be validated by injecting
    // a ReplicationWatchDog instance with a mock ReplicationTask, which requires
    // controlled registration here. Not part of the service's public surface and
    // not intended for production use.
    void registerReplicationWatchDog(ShardKind shardKind, int shardId, ReplicationWatchDog watchDog) {
        rwlock.writeLock().lock();
        try {
            replications.get(shardKind).put(shardId, watchDog);
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    private void launchReplicationWorkers(ShardKind shardKind, int numberOfShards) {
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            try {
                startReplication(shardKind, shardId, false);
            } catch (ReplicationAlreadyExistsException | NoRouteFoundException exp) {
                LOGGER.debug("Skipping replication initialization for {}-{}: {}",
                        shardKind, shardId, exp.getMessage());
            }
        }
    }

    public void start() {
        for (ShardKind shardKind : ShardKind.values()) {
            if (shardKind.equals(ShardKind.REDIS)) {
                int numberOfShards = context.getConfig().getInt("redis.shards");
                launchReplicationWorkers(shardKind, numberOfShards);
            } else if (shardKind.equals(ShardKind.BUCKET)) {
                int numberOfShards = context.getConfig().getInt("bucket.shards");
                launchReplicationWorkers(shardKind, numberOfShards);
            }
        }
    }

    /**
     * Triggers reconnection for an active replication watchdog after a topology change.
     *
     * @param shardKind the shard kind
     * @param shardId   the shard identifier
     */
    public void reconnect(ShardKind shardKind, int shardId) {
        ReplicationWatchDog watchdog;

        rwlock.readLock().lock();
        try {
            watchdog = replications.get(shardKind).get(shardId);
        } finally {
            rwlock.readLock().unlock();
        }

        if (watchdog != null) {
            LOGGER.info("Topology change detected for shard {}-{}; reconnecting replication watchdog", shardKind, shardId);
            try {
                watchdog.reconnect();
            } catch (Throwable t) {
                LOGGER.error("Replication watchdog reconnect failed for shard {}-{}", shardKind, shardId, t);
            }
        } else {
            LOGGER.warn("Topology change detected for shard {}-{}, but no replication watchdog found; reconnect skipped", shardKind, shardId);
        }
    }

    /**
     * Stops replication for a shard and optionally suppresses automatic restart.
     *
     * @param shardKind  the shard kind
     * @param shardId    the shard identifier
     * @param suppressed if true, prevents automatic restart until explicitly started
     * @throws NoReplicationFoundException if no replication is running for the shard
     */
    public void stopReplication(ShardKind shardKind, int shardId, boolean suppressed) {
        ReplicationWatchDog watchdog;

        rwlock.writeLock().lock();
        try {
            watchdog = replications.get(shardKind).remove(shardId);
            if (watchdog == null) {
                throw new NoReplicationFoundException(String.format("No replication found for for %s-%d", shardKind, shardId));
            }
            suppressionFlag.get(shardKind).put(shardId, suppressed);
        } finally {
            rwlock.writeLock().unlock();
        }

        try {
            watchdog.stop();
        } catch (Exception exp) {
            LOGGER.error("Failed to gracefully stop replication for {}-{}",
                    shardKind, shardId, exp);
            throw exp;
        }
    }

    @Override
    public void shutdown() {
        for (var shardMap : replications.values()) {
            for (var entry : shardMap.entrySet()) {
                ReplicationWatchDog watchdog = entry.getValue();
                try {
                    watchdog.stop();
                } catch (Exception e) {
                    LOGGER.error("Failed to stop replication watchdog {}-{}",
                            watchdog.getShardKind(), watchdog.getShardId(), e);
                }
            }
        }
        for (var shardMap : replications.values()) {
            shardMap.clear();
        }
        if (!ExecutorServiceUtil.shutdownNowThenAwaitTermination(executor)) {
            LOGGER.warn("Replication service cannot be stopped gracefully");
        }
    }

    /**
     * Monitors and manages a single shard's replication task with retry logic.
     *
     * <p>The watchdog runs the replication task and retries on failure up to a configured
     * maximum. If failures occur faster than the reset threshold, the retry counter
     * increments; otherwise it resets, allowing recovery from transient issues.</p>
     */
    protected class ReplicationWatchDog implements Runnable {
        private final int MAX_RETRIES;
        private final long RETRY_INTERVAL;
        private final long RESET_THRESHOLD_NANOS;
        private final ShardKind shardKind;
        private final int shardId;
        private final ReplicationTask replication;
        private volatile boolean running;
        private volatile boolean stopped;

        ReplicationWatchDog(ShardKind shardKind, int shardId, ReplicationTask replication) {
            this.shardKind = shardKind;
            this.shardId = shardId;
            this.replication = replication;
            this.MAX_RETRIES = context.getConfig().getInt("volume.replication.max_retries");
            this.RETRY_INTERVAL = context.getConfig().getInt("volume.replication.retry_interval");
            this.RESET_THRESHOLD_NANOS = TimeUnit.SECONDS.toNanos(context.getConfig().getInt("volume.replication.reset_threshold"));
        }

        public ShardKind getShardKind() {
            return shardKind;
        }

        public int getShardId() {
            return shardId;
        }

        @Override
        public void run() {
            String volumeName = VolumeNames.format(shardKind, shardId);
            LOGGER.info("Starting replication on Volume={}", volumeName);
            int retry = 0;
            long startedAt = 0;
            while (!stopped) {
                try {
                    Route route = routing.findRoute(shardKind, shardId);
                    if (route == null || !route.standbys().contains(context.getMember())) {
                        stopReplication(shardKind, shardId, false);
                        break;
                    }
                    running = true;
                    startedAt = System.nanoTime();
                    replication.start();
                    return;
                } catch (Throwable t) {
                    if (stopped) {
                        return;
                    }

                    long elapsed = System.nanoTime() - startedAt;
                    if (elapsed > RESET_THRESHOLD_NANOS) {
                        retry = 0;
                    } else {
                        retry++;
                    }

                    if (retry > MAX_RETRIES) {
                        LOGGER.error("Replication permanently failed on Volume={} after {} rapid attempts",
                                volumeName, retry, t);
                        stopReplication(shardKind, shardId, false);
                        return;
                    }

                    LOGGER.error("Replication failed on Volume={} – retrying", volumeName, t);
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(RETRY_INTERVAL));
                } finally {
                    running = false;
                }
            }
        }

        public boolean isRunning() {
            return running;
        }

        public void reconnect() {
            replication.reconnect();
        }

        public void stop() {
            stopped = true;
            replication.shutdown();
            LOGGER.info("Stopped replication on Volume={}", VolumeNames.format(shardKind, shardId));
        }
    }
}