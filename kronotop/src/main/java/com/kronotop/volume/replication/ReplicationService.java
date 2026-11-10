/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import java.util.function.Supplier;

/**
 * Manages volume replication on standby members. It runs one replication watchdog per shard and
 * reacts to the routing events that start, stop or reconnect a replication.
 */
public class ReplicationService extends BaseKronotopService implements KronotopService {
    public static final String NAME = "Replication";
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationService.class);
    private final RoutingService routing;
    private final ExecutorService executor;

    private final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
    private final Map<Integer, ReplicationWatchDog> replications = new HashMap<>();
    private final Map<Integer, Boolean> suppressionFlag = new HashMap<>();

    /**
     * Creates the service and registers its routing event hooks.
     *
     * @param context the context of the member
     */
    public ReplicationService(Context context) {
        super(context, NAME);
        this.routing = context.getService(RoutingService.NAME);
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("kr.volume-replication-%d")
                .build();
        this.executor = Executors.newCachedThreadPool(factory);

        this.routing.registerHook(RoutingEventKind.START_REPLICATION, new StartReplicationHook(this));
        this.routing.registerHook(RoutingEventKind.STOP_REPLICATION, new StopReplicationHook(this));
        this.routing.registerHook(RoutingEventKind.PRIMARY_OWNER_CHANGED, new PrimaryOwnerChangedHook(this));
    }

    /**
     * Returns the IDs of the shards that have a running replication.
     *
     * @return the shard IDs, empty if no replication is running
     */
    public List<Integer> listActiveReplications() {
        rwlock.readLock().lock();
        try {
            return replications.entrySet().stream()
                    .filter(it -> it.getValue().isRunning())
                    .map(Map.Entry::getKey)
                    .toList();
        } finally {
            rwlock.readLock().unlock();
        }
    }

    /**
     * Starts the replication of a shard if this member is a standby of it.
     *
     * @param shardId  the shard ID
     * @param explicit true to clear the suppression flag, false to skip the start when the
     *                 replication is suppressed
     * @throws NoRouteFoundException             if the shard has no route
     * @throws ReplicationAlreadyExistsException if the shard already has a replication
     */
    public void startReplication(int shardId, boolean explicit) {
        Route route = routing.findRoute(shardId);
        if (route == null) {
            throw new NoRouteFoundException(
                    "No route found for shard " + shardId
            );
        }
        if (route.standbys().contains(context.getMember())) {
            rwlock.writeLock().lock();
            try {
                if (!explicit) {
                    Boolean suppressed = suppressionFlag.get(shardId);
                    if (Objects.requireNonNullElse(suppressed, false)) {
                        return;
                    }
                } else {
                    suppressionFlag.remove(shardId);
                }
                ReplicationWatchDog watchDog = replications.get(shardId);
                if (watchDog != null) {
                    throw new ReplicationAlreadyExistsException(
                            "A replication watchdog is already registered for shard " + shardId
                    );
                }
                VolumeConfig config = new VolumeConfigGenerator(context, shardId).volumeConfig();
                Supplier<ReplicationTask> factory = () -> new VolumeReplication(context, shardId, config.dataDir());
                watchDog = new ReplicationWatchDog(shardId, factory);
                replications.put(shardId, watchDog);
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
    void registerReplicationWatchDog(int shardId, ReplicationWatchDog watchDog) {
        // TODO: Unused in production
        rwlock.writeLock().lock();
        try {
            replications.put(shardId, watchDog);
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    @Override
    public void start() {
        for (int shardId : context.getShardRegistry().getShardIds()) {
            try {
                // Launch replication workers
                startReplication(shardId, false);
            } catch (ReplicationAlreadyExistsException | NoRouteFoundException exp) {
                LOGGER.trace("Skipping replication initialization for shard {}: {}",
                        shardId, exp.getMessage());
            }
        }
    }

    /**
     * Reconnects the replication of a shard after a topology change. Does nothing if the shard
     * has no replication.
     *
     * @param shardId the shard ID
     */
    public void reconnect(int shardId) {
        ReplicationWatchDog watchdog;

        rwlock.readLock().lock();
        try {
            watchdog = replications.get(shardId);
        } finally {
            rwlock.readLock().unlock();
        }

        if (watchdog != null) {
            LOGGER.info("Topology change detected for shard {}; reconnecting replication watchdog", shardId);
            try {
                watchdog.reconnect();
            } catch (Throwable t) {
                LOGGER.error("Replication watchdog reconnect failed for shard {}", shardId, t);
            }
        } else {
            LOGGER.warn("Topology change detected for shard {}, but no replication watchdog found; reconnect skipped", shardId);
        }
    }

    /**
     * Stops the replication of a shard.
     *
     * @param shardId    the shard ID
     * @param suppressed true to block automatic restart until an explicit start
     * @throws NoReplicationFoundException if the shard has no replication
     */
    public void stopReplication(int shardId, boolean suppressed) {
        ReplicationWatchDog watchdog;

        rwlock.writeLock().lock();
        try {
            watchdog = replications.remove(shardId);
            if (watchdog == null) {
                throw new NoReplicationFoundException(String.format("No replication found for shard %d", shardId));
            }
            suppressionFlag.put(shardId, suppressed);
        } finally {
            rwlock.writeLock().unlock();
        }

        try {
            watchdog.stop();
        } catch (Exception exp) {
            LOGGER.error("Failed to gracefully stop replication for shard {}",
                    shardId, exp);
            throw exp;
        }
    }

    // Called when a watchdog ends on its own, so there is nothing to stop. The entry is removed
    // only if it still maps to the given watchdog, which keeps a concurrent restart safe.
    private void deregister(int shardId, ReplicationWatchDog watchDog) {
        rwlock.writeLock().lock();
        try {
            replications.remove(shardId, watchDog);
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    @Override
    public void shutdown() {
        List<ReplicationWatchDog> watchdogs;

        rwlock.writeLock().lock();
        try {
            watchdogs = new ArrayList<>(replications.values());
            replications.clear();
        } finally {
            rwlock.writeLock().unlock();
        }

        for (ReplicationWatchDog watchdog : watchdogs) {
            try {
                watchdog.stop();
            } catch (Exception e) {
                LOGGER.error("Failed to stop replication watchdog for shard {}",
                        watchdog.getShardId(), e);
            }
        }

        if (!ExecutorServiceUtil.shutdownNowThenAwaitTermination(executor)) {
            LOGGER.warn("Replication service cannot be stopped gracefully");
        }
    }

    /**
     * Runs the replication task of a shard and retries it on failure. It stops the replication
     * when the task fails faster than the reset threshold more times than the retry limit.
     */
    protected class ReplicationWatchDog implements Runnable {
        private final int MAX_RETRIES;
        private final long RETRY_INTERVAL;
        private final long RESET_THRESHOLD_NANOS;
        private final int shardId;
        private final Supplier<ReplicationTask> taskFactory;
        private volatile ReplicationTask currentTask;
        private volatile boolean running;
        private volatile boolean stopped;

        ReplicationWatchDog(int shardId, Supplier<ReplicationTask> taskFactory) {
            this.shardId = shardId;
            this.taskFactory = taskFactory;
            this.MAX_RETRIES = context.getConfig().getInt("volume.replication.max_retries");
            this.RETRY_INTERVAL = context.getConfig().getInt("volume.replication.retry_interval");
            this.RESET_THRESHOLD_NANOS = TimeUnit.SECONDS.toNanos(context.getConfig().getInt("volume.replication.reset_threshold"));
        }

        public int getShardId() {
            return shardId;
        }

        @Override
        public void run() {
            String volumeName = VolumeNames.format(shardId);
            LOGGER.info("Starting replication on Volume={}", volumeName);
            int retry = 0;
            long startedAt = 0;
            while (!stopped) {
                ReplicationTask task = null;
                try {
                    Route route = routing.findRoute(shardId);
                    if (route == null || !route.standbys().contains(context.getMember())) {
                        try {
                            stopReplication(shardId, false);
                        } catch (NoReplicationFoundException ignored) {
                        }
                        break;
                    }
                    startedAt = System.nanoTime();
                    task = taskFactory.get();
                    currentTask = task;
                    running = true;
                    // stop() may have run between taskFactory.get() and publishing currentTask,
                    // in which case it saw a null currentTask and never shut this task down.
                    // Re-check after publishing: stop() writes stopped before reading currentTask,
                    // we write currentTask before reading stopped, so at least one side observes
                    // the other (both volatile). Otherwise task.start() would run past a stop request.
                    if (stopped) {
                        task.shutdown();
                        return;
                    }
                    task.start();
                    deregister(shardId, this);
                    return;
                } catch (Throwable t) {
                    if (stopped) {
                        return;
                    }

                    if (task != null) {
                        try {
                            task.shutdown();
                        } catch (Exception ignored) {
                        }
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
                        try {
                            stopReplication(shardId, false);
                        } catch (NoReplicationFoundException ignored) {
                        }
                        return;
                    }

                    LOGGER.error("Replication failed on Volume={} – retrying", volumeName, t);
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(RETRY_INTERVAL));
                } finally {
                    currentTask = null;
                    running = false;
                }
            }
        }

        public boolean isRunning() {
            return running;
        }

        public void reconnect() {
            ReplicationTask task = currentTask;
            if (task != null) {
                task.reconnect();
            }
        }

        public void stop() {
            stopped = true;
            ReplicationTask task = currentTask;
            if (task != null) {
                task.shutdown();
            }
            LOGGER.info("Stopped replication on Volume={}", VolumeNames.format(shardId));
        }
    }
}