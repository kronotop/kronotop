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

package com.kronotop.volume;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.KronotopService;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.core.InfoCollector;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.KeyWatcher;
import com.kronotop.journal.*;
import com.kronotop.metrics.InfoReport;
import com.kronotop.server.CommandAlreadyRegisteredException;
import com.kronotop.server.ServerKind;
import com.kronotop.task.TaskService;
import com.kronotop.volume.handlers.*;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Owns the volumes opened on this member and registers the internal volume handlers.
 */
public class VolumeService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Volume";
    private static final int PREFIX_INVALIDATIONS_JOURNAL_BATCH_SIZE = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(VolumeService.class);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final HashMap<String, Volume> volumes = new HashMap<>();
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final ScheduledThreadPoolExecutor scheduler;
    private final Consumer prefixInvalidationsConsumer;
    private final RoutingService routing;
    private final ConcurrentHashMap<String, DirectorySubspace> subspaceCache;
    private final ConcurrentHashMap<DirectorySubspace, Long> volumeIdCache;
    private final ConcurrentHashMap<DirectorySubspace, byte[]> mutationTriggerKeyCache;
    private final MutationWatcher mutationWatcher;
    private volatile boolean isShutdown;

    public VolumeService(Context context) throws CommandAlreadyRegisteredException {
        super(context, NAME);

        this.routing = context.getService(RoutingService.NAME);
        assert routing != null;

        this.subspaceCache = new ConcurrentHashMap<>();
        this.volumeIdCache = new ConcurrentHashMap<>();
        this.mutationTriggerKeyCache = new ConcurrentHashMap<>();
        this.mutationWatcher = new MutationWatcher();

        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("kr.volume-%d").build();
        this.scheduler = new ScheduledThreadPoolExecutor(3, factory);

        String consumerId = String.format("%s-member:%s", JournalName.PREFIX_INVALIDATIONS.getValue(), context.getMember().getId());
        ConsumerConfig config = new ConsumerConfig(consumerId, JournalName.PREFIX_INVALIDATIONS.getValue(), ConsumerConfig.Offset.RESUME);
        this.prefixInvalidationsConsumer = new Consumer(context, config);

        handlerMethod(ServerKind.INTERNAL, new SegmentRangeHandler(this));
        handlerMethod(ServerKind.INTERNAL, new SegmentTailPointerHandler(this));
        handlerMethod(ServerKind.INTERNAL, new SegmentInsertHandler(this));
        handlerMethod(ServerKind.INTERNAL, new ChangeLogWatchHandler(this));
        handlerMethod(ServerKind.INTERNAL, new ChangeLogRangeHandler(this));
        handlerMethod(ServerKind.INTERNAL, new VolumeAdminHandler(this));
        handlerMethod(ServerKind.INTERNAL, new VolumeInspectHandler(this));
        handlerMethod(ServerKind.INTERNAL, new VolumeStatsHandler(this));

        resumeMarkStalePrefixesTaskIfAny();
    }

    private static @NonNull String prepareVolumeSection(Volume volume) {
        VolumeStats stats = volume.getStats();
        return "name=" + volume.getConfig().name() +
                ",status=" + volume.getStatus() +
                ",vacuum_active=" + (volume.isVacuumActive() ? 1 : 0) +
                ",appends=" + stats.getAppends() +
                ",deletes=" + stats.getDeletes() +
                ",updates=" + stats.getUpdates() +
                ",gets=" + stats.getGets() +
                ",bytes_appended=" + stats.getBytesAppended() +
                ",bytes_read=" + stats.getBytesRead() +
                ",segments_created=" + stats.getSegmentsCreated();
    }

    public MutationWatcher mutationWatcher() {
        return mutationWatcher;
    }

    /**
     * Starts the prefix-invalidations consumer, its journal watcher, and the periodic fallback check.
     * Returns after the watcher has drained the journal once.
     */
    @Override
    public void start() {
        prefixInvalidationsConsumer.start();

        PrefixInvalidationsWatcher prefixInvalidationsWatcher = new PrefixInvalidationsWatcher();
        scheduler.execute(prefixInvalidationsWatcher);
        prefixInvalidationsWatcher.waitUntilStarted();

        scheduler.scheduleAtFixedRate(new PeriodicPrefixInvalidationsCheckerTask(), 5, 5, TimeUnit.MINUTES);
    }

    private void checkAndCreateDataDir(String dataDir) {
        try {
            Files.createDirectories(Paths.get(dataDir));
        } catch (FileAlreadyExistsException e) {
            LOGGER.error("{} already exists but is not a directory", dataDir, e);
            throw new KronotopException(e);
        } catch (IOException e) {
            LOGGER.error("{} could not be created", dataDir, e);
            throw new KronotopException(e);
        }
    }

    /**
     * Resumes {@link MarkStalePrefixesTask} on startup when the owner stored in its task metadata is this member.
     * The task continues from the last prefix it persisted. Missing task metadata means there is nothing to resume.
     */
    private void resumeMarkStalePrefixesTaskIfAny() {
        try (Transaction tr = context.getTransactionManager().create()) {
            KronotopDirectoryNode node = KronotopDirectory.
                    kronotop().
                    cluster(context.getClusterName()).
                    metadata().
                    tasks().
                    task(MarkStalePrefixesTask.NAME);
            try {
                DirectorySubspace subspace = context.getDirectoryLayer().open(tr, node.toList()).join();
                byte[] value = tr.get(subspace.pack(MarkStalePrefixesTask.METADATA_KEY.MEMBER_ID.name())).join();
                if (value == null) {
                    // This should not be happened
                    return;
                }
                String memberId = new String(value);
                if (context.getMember().getId().equals(memberId)) {
                    TaskService taskService = context.getService(TaskService.NAME);
                    MarkStalePrefixesTask task = new MarkStalePrefixesTask(context);
                    taskService.execute(task);
                    LOGGER.info("Resuming {}", MarkStalePrefixesTask.NAME);
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
     * Opens a volume for the given config and creates its data directory if needed.
     * Returns the existing volume when one with the same name is already open.
     *
     * @throws IOException if the data directory or the volume cannot be created
     */
    public Volume newVolume(VolumeConfig config) throws IOException {
        lock.writeLock().lock();
        try {
            if (volumes.containsKey(config.name())) {
                Volume volume = volumes.get(config.name());
                if (!volume.isClosed()) {
                    // Already have the volume
                    return volume;
                }
            }
            // Create a new volume.
            checkAndCreateDataDir(config.dataDir());
            // TODO: Create folders for this specific volume in the constructor.
            Volume volume = new Volume(context, config);
            volumes.put(config.name(), volume);
            return volume;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @throws ClosedVolumeException  if the volume is closed
     * @throws VolumeNotOpenException if the volume does not exist
     */
    public Volume findVolume(String name) {
        lock.readLock().lock();
        try {
            if (volumes.containsKey(name)) {
                Volume volume = volumes.get(name);
                if (volume.isClosed()) {
                    throw new ClosedVolumeException(name);
                }
                return volume;
            }
            throw new VolumeNotOpenException(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Closes the volume and removes it from the service.
     *
     * @throws VolumeNotOpenException if the volume does not exist
     */
    public void closeVolume(String name) {
        lock.writeLock().lock();
        try {
            Volume volume = volumes.get(name);
            if (volume != null) {
                volume.close();
                volumes.remove(name);
                return;
            }
            throw new VolumeNotOpenException(name);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns an unmodifiable snapshot of the volumes, including closed ones not yet removed.
     */
    public List<Volume> list() {
        lock.readLock().lock();
        try {
            // Returns an unmodifiable list
            return volumes.values().stream().toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void collectInfo(InfoCollector collector) {
        List<Volume> open = list().stream()
                .filter(volume -> !volume.isClosed())
                .sorted(Comparator.comparing(volume -> volume.getConfig().name()))
                .toList();
        collector.put(InfoReport.VOLUME_SECTION, "volume_count", open.size());
        int index = 0;
        for (Volume volume : open) {
            String line = prepareVolumeSection(volume);
            collector.put(InfoReport.VOLUME_SECTION, "volume" + index, line);
            index++;
        }
    }

    public DirectorySubspace openSubspace(String volumeName) {
        return subspaceCache.computeIfAbsent(volumeName, ignored -> {
            VolumeNames.Parsed parsed = VolumeNames.parse(volumeName);
            VolumeConfigGenerator volumeConfigGenerator = new VolumeConfigGenerator(context, parsed.shardKind(), parsed.shardId());
            return volumeConfigGenerator.openVolumeSubspace();
        });
    }

    /**
     * Reads the volume id from FoundationDB on first use and caches it per subspace.
     */
    public long getVolumeId(DirectorySubspace subspace) {
        return volumeIdCache.computeIfAbsent(subspace, ds -> {
            try (Transaction tr = context.getTransactionManager().create()) {
                return VolumeMetadataUtil.readVolumeId(tr, new VolumeSubspace(ds));
            }
        });
    }

    /**
     * Computes the mutation trigger key on first use and caches it per subspace.
     */
    public byte[] getMutationTriggerKey(DirectorySubspace subspace) {
        return mutationTriggerKeyCache.computeIfAbsent(subspace, VolumeUtil::computeMutationTriggerKey);
    }

    public boolean hasVolumeOwnership(Volume volume) {
        Integer shardId = volume.getAttribute(VolumeAttributes.SHARD_ID);
        if (shardId == null) {
            return false;
        }
        ShardKind shardKind = volume.getAttribute(VolumeAttributes.SHARD_KIND);
        Route route = routing.findRoute(shardKind, shardId);
        return route.primary().equals(context.getMember());
    }

    private void processPrefixInvalidation(Transaction tr, Event event) {
        byte[] data = JSONUtil.readValue(event.value(), byte[].class);
        Prefix prefix = Prefix.fromBytes(data);
        VolumeSession session = new VolumeSession(tr, prefix);

        lock.readLock().lock();
        try {
            for (Volume volume : volumes.values()) {
                if (hasVolumeOwnership(volume)) {
                    volume.clearPrefix(session);
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Drains the prefix-invalidations journal. Each transaction consumes up to 100 events and clears
     * their prefixes from the volumes this member owns. A failed event stops the drain; the periodic
     * task retries it later. A commit conflict restarts the drain.
     */
    private synchronized void fetchPrefixInvalidations() {
        boolean done = false;
        while (!done) {
            try (Transaction tr = context.getTransactionManager().create()) {
                for (int i = 0; i < PREFIX_INVALIDATIONS_JOURNAL_BATCH_SIZE; i++) {
                    Event event = prefixInvalidationsConsumer.consume(tr);
                    if (event == null) {
                        done = true;
                        break;
                    }
                    try {
                        processPrefixInvalidation(tr, event);
                        prefixInvalidationsConsumer.markConsumed(tr, event);
                    } catch (Exception e) {
                        // Periodic task will try to process again, quit now.
                        done = true;
                        if (e instanceof IllegalConsumerStateException) {
                            throw e;
                        }
                        LOGGER.error("Failed to process a prefix invalidation", e);
                        break;
                    }
                }
                tr.commit().join();
            } catch (IllegalConsumerStateException e) {
                // Ignore this exception, this is mostly about the concurrency in the integration tests.
                break;
            } catch (CompletionException e) {
                if (e.getCause() instanceof FDBException ex) {
                    if (ex.getCode() == 1020) {
                        // 1020 -> not_committed - Transaction not committed due to conflict with another transaction
                        fetchPrefixInvalidations();
                        return;
                    }
                }
                throw e;
            }
        }
    }

    /**
     * Stops the journal consumer, the watcher, and the scheduler, then closes every volume.
     */
    @Override
    public void shutdown() {
        try {
            isShutdown = true;
            prefixInvalidationsConsumer.stop();
            keyWatcher.unwatchAll();
            scheduler.shutdownNow();

            lock.writeLock().lock();
            try {
                for (Map.Entry<String, Volume> entry : volumes.entrySet()) {
                    entry.getValue().close();
                }
                volumes.clear();
            } finally {
                lock.writeLock().unlock();
            }

            if (!scheduler.awaitTermination(
                    ExecutorServiceUtil.DEFAULT_TIMEOUT,
                    ExecutorServiceUtil.DEFAULT_TIMEOUT_TIMEUNIT
            )) {
                LOGGER.warn("{} service cannot be stopped gracefully", NAME);
            }
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException("Operation was interrupted while waiting", exp);
        }
    }

    /**
     * Fallback drain of the prefix-invalidations journal. Runs every 5 minutes and picks up events
     * the watcher missed or failed to process.
     */
    private class PeriodicPrefixInvalidationsCheckerTask implements Runnable {

        @Override
        public void run() {
            if (isShutdown) {
                return;
            }
            fetchPrefixInvalidations();
        }
    }

    /**
     * Watches the trigger key of the prefix-invalidations journal and drains the journal on every change.
     * Reschedules itself after each run until the service shuts down.
     */
    private class PrefixInvalidationsWatcher implements Runnable {
        private final CountDownLatch latch = new CountDownLatch(1);

        public void waitUntilStarted() {
            try {
                latch.await(); // Wait until the watcher is started
            } catch (InterruptedException exp) {
                Thread.currentThread().interrupt();
                throw new KronotopException("Operation was interrupted while waiting", exp);
            }
        }

        @Override
        public void run() {
            if (isShutdown) {
                return;
            }
            try (Transaction tr = context.getTransactionManager().create()) {
                JournalService journal = context.getService(JournalService.NAME);
                CompletableFuture<Void> watcher = keyWatcher.watch(tr, journal.getJournalMetadata(JournalName.PREFIX_INVALIDATIONS.getValue()).trigger());
                tr.commit().join();
                try {
                    fetchPrefixInvalidations();
                    latch.countDown();
                    watcher.join();
                } catch (CancellationException e) {
                    LOGGER.debug("{} watcher has been cancelled", JournalName.PREFIX_INVALIDATIONS);
                    return;
                }
                fetchPrefixInvalidations();
            } catch (Exception e) {
                LOGGER.error("Error while watching journal: {}", JournalName.PREFIX_INVALIDATIONS, e);
            } finally {
                if (!isShutdown) {
                    scheduler.execute(this);
                }
            }
        }
    }
}
