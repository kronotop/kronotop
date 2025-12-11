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

package com.kronotop.volume;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.KronotopService;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingEventKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.KeyWatcher;
import com.kronotop.journal.*;
import com.kronotop.server.CommandAlreadyRegisteredException;
import com.kronotop.server.ServerKind;
import com.kronotop.task.TaskService;
import com.kronotop.volume.handlers.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * VolumeService is a service class that handles the management of volumes within the Kronotop database system.
 * It extends CommandHandlerService and implements the KronotopService interface, providing methods to create, find,
 * close, and list volumes. The service ensures thread safety using a ReentrantReadWriteLock.
 */
public class VolumeService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Volume";
    private static final int DISUSED_PREFIXES_JOURNAL_BATCH_SIZE = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(VolumeService.class);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final HashMap<String, Volume> volumes = new HashMap<>();
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final ScheduledThreadPoolExecutor scheduler;
    private final Consumer disusedPrefixesConsumer;
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

        String consumerId = String.format("%s-member:%s", JournalName.DISUSED_PREFIXES.getValue(), context.getMember().getId());
        ConsumerConfig config = new ConsumerConfig(consumerId, JournalName.DISUSED_PREFIXES.getValue(), ConsumerConfig.Offset.RESUME);
        this.disusedPrefixesConsumer = new Consumer(context, config);

        handlerMethod(ServerKind.INTERNAL, new SegmentRangeHandler(this));
        handlerMethod(ServerKind.INTERNAL, new SegmentTailPointerHandler(this));
        handlerMethod(ServerKind.INTERNAL, new SegmentInsertHandler(this));
        handlerMethod(ServerKind.INTERNAL, new ChangeLogWatchHandler(this));
        handlerMethod(ServerKind.INTERNAL, new ChangeLogRangeHandler(this));
        handlerMethod(ServerKind.INTERNAL, new VolumeAdminHandler(this));
        handlerMethod(ServerKind.INTERNAL, new VolumeInspectHandler(this));

        routing.registerHook(RoutingEventKind.PRIMARY_OWNER_CHANGED, new SubmitVacuumTaskHook(this));
        routing.registerHook(RoutingEventKind.PRIMARY_OWNER_CHANGED, new StopVacuumTaskHook(this));

        resumeMarkStalePrefixesTaskIfAny();

        this.scheduler.scheduleAtFixedRate(new CleanupStaleSegmentsOnStandbyTask(), 1, 1, TimeUnit.HOURS);
    }

    public MutationWatcher mutationWatcher() {
        return mutationWatcher;
    }

    /**
     * Starts the VolumeService by executing a background task for monitoring and handling disused prefixes.
     * This method schedules the {@code DisusePrefixesWatcher} task for execution using the internal scheduler.
     * The watcher is responsible for observing specific journal triggers and
     * performing actions based on the retrieved journal metadata.
     * <p>
     * If the service is shut down, no further actions are initiated by the watcher.
     * The watcher reschedules itself upon completion unless the service has been shut down.
     */
    public void start() {
        disusedPrefixesConsumer.start();

        DisusedPrefixesWatcher disusedPrefixesWatcher = new DisusedPrefixesWatcher();
        scheduler.execute(disusedPrefixesWatcher);
        disusedPrefixesWatcher.waitUntilStarted();

        scheduler.scheduleAtFixedRate(new PeriodicDisusedPrefixesCheckerTask(), 5, 5, TimeUnit.MINUTES);
    }

    /**
     * Checks the existence of a directory specified by the given path and creates it if it does not exist.
     * If the path exists but is not a directory, or if an I/O error occurs during creation, an exception is thrown.
     *
     * @param dataDir the path of the directory to check and create if necessary
     * @throws KronotopException if the path exists but is not a directory, or if an I/O error occurs
     */
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
     * Submits a vacuum task for the specified volume if any pending vacuum task exists.
     * The method checks the metadata to determine if a vacuum operation is needed.
     * If the vacuum task is already completed, no further action is taken.
     *
     * @param volume the volume for which the vacuum task should be checked and submitted
     */
    protected void submitVacuumTaskIfAny(Volume volume) {
        if (!hasVolumeOwnership(volume)) {
            // ownership belongs to another cluster member
            return;
        }
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumMetadata vacuumMetadata = VacuumMetadata.load(tr, volume.getConfig().subspace());
            if (vacuumMetadata == null) {
                return;
            }
            TaskService taskService = context.getService(TaskService.NAME);
            VacuumTask task = new VacuumTask(context, volume, vacuumMetadata);
            LOGGER.debug("Submitting vacuum task {} for volume {}", task, volume.getConfig().name());
            taskService.execute(task);
        }
    }

    /**
     * Resumes MarkStalePrefixesTask for marking stale prefixes if it was previously started by the current member.
     * This method attempts to locate the metadata of the `MarkStalePrefixesTask` in the FoundationDB directory.
     * If it finds an entry that matches the current member's ID, it resumes executing the task.
     * <p>
     * The method:
     * - Establishes a transaction with the FoundationDB instance.
     * - Navigates the directory structure to locate metadata for the MarkStalePrefixesTask.
     * - Validates the member ID stored in the metadata and ensures it matches the current member's ID.
     * - Invokes the task execution using the `TaskService`.
     * - Logs relevant information upon resuming the task.
     * <p>
     * If the metadata directory does not exist or the metadata entry is not found, no action is taken.
     * This method safely ignores a `NoSuchDirectoryException` in the case where the required directory structure
     * for the `MarkStalePrefixesTask` does not exist.
     */
    private void resumeMarkStalePrefixesTaskIfAny() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KronotopDirectoryNode node = KronotopDirectory.
                    kronotop().
                    cluster(context.getClusterName()).
                    metadata().
                    tasks().
                    task(MarkStalePrefixesTask.NAME);
            try {
                DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, node.toList()).join();
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
                    LOGGER.info("Resuming " + MarkStalePrefixesTask.NAME);
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
     * Creates a new volume based on the provided configuration.
     * If a volume with the same name already exists and is not closed, it returns the existing volume.
     * Otherwise, it creates a new volume, ensuring the necessary directories are created.
     *
     * @param config the configuration for the volume to be created
     * @return the created volume
     * @throws IOException if an I/O error occurs during the creation of the directory or volume
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
            submitVacuumTaskIfAny(volume);
            volumes.put(config.name(), volume);
            return volume;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Finds and returns a volume by name if it exists and is open.
     *
     * @param name the name of the volume to be found
     * @return the volume with the specified name
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
     * Closes the specified volume by name, removing it from the managed volumes.
     * If the volume does not exist or is already closed, a VolumeNotOpenException is thrown.
     *
     * @param name the name of the volume to be closed
     * @throws VolumeNotOpenException if the volume with the specified name does not exist or is already closed
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
     * Retrieves a list of all managed volumes.
     * <p>
     * This method returns an unmodifiable list of the volumes currently managed by the service.
     * The list will reflect any changes made to the volumes at the time of retrieval.
     *
     * @return a list of all managed volumes
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

    public DirectorySubspace openSubspace(String volumeName) {
        return subspaceCache.computeIfAbsent(volumeName, ignored -> {
            VolumeNames.Parsed parsed = VolumeNames.parse(volumeName);
            VolumeConfigGenerator volumeConfigGenerator = new VolumeConfigGenerator(context, parsed.shardKind(), parsed.shardId());
            return volumeConfigGenerator.openVolumeSubspace();
        });
    }

    /**
     * Returns the volumeId for the given subspace, loading and caching it if necessary.
     *
     * @param subspace the directory subspace of the volume
     * @return the volumeId
     */
    public long getVolumeId(DirectorySubspace subspace) {
        return volumeIdCache.computeIfAbsent(subspace, s -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeMetadata metadata = VolumeMetadata.load(tr, s);
                return metadata.getVolumeId();
            }
        });
    }

    /**
     * Returns the mutation trigger key for the given subspace, computing and caching it if necessary.
     *
     * @param subspace the directory subspace of the volume
     * @return the mutation trigger key
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

    private void processDisusePrefix(Transaction tr, Event event) {
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
     * Processes events from the "disused-prefixes" journal and handles them accordingly.
     * This method continuously polls the journal for up to 100 events at a time and processes
     * each event by invoking {@code processDisusePrefix(Transaction tr, Event event)}.
     * <p>
     * The method operates within a transaction context provided by FoundationDB. Each event represents
     * a disused prefix in the system, and this method ensures that the prefix is properly cleared
     * from all volumes. The transaction commits successfully after processing each batch of events.
     * <p>
     * If no further events are available in the journal, the method terminates the polling loop.
     * <p>
     * Exceptions that occur during the prefix processing are caught and logged, but their occurrence
     * does not interrupt the batch processing of remaining events within the transaction.
     */
    private synchronized void fetchDisusedPrefixes() {
        boolean done = false;
        while (!done) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                for (int i = 0; i < DISUSED_PREFIXES_JOURNAL_BATCH_SIZE; i++) {
                    Event event = disusedPrefixesConsumer.consume(tr);
                    if (event == null) {
                        done = true;
                        break;
                    }
                    try {
                        processDisusePrefix(tr, event);
                        disusedPrefixesConsumer.markConsumed(tr, event);
                    } catch (Exception e) {
                        // Periodic task will try to process again, quit now.
                        done = true;
                        if (e instanceof IllegalConsumerStateException) {
                            throw e;
                        }
                        LOGGER.error("Failed to process a disused prefix", e);
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
                        fetchDisusedPrefixes();
                        return;
                    }
                }
                throw e;
            }
        }
    }

    /**
     * Shuts down the VolumeService, ensuring that all managed volumes are properly closed and cleared.
     * This method acquires a write lock to ensure thread safety during the shutdown process.
     * It iterates through all entries in the volumes map, closes each Volume, and then clears the map.
     */
    @Override
    public void shutdown() {
        try {
            isShutdown = true;
            disusedPrefixesConsumer.stop();
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

            if (!scheduler.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.warn("{} service cannot be stopped gracefully", NAME);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KronotopException("Operation was interrupted while waiting", e);
        }
    }

    /**
     * A periodic task that checks and processes disused prefixes in the system.
     * <p>
     * This task is designed to be executed as part of a scheduled background process in the containing
     * {@code VolumeService} class. It triggers the fetching and processing of disused prefixes by invoking
     * the {@code fetchDisusedPrefixes} method on the parent class.
     * <p>
     * The main operation of this task involves handling entries in the "disused-prefixes" journal, which keeps
     * track of prefixes that are no longer being used. Each disused prefix is processed and cleared from
     * the system. The task runs within a transactional context to ensure integrity and consistency during its operation.
     * <p>
     * This class implements the {@code Runnable} interface, making it suitable for execution by a thread or
     * a scheduled executor service.
     */
    private class PeriodicDisusedPrefixesCheckerTask implements Runnable {

        @Override
        public void run() {
            if (isShutdown) {
                return;
            }
            fetchDisusedPrefixes();
        }
    }

    /**
     * Represents a task for monitoring and handling disused prefixes in the system.
     * This class implements the {@link Runnable} interface and is designed to be executed
     * by a scheduler. It continuously watches a key in the "disused-prefixes" journal and
     * processes related events when triggered.
     * <p>
     * The {@code run()} method is the main entry point for the task execution, and it
     * performs the following actions:
     * <p>
     * - If the service is in a shutdown state, the method gracefully exits without taking
     * any further action.
     * - Creates a FoundationDB transaction and sets up a key watcher on the journal associated
     * with disused prefixes. The watcher is triggered when the associated key changes or
     * is deleted.
     * - Processes disused prefix events by invoking {@link VolumeService#fetchDisusedPrefixes}.
     * - Handles exceptions during the watching or processing stages by logging errors,
     * while ensuring that the task reschedules itself unless the service is shutdown.
     * <p>
     * In case of cancellation (e.g., a key watcher being explicitly cancelled), the method
     * handles it gracefully and terminates further processing loop execution.
     * <p>
     * This task ensures that disused prefixes are timely handled, maintaining proper
     * cleanup of resources and states in the system.
     */
    private class DisusedPrefixesWatcher implements Runnable {
        private final CountDownLatch latch = new CountDownLatch(1);

        public void waitUntilStarted() {
            try {
                latch.await(); // Wait until the watcher is started
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KronotopException("Operation was interrupted while waiting", e);
            }
        }

        @Override
        public void run() {
            if (isShutdown) {
                return;
            }
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                CompletableFuture<Void> watcher = keyWatcher.watch(tr, context.getJournal().getJournalMetadata(JournalName.DISUSED_PREFIXES.getValue()).trigger());
                tr.commit().join();
                try {
                    fetchDisusedPrefixes();
                    latch.countDown();
                    watcher.join();
                } catch (CancellationException e) {
                    LOGGER.debug("{} watcher has been cancelled", JournalName.DISUSED_PREFIXES);
                    return;
                }
                fetchDisusedPrefixes();
            } catch (Exception e) {
                LOGGER.error("Error while watching journal: {}", JournalName.DISUSED_PREFIXES, e);
            } finally {
                if (!isShutdown) {
                    scheduler.execute(this);
                }
            }
        }
    }

    /**
     * This task is responsible for cleaning up stale segments in volumes that are not owned by the
     * current member. It is designed to be executed as a background task within the context of
     * volume management.
     * <p>
     * The cleanup process iterates through all volumes and verifies ownership. If the volume is
     * owned by the current member, the task skips it, as cleaning stale segments for owned volumes
     * is handled by a separate vacuum task. For volumes not owned, the task invokes the necessary
     * operations to clean up stale segments.
     * <p>
     * The task is implemented as a {@code Runnable} and is designed to respect the shutdown state
     * of the service. If the service is in a shutdown state, the task terminates without performing
     * any action.
     * <p>
     * Exception handling is implemented to ensure issues during the cleanup do not interrupt the
     * processing of other volumes. All errors are logged for further analysis.
     * <p>
     * Thread-safety and consistency are maintained by periodically checking the shutdown state
     * during execution.
     */
    private class CleanupStaleSegmentsOnStandbyTask implements Runnable {

        @Override
        public void run() {
            if (isShutdown) {
                return;
            }
            try {
                for (Volume volume : list()) {
                    if (isShutdown) {
                        break;
                    }
                    if (hasVolumeOwnership(volume)) {
                        // If this member is the current owner of volume, cleaning up the stale segment
                        // must be handled by the vacuum task.
                        continue;
                    }
                    // potentially a time-consuming operation
                    volume.cleanupStaleSegments();
                }
            } catch (Exception e) {
                LOGGER.error("Error while cleaning up stale segments", e);
            }
        }
    }
}
