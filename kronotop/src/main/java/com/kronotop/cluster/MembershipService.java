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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.BaseKronotopService;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.KronotopService;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.*;
import com.kronotop.journal.*;
import io.github.resilience4j.retry.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * MembershipService manages cluster membership, health monitoring, and failure detection in Kronotop.
 *
 * <p>This service is responsible for:</p>
 * <ul>
 *   <li>Member registration and lifecycle management (registration, updates, removal)</li>
 *   <li>Heartbeat generation and monitoring for liveness detection</li>
 *   <li>Failure detection using heartbeat timeouts</li>
 *   <li>Cluster event distribution (member join/leave events)</li>
 *   <li>Member status tracking (RUNNING, STOPPED, UNAVAILABLE)</li>
 * </ul>
 *
 * <p><b>Architecture:</b></p>
 * <p>The service uses three concurrent background tasks:</p>
 * <ul>
 *   <li>{@link HeartbeatTask}: Periodically updates this member's heartbeat timestamp in FoundationDB</li>
 *   <li>{@link FailureDetectionTask}: Monitors other members' heartbeats and marks them as dead/alive</li>
 *   <li>{@link ClusterEventsJournalWatcher}: Watches for cluster events (joins/leaves) and updates local state</li>
 * </ul>
 *
 * <p><b>Failure Detection:</b></p>
 * <p>Members are considered dead if they fail to update their heartbeat within {@code heartbeat.maximum_silent_period}.
 * The failure detector tracks expected vs actual heartbeat counts and can detect both member failures and recoveries.</p>
 *
 * <p><b>Thread Safety:</b></p>
 * <p>This service is thread-safe. Member views and subspaces are stored in concurrent maps and updated atomically.</p>
 *
 * @see MemberRegistry
 * @see MemberView
 * @see Member
 */
public class MembershipService extends BaseKronotopService implements KronotopService {
    public static final String NAME = "Membership";

    /**
     * Byte array representing value 1 in little-endian format for atomic increment operations.
     */
    private static final byte[] PLUS_ONE = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian

    private static final Logger LOGGER = LoggerFactory.getLogger(MembershipService.class);

    /**
     * Application context providing access to core services and configuration.
     */
    private final Context context;

    /**
     * Registry managing member persistence and retrieval from FoundationDB.
     */
    private final MemberRegistry registry;

    /**
     * Scheduler for background tasks (heartbeat, failure detection, event watching).
     */
    private final ScheduledThreadPoolExecutor scheduler;

    private final ExecutorService executor;

    /**
     * Watcher for monitoring key changes in FoundationDB.
     */
    private final KeyWatcher keyWatcher = new KeyWatcher();

    /**
     * Mapping of members to their FoundationDB directory subspaces for heartbeat storage.
     */
    private final ConcurrentHashMap<Member, DirectorySubspace> subspaces = new ConcurrentHashMap<>();

    /**
     * Local view of other cluster members and their health status.
     */
    private final ConcurrentHashMap<Member, MemberView> others = new ConcurrentHashMap<>();

    /**
     * Consumer for cluster events journal (member join/leave notifications).
     */
    private final Consumer clusterEventsConsumer;

    /**
     * Interval in seconds between heartbeat updates (from cluster.heartbeat.interval config).
     */
    private final int heartbeatInterval;

    /**
     * Maximum silent period in seconds before a member is considered dead (from cluster.heartbeat.maximum_silent_period config).
     */
    private final int heartbeatMaximumSilentPeriod;

    /**
     * Flag indicating whether the service is shutting down.
     */
    private volatile boolean shutdown;

    /**
     * Constructs a new MembershipService with the given context.
     *
     * <p>Initializes the member registry, cluster events consumer, and scheduler for background tasks.
     * The scheduler is configured with 3 threads to run:
     * <ul>
     *   <li>Heartbeat generation task</li>
     *   <li>Failure detection task</li>
     *   <li>Cluster events journal watcher</li>
     * </ul>
     *
     * @param context the application context providing access to configuration and services
     */
    public MembershipService(Context context) {
        super(context, NAME);

        this.context = context;
        this.registry = new MemberRegistry(context);
        this.heartbeatInterval = context.getConfig().getInt("cluster.heartbeat.interval");
        this.heartbeatMaximumSilentPeriod = context.getConfig().getInt("cluster.heartbeat.maximum_silent_period");

        String consumerId = String.format("%s-member:%s", JournalName.CLUSTER_EVENTS.getValue(), context.getMember().getId());
        ConsumerConfig config = new ConsumerConfig(consumerId, JournalName.CLUSTER_EVENTS.getValue(), ConsumerConfig.Offset.LATEST);
        this.clusterEventsConsumer = new Consumer(context, config);

        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("kr.membership-%d").build();
        this.scheduler = new ScheduledThreadPoolExecutor(2, factory);
        this.executor = Executors.newSingleThreadExecutor(factory);
    }

    /**
     * Opens the FoundationDB directory subspace for a given member.
     *
     * @param tr     the transaction to use for directory operations
     * @param member the member whose directory subspace should be opened
     * @return the directory subspace for the member
     */
    private DirectorySubspace openMemberSubspace(Transaction tr, Member member) {
        KronotopDirectoryNode directory = registry.getDirectoryNode(member.getId());
        return DirectoryLayer.getDefault().open(tr, directory.toList()).join();
    }

    /**
     * Initializes the internal state by loading all RUNNING members from the registry.
     *
     * <p>For each RUNNING member, this method:
     * <ul>
     *   <li>Opens their directory subspace</li>
     *   <li>Retrieves their current heartbeat value</li>
     *   <li>Creates a MemberView to track their health</li>
     *   <li>Caches the subspace for efficient heartbeat monitoring</li>
     * </ul>
     */
    private void initializeInternalState() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (Member member : registry.listMembers(tr)) {
                if (!member.getStatus().equals(MemberStatus.RUNNING)) {
                    continue;
                }
                DirectorySubspace subspace = openMemberSubspace(tr, member);
                long heartbeat = Heartbeat.get(tr, subspace);
                MemberView memberView = new MemberView(heartbeat);
                others.put(member, memberView);
                subspaces.put(member, subspace);
            }
        }
    }

    /**
     * Starts the MembershipService and all background monitoring tasks.
     *
     * <p><b>Startup sequence:</b></p>
     * <ol>
     *   <li>Sets this member's status to RUNNING</li>
     *   <li>Registers the member in the registry (or updates if already registered)</li>
     *   <li>Validates that no duplicate member is already running (prevents split-brain)</li>
     *   <li>Initializes internal state by loading existing RUNNING members</li>
     *   <li>Publishes a MemberJoinEvent to the cluster events journal</li>
     *   <li>Starts the cluster events consumer</li>
     *   <li>Launches three background tasks: ClusterEventsJournalWatcher, HeartbeatTask, FailureDetectionTask</li>
     * </ol>
     *
     * <p><b>Duplicate member detection:</b></p>
     * <p>If a member with the same ID is already RUNNING, this method will throw {@link MemberAlreadyRunningException}
     * unless {@code cluster.force_initialization=true} is set in the configuration. This safety check prevents
     * accidental duplicate member registration that could cause cluster inconsistencies.</p>
     *
     * @throws MemberAlreadyRunningException if a member with the same ID is already in RUNNING status
     */
    public void start() {
        Member member = context.getMember();
        member.setStatus(MemberStatus.RUNNING);

        if (!registry.isAdded(member.getId())) {
            registry.add(member);
            LOGGER.info("Member: {} has been registered", member.getId());
        } else {
            // We have a member with this id, check the member's status.
            Member registeredMember = registry.findMember(member.getId());
            LOGGER.debug("The previous status is {}", registeredMember.getStatus());
            if (registeredMember.getStatus().equals(MemberStatus.RUNNING)) {
                if (context.getConfig().hasPath("cluster.force_initialization") &&
                        context.getConfig().getBoolean("cluster.force_initialization")) {
                    return;
                }
                LOGGER.warn("Kronotop failed at initialization because Member: {} is already in {} status", member.getId(), MemberStatus.RUNNING);
                LOGGER.warn("If this member is suspected to be dead or not gracefully stopped, you can set its status to UNAVAILABLE.");
                LOGGER.warn("You can also set the JVM property -Dcluster.force_initialization=true and start the process again, but this can be risky.");
                throw new MemberAlreadyRunningException(member.getId());
            }

            registry.update(member);
        }

        initializeInternalState();

        // Publish a MemberJoinEvent
        context.getJournal().getPublisher().publish(JournalName.CLUSTER_EVENTS, new MemberJoinEvent(member));
        clusterEventsConsumer.start();

        ClusterEventsJournalWatcher eventsJournalWatcher = new ClusterEventsJournalWatcher();
        executor.submit(eventsJournalWatcher);
        eventsJournalWatcher.waitUntilStarted();

        scheduler.submit(new HeartbeatTask());
        scheduler.submit(new FailureDetectionTask());
    }

    /**
     * Retrieves the latest heartbeat timestamp for a given member.
     *
     * @param member the member whose latest heartbeat timestamp is to be retrieved
     * @return the latest heartbeat timestamp of the member, or 0 if the member is not found
     */
    public long getLatestHeartbeat(Member member) {
        MemberView memberView = others.get(member);
        if (memberView == null) {
            return 0;
        }
        return memberView.getLatestHeartbeat();
    }

    /**
     * Retrieves a sorted set of members. Members are sorted by their process id.
     *
     * @return a TreeSet containing Member objects sorted by their process IDs.
     */
    public TreeSet<Member> listMembers() {
        return registry.listMembers();
    }

    /**
     * Finds and returns the Member object associated with the specified member ID.
     *
     * @param memberId the unique identifier of the member to be retrieved
     * @return the Member object associated with the specified member ID
     */
    public Member findMember(String memberId) {
        return registry.findMember(memberId);
    }

    /**
     * Finds and returns the Member object associated with the specified member ID.
     *
     * @param tr       the transaction within which the member lookup is to be performed
     * @param memberId the unique identifier of the member to be retrieved
     * @return the Member object associated with the specified member ID
     */
    public Member findMember(Transaction tr, String memberId) {
        return registry.findMember(tr, memberId);
    }

    /**
     * Checks whether a member is registered in the system.
     *
     * @param memberId the unique identifier of the member to be checked
     * @return true if the member is registered, false otherwise
     */
    public boolean isMemberRegistered(String memberId) {
        return registry.isAdded(memberId);
    }

    /**
     * Updates the information of a specified member in the registry within the given transaction.
     *
     * <p>This method persists the member's updated state to FoundationDB. Changes include
     * status updates, address changes, or any other member metadata modifications.</p>
     *
     * @param tr     the transaction to use for the update operation
     * @param member the Member object containing the updated information
     */
    public void updateMember(Transaction tr, Member member) {
        registry.update(tr, member);
    }

    /**
     * Removes a member from the registry if their status is not RUNNING.
     *
     * <p>This is a safety measure to prevent accidental removal of active members. Members must be
     * in STOPPED or UNAVAILABLE status before they can be removed from the cluster.</p>
     *
     * @param tr       the transaction object used for querying and removing the member
     * @param memberId the unique identifier of the member to be removed
     * @throws KronotopException if the member is in RUNNING status
     */
    public void removeMember(Transaction tr, String memberId) {
        Member member = registry.findMember(tr, memberId);
        if (member.getStatus().equals(MemberStatus.RUNNING)) {
            throw new KronotopException("Member in " + MemberStatus.RUNNING + " status cannot be removed");
        }
        registry.remove(tr, memberId);
    }

    /**
     * Retrieves a read-only view of the other members in the cluster and their health status.
     *
     * <p>The returned map contains all RUNNING members in the cluster except the current member.
     * Each MemberView provides information about:
     * <ul>
     *   <li>Latest heartbeat timestamp</li>
     *   <li>Expected heartbeat count</li>
     *   <li>Alive status (whether the member is responding to heartbeat checks)</li>
     * </ul>
     *
     * @return an unmodifiable map where the keys are Member objects and the values are MemberView objects
     */
    public Map<Member, MemberView> getOthers() {
        return Collections.unmodifiableMap(others);
    }

    /**
     * Shuts down the MembershipService gracefully.
     *
     * <p><b>Shutdown sequence:</b></p>
     * <ol>
     *   <li>Sets the shutdown flag to stop background tasks</li>
     *   <li>Unwatches all FoundationDB keys</li>
     *   <li>Stops the cluster events consumer</li>
     *   <li>Shuts down the scheduler and waits up to 6 seconds for task completion</li>
     *   <li>Updates this member's status to STOPPED and publishes a MemberLeftEvent</li>
     * </ol>
     *
     * <p>If the scheduler cannot be stopped gracefully within 6 seconds, a warning is logged.</p>
     *
     * @throws RuntimeException if shutdown is interrupted
     */
    @Override
    public void shutdown() {
        try {
            shutdown = true;
            keyWatcher.unwatchAll();
            clusterEventsConsumer.stop();

            // executor
            executor.shutdownNow();
            if (!executor.awaitTermination(
                    ExecutorServiceUtil.DEFAULT_TIMEOUT,
                    ExecutorServiceUtil.DEFAULT_TIMEOUT_TIMEUNIT
            )) {
                LOGGER.warn("{} service did not stop gracefully (executor)", NAME);
            }

            // scheduler
            scheduler.shutdownNow();
            if (!scheduler.awaitTermination(
                    ExecutorServiceUtil.DEFAULT_TIMEOUT,
                    ExecutorServiceUtil.DEFAULT_TIMEOUT_TIMEUNIT
            )) {
                LOGGER.warn("{} service did not stop gracefully (scheduler)", NAME);
            }
        } catch (InterruptedException exp) {
            Thread.currentThread().interrupt();
            throw new KronotopException(exp);
        } finally {
            Member member = context.getMember();
            updateMemberStatusAndLeftCluster(member, MemberStatus.STOPPED);
        }
    }

    /**
     * Triggers the cluster topology watcher by incrementing a counter in FoundationDB.
     *
     * <p>This method atomically increments the {@code CLUSTER_TOPOLOGY_CHANGED} key in the cluster
     * metadata subspace. External watchers monitoring this key will be notified of the change,
     * allowing them to react to topology changes (e.g., member joins, leaves, or routing updates).</p>
     *
     * <p>The increment is performed using FoundationDB's atomic ADD mutation, ensuring the operation
     * is efficient and doesn't require reading the current value.</p>
     *
     * @param tr the transaction object used to perform the mutation operation
     */
    public void triggerClusterTopologyWatcher(Transaction tr) {
        DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
        byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_TOPOLOGY_CHANGED));
        tr.mutate(MutationType.ADD, key, PLUS_ONE);
    }

    /**
     * Updates the member's status and publishes a MemberLeftEvent to the cluster.
     *
     * <p>This method is called during shutdown to notify the cluster that this member is leaving.
     * The status update and event publication are performed atomically within a single transaction.</p>
     *
     * <p>If the transaction fails, the member's status is rolled back to its initial value to maintain
     * consistency between the in-memory state and the persisted state.</p>
     *
     * @param member the member whose status is being updated
     * @param status the new status to set (typically STOPPED)
     */
    private void updateMemberStatusAndLeftCluster(Member member, MemberStatus status) {
        MemberStatus initialStatus = member.getStatus();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            member.setStatus(status);
            registry.update(tr, member);
            context.getJournal().getPublisher().publish(tr, JournalName.CLUSTER_EVENTS, new MemberLeftEvent(member));
            tr.commit().join();
        } catch (Exception e) {
            // Rolls back the internal state
            member.setStatus(initialStatus);
        }
    }

    /**
     * Processes a MemberJoinEvent by loading the new member's information and initializing tracking.
     *
     * <p>When a new member joins the cluster, this method:
     * <ul>
     *   <li>Retrieves the member's details from the registry</li>
     *   <li>Opens their directory subspace for heartbeat monitoring</li>
     *   <li>Reads their initial heartbeat value</li>
     *   <li>Creates a MemberView to track their health status</li>
     *   <li>Caches the subspace for efficient future access</li>
     * </ul>
     *
     * @param data the serialized MemberJoinEvent
     */
    private void processMemberJoinEvent(byte[] data) {
        MemberJoinEvent event = JSONUtil.readValue(data, MemberJoinEvent.class);
        Member member = registry.findMember(event.memberId());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = openMemberSubspace(tr, member);
            long heartbeat = Heartbeat.get(tr, subspace);
            subspaces.put(member, subspace);
            others.put(member, new MemberView(heartbeat));
        }

        if (!context.getMember().equals(member)) {
            // A new cluster member has joined.
            LOGGER.info("Member join: {}", member.getExternalAddress());
        }
    }

    /**
     * Processes a MemberLeftEvent by cleaning up the departed member's tracking data.
     *
     * <p>When a member leaves the cluster, this method:
     * <ul>
     *   <li>Removes the member's directory subspace from the cache</li>
     *   <li>Removes the member's health view</li>
     *   <li>Shuts down any internal connections to the departed member</li>
     * </ul>
     *
     * @param data the serialized MemberLeftEvent
     */
    private void processMemberLeftEvent(byte[] data) {
        MemberLeftEvent event = JSONUtil.readValue(data, MemberLeftEvent.class);
        Member member = registry.findMember(event.memberId());
        subspaces.remove(member);
        others.remove(member);

        context.getInternalConnectionPool().shutdown(member);
        if (!context.getMember().equals(member)) {
            LOGGER.info("Member left: {}", member.getExternalAddress());
        }
    }

    /**
     * Processes a cluster event by dispatching to the appropriate handler based on event type.
     *
     * @param event the cluster event to process
     * @throws KronotopException if the event type is unknown
     */
    private void processClusterEvent(Event event) {
        BaseBroadcastEvent baseBroadcastEvent = JSONUtil.readValue(event.value(), BaseBroadcastEvent.class);
        switch (baseBroadcastEvent.kind()) {
            case MEMBER_JOIN -> processMemberJoinEvent(event.value());
            case MEMBER_LEFT -> processMemberLeftEvent(event.value());
            default -> throw new KronotopException("Unknown broadcast event: " + baseBroadcastEvent.kind());
        }
    }

    /**
     * Fetches and processes all pending cluster events from the journal.
     *
     * <p>This method runs in a synchronized manner to ensure events are processed sequentially.
     * It consumes events from the cluster events journal, processes them (member join/leave),
     * and updates the consumer offset. If an event fails to process, it is skipped and logged.</p>
     *
     * <p>The method continues consuming events until no more are available, then commits the
     * transaction to persist the updated consumer offset.</p>
     */
    private synchronized void fetchClusterEvents() {
        Retry retry = TransactionUtils.retry(10, Duration.ofMillis(100));
        retry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                while (true) {
                    // Try to consume the latest event.
                    Event event;
                    try {
                        event = clusterEventsConsumer.consume(tr);
                        if (event == null) {
                            LOGGER.trace("No more cluster events to consume");
                            break;
                        }
                    } catch (IllegalConsumerStateException exp) {
                        LOGGER.warn("Cluster event consumer is in an illegal state while consuming: {}", exp.getMessage());
                        break;
                    }

                    try {
                        processClusterEvent(event);
                        clusterEventsConsumer.markConsumed(tr, event);
                    } catch (IllegalConsumerStateException exp) {
                        LOGGER.warn("Cluster event consumer is in an illegal state while marking consumed: {}", exp.getMessage());
                        break;
                    } catch (Exception e) {
                        LOGGER.error("Cluster event processing failed for event={} – skipping", event, e);
                    }
                }
                tr.commit().join();
                LOGGER.debug("Cluster event batch committed");
            }
        });
    }

    /**
     * Background task that watches the cluster events journal for new events.
     *
     * <p>This task uses FoundationDB's watch mechanism to efficiently wait for new events.
     * When the journal trigger key changes, the watcher is notified and fetches all pending events.</p>
     *
     * <p>The task runs continuously until the service is shut down, re-scheduling itself
     * after each iteration.</p>
     */
    private class ClusterEventsJournalWatcher implements Runnable {
        /**
         * Latch to signal when the watcher has started and is ready.
         */
        private final CountDownLatch latch = new CountDownLatch(1);

        /**
         * Blocks until the watcher has started and performed its initial event fetch.
         *
         * @throws RuntimeException if the wait is interrupted
         */
        public void waitUntilStarted() {
            try {
                latch.await(); // Wait until the watcher is started
            } catch (InterruptedException exp) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Wait interrupted", exp);
            }
        }

        @Override
        public void run() {
            if (shutdown) {
                return;
            }

            while (!shutdown) {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    CompletableFuture<Void> watcher = keyWatcher.watch(tr, context.getJournal().getJournalMetadata(JournalName.CLUSTER_EVENTS.getValue()).trigger());
                    tr.commit().join();
                    try {
                        // Try to fetch the latest events before start waiting
                        fetchClusterEvents();
                        latch.countDown();
                        watcher.join();
                    } catch (CancellationException e) {
                        LOGGER.debug("{} watcher has been cancelled", JournalName.CLUSTER_EVENTS);
                        return;
                    }
                    // A new event is ready to read
                    fetchClusterEvents();
                } catch (Exception e) {
                    LOGGER.error("Error while watching journal: {}", JournalName.CLUSTER_EVENTS, e);
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                }
            }
        }
    }

    /**
     * Background task that periodically updates this member's heartbeat timestamp.
     *
     * <p>This task runs every {@code heartbeatInterval} seconds and updates the member's
     * heartbeat value in FoundationDB. Other members monitor this value to determine if
     * this member is alive.</p>
     *
     * <p>The task reschedules itself after each execution until the service is shut down.</p>
     */
    private class HeartbeatTask implements Runnable {
        /**
         * Directory subspace where this member's heartbeat is stored.
         */
        private final DirectorySubspace subspace = subspaces.get(context.getMember());

        @Override
        public void run() {
            if (shutdown) {
                return;
            }
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Heartbeat.set(tr, subspace);
                tr.commit().join();
            } catch (Exception e) {
                LOGGER.error("Error while updating heartbeat", e);
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            } finally {
                if (!shutdown) {
                    scheduler.schedule(this, heartbeatInterval, TimeUnit.SECONDS);
                }
            }
        }
    }

    /**
     * Checks the health status of all cluster members by comparing their heartbeats.
     *
     * <p>For each member in the cluster, this method:
     * <ul>
     *   <li>Reads the latest heartbeat from FoundationDB</li>
     *   <li>Updates the member's view if heartbeat changed, or increments expected heartbeat if unchanged</li>
     *   <li>Marks the member as dead if silent period exceeds the threshold</li>
     *   <li>Reincarnates previously dead members if they resume sending heartbeats</li>
     * </ul>
     *
     * <p>Members already marked as dead are skipped to avoid unnecessary processing.</p>
     *
     * @param maxSilentPeriod the maximum number of heartbeat intervals a member can be silent
     *                        before being considered dead
     */
    void checkClusterMembers(long maxSilentPeriod) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (Member member : others.keySet()) {
                DirectorySubspace subspace = subspaces.get(member);
                long latestHeartbeat = Heartbeat.get(tr, subspace);
                MemberView view = others.computeIfPresent(member, (m, memberView) -> {
                    if (!memberView.isAlive()) {
                        // continue
                        return memberView;
                    }

                    if (memberView.getLatestHeartbeat() != latestHeartbeat) {
                        memberView.setLatestHeartbeat(latestHeartbeat);
                    } else {
                        memberView.increaseExpectedHeartbeat();
                    }
                    return memberView;
                });

                if (view == null) {
                    // This should not be possible
                    continue;
                }

                long silentPeriod = view.getExpectedHeartbeat() - view.getLatestHeartbeat();
                if (view.isAlive() && silentPeriod > maxSilentPeriod) {
                    LOGGER.warn("{} has been suspected to be dead", member.getId());
                    view.setAlive(false);
                    return;
                }

                if (!view.isAlive() && silentPeriod < maxSilentPeriod) {
                    LOGGER.warn("{} has been reincarnated", member.getId());
                    view.setAlive(true);
                }
            }
        }
    }

    /**
     * Background task that monitors other members' heartbeats and detects failures.
     *
     * <p>This task runs every {@code heartbeatInterval} seconds and checks the heartbeat
     * status of all other members in the cluster. It implements a simple failure detector:</p>
     * <ul>
     *   <li>If a member's heartbeat hasn't changed for {@code maxSilentPeriod} intervals, mark it as dead</li>
     *   <li>If a previously dead member's heartbeat starts changing again, mark it as alive (reincarnated)</li>
     * </ul>
     *
     * <p>The failure detector tracks both the latest observed heartbeat and the expected heartbeat
     * count. The difference between these values represents the silent period.</p>
     *
     * <p>The task reschedules itself after each execution until the service is shut down.</p>
     */
    private class FailureDetectionTask implements Runnable {
        /**
         * Maximum number of heartbeat intervals a member can be silent before being considered dead.
         * Calculated as: ceil(heartbeatMaximumSilentPeriod / heartbeatInterval)
         */
        private final long maxSilentPeriod = (long) Math.ceil((double) heartbeatMaximumSilentPeriod / heartbeatInterval);

        @Override
        public void run() {
            if (shutdown) {
                return;
            }
            try {
                checkClusterMembers(maxSilentPeriod);
            } catch (Exception e) {
                LOGGER.error("Error while running failure detection task", e);
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            } finally {
                if (!shutdown) {
                    scheduler.schedule(this, heartbeatInterval, TimeUnit.SECONDS);
                }
            }
        }
    }
}
