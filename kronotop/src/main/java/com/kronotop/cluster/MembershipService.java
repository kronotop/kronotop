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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.BaseKronotopService;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.KronotopService;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.KeyWatcher;
import com.kronotop.journal.*;
import com.kronotop.transaction.TransactionUtil;
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
 * Manages cluster membership, heartbeat generation, and failure detection.
 */
public class MembershipService extends BaseKronotopService implements KronotopService {
    public static final String NAME = "Membership";

    /**
     * Value 1 in little-endian format for atomic ADD mutations.
     */
    private static final byte[] PLUS_ONE = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian

    private static final Logger LOGGER = LoggerFactory.getLogger(MembershipService.class);

    private final Context context;

    private final MemberRegistry registry;

    private final ScheduledThreadPoolExecutor scheduler;

    private final ExecutorService executor;

    private final KeyWatcher keyWatcher = new KeyWatcher();

    /**
     * Maps members to their heartbeat directory subspaces.
     */
    private final ConcurrentHashMap<Member, DirectorySubspace> subspaces = new ConcurrentHashMap<>();

    /**
     * Tracks other cluster members and their observed health status.
     */
    private final ConcurrentHashMap<Member, MemberView> knownMembers = new ConcurrentHashMap<>();

    private final Consumer clusterEventsConsumer;

    private final int heartbeatInterval;

    private final int heartbeatMaximumSilentPeriod;

    private volatile boolean shutdown;

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

    private DirectorySubspace openMemberSubspace(Transaction tr, Member member) {
        KronotopDirectoryNode directory = registry.getDirectoryNode(member.getId());
        return context.getDirectoryLayer().open(tr, directory.toList()).join();
    }

    private void initializeInternalState() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (Member member : registry.listMembers(tr)) {
                if (!member.getStatus().equals(MemberStatus.RUNNING)) {
                    continue;
                }
                DirectorySubspace subspace = openMemberSubspace(tr, member);
                long heartbeat = Heartbeat.get(tr, subspace);
                MemberView memberView = new MemberView(heartbeat);
                knownMembers.put(member, memberView);
                subspaces.put(member, subspace);
            }
        }
    }

    /**
     * Registers this member in the cluster and starts background monitoring.
     *
     * @throws MemberAlreadyRunningException if a member with the same ID is already RUNNING
     */
    public void start() {
        Member member = context.getMember();
        member.setStatus(MemberStatus.RUNNING);

        boolean forceInitialization = context.getConfig().hasPath("cluster.force_initialization") &&
                context.getConfig().getBoolean("cluster.force_initialization");
        registry.registerOrUpdate(member, forceInitialization);

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
     * Returns the latest heartbeat timestamp for the given member, or 0 if unknown.
     */
    public long getLatestHeartbeat(Member member) {
        MemberView memberView = knownMembers.get(member);
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
     * Finds the member with the given ID.
     *
     * @throws MemberNotRegisteredException if no member exists with that ID
     */
    public Member findMember(String memberId) {
        return registry.findMember(memberId);
    }

    /**
     * Finds the member with the given ID within the given transaction.
     *
     * @throws MemberNotRegisteredException if no member exists with that ID
     */
    public Member findMember(Transaction tr, String memberId) {
        return registry.findMember(tr, memberId);
    }

    public boolean isMemberRegistered(String memberId) {
        return registry.isAdded(memberId);
    }

    public boolean isMemberRegistered(Transaction tr, String memberId) {
        return registry.isAdded(tr, memberId);
    }

    /**
     * Persists updated member state within the given transaction.
     */
    public void updateMember(Transaction tr, Member member) {
        registry.update(tr, member);
    }

    /**
     * Removes a member from the registry. The member must not be in RUNNING status.
     *
     * @throws KronotopException if the member is currently RUNNING
     */
    public void removeMember(Transaction tr, String memberId) {
        Member member = registry.findMember(tr, memberId);
        if (member.getStatus().equals(MemberStatus.RUNNING)) {
            throw new KronotopException("Member in " + MemberStatus.RUNNING + " status cannot be removed");
        }
        registry.remove(tr, memberId);
    }

    /**
     * Returns a read-only view of known cluster members and their health status.
     */
    public Map<Member, MemberView> getKnownMembers() {
        return Collections.unmodifiableMap(knownMembers);
    }

    /**
     * Stops all background tasks and marks this member as STOPPED.
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
     * Increments the cluster topology version to notify external watchers of a topology change.
     */
    public void triggerClusterTopologyWatcher(Transaction tr) {
        DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
        byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_TOPOLOGY_CHANGED));
        tr.mutate(MutationType.ADD, key, PLUS_ONE);
    }

    /**
     * Updates the member's status and publishes a MemberLeftEvent.
     */
    private void updateMemberStatusAndLeftCluster(Member member, MemberStatus status) {
        MemberStatus initialStatus = member.getStatus();
        Retry retry = TransactionUtil.retry(10, Duration.ofMillis(50));
        try {
            retry.executeRunnable(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    member.setStatus(status);
                    registry.update(tr, member);
                    context.getJournal().getPublisher().publish(tr, JournalName.CLUSTER_EVENTS, new MemberLeftEvent(member));
                    tr.commit().join();
                } catch (Exception e) {
                    member.setStatus(initialStatus);
                    throw e;
                }
            });
        } catch (Exception e) {
            member.setStatus(initialStatus);
            LOGGER.error("Failed to update member status during shutdown after retries", e);
        }
    }

    /**
     * Handles a member join event by initializing tracking state for the new member.
     */
    private void processMemberJoinEvent(byte[] data) {
        MemberJoinEvent event = JSONUtil.readValue(data, MemberJoinEvent.class);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Member member = registry.findMember(tr, event.memberId());
            DirectorySubspace subspace = openMemberSubspace(tr, member);
            long heartbeat = Heartbeat.get(tr, subspace);
            subspaces.put(member, subspace);
            knownMembers.put(member, new MemberView(heartbeat));
            if (!context.getMember().equals(member)) {
                LOGGER.info("Member join: {}", member.getExternalAddress());
            }
        }
    }

    /**
     * Handles a member left event by cleaning up tracking state for the departed member.
     */
    private void processMemberLeftEvent(byte[] data) {
        MemberLeftEvent event = JSONUtil.readValue(data, MemberLeftEvent.class);
        Member member;
        try {
            member = registry.findMember(event.memberId());
        } catch (MemberNotRegisteredException e) {
            LOGGER.warn("Member {} already removed from registry, cleaning up local state", event.memberId());
            member = knownMembers.keySet().stream()
                    .filter(m -> m.getId().equals(event.memberId()))
                    .findFirst()
                    .orElse(null);
            if (member == null) {
                return;
            }
        }
        subspaces.remove(member);
        knownMembers.remove(member);
        context.getInternalClientPool().evict(member);

        if (!context.getMember().equals(member)) {
            LOGGER.info("Member left: {}", member.getExternalAddress());
        }
    }

    /**
     * Dispatches a cluster event to the appropriate handler.
     *
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
     * Consumes and processes all pending events from the cluster events journal.
     */
    private synchronized void fetchClusterEvents() {
        Retry retry = TransactionUtil.retry(10, Duration.ofMillis(100));
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
                        LOGGER.error("Cluster event processing failed for event={} – will retry on next trigger", event, e);
                        break;
                    }
                }
                tr.commit().join();
                LOGGER.debug("Cluster event batch committed");
            }
        });
    }

    /**
     * Evaluates the health of all known cluster members based on their heartbeat activity.
     *
     * @param maxSilentPeriod maximum missed heartbeat intervals before a member is considered dead
     */
    void checkClusterMembers(long maxSilentPeriod) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (Member member : knownMembers.keySet()) {
                DirectorySubspace subspace = subspaces.get(member);
                if (subspace == null) {
                    continue;
                }
                long latestHeartbeat = Heartbeat.get(tr, subspace);
                MemberView view = knownMembers.computeIfPresent(member, (ignored, memberView) -> {
                    if (memberView.getLatestHeartbeat() != latestHeartbeat) {
                        memberView.setLatestHeartbeat(latestHeartbeat);
                    } else if (memberView.isAlive()) {
                        memberView.increaseExpectedHeartbeat();
                    }
                    return memberView;
                });

                if (view == null) {
                    continue;
                }

                long silentPeriod = view.getExpectedHeartbeat() - view.getLatestHeartbeat();
                if (view.isAlive() && silentPeriod > maxSilentPeriod) {
                    LOGGER.warn("{} has been suspected to be dead", member.getId());
                    view.setAlive(false);
                    continue;
                }

                if (!view.isAlive() && silentPeriod < maxSilentPeriod) {
                    LOGGER.warn("{} has been reincarnated", member.getId());
                    view.setAlive(true);
                }
            }
        }
    }

    /**
     * Watches the cluster events journal for new events using FoundationDB watches.
     */
    private class ClusterEventsJournalWatcher implements Runnable {
        private final CountDownLatch latch = new CountDownLatch(1);

        /**
         * Blocks until the watcher has started and performed its initial event fetch.
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
     * Periodically updates this member's heartbeat in FoundationDB.
     */
    private class HeartbeatTask implements Runnable {
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
     * Periodically checks other members' heartbeats and detects failures.
     */
    private class FailureDetectionTask implements Runnable {
        /**
         * Maximum missed heartbeat intervals before a member is dead: ceil(maximumSilentPeriod / interval).
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
