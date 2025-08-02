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
import com.kronotop.BaseKronotopService;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.KronotopService;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.KeyWatcher;
import com.kronotop.journal.Consumer;
import com.kronotop.journal.ConsumerConfig;
import com.kronotop.journal.Event;
import com.kronotop.journal.JournalName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.*;

/**
 * Membership service implements all business logic around cluster membership and health checks.
 */
public class MembershipService extends BaseKronotopService implements KronotopService {
    public static final String NAME = "Membership";
    private static final byte[] PLUS_ONE = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian
    private static final Logger LOGGER = LoggerFactory.getLogger(MembershipService.class);
    private final Context context;
    private final MemberRegistry registry;
    private final ScheduledThreadPoolExecutor scheduler;
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final ConcurrentHashMap<Member, DirectorySubspace> subspaces = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Member, MemberView> others = new ConcurrentHashMap<>();
    private final Consumer clusterEventsConsumer;
    private final int heartbeatInterval;
    private final int heartbeatMaximumSilentPeriod;
    private volatile boolean isShutdown;

    public MembershipService(Context context) {
        super(context, NAME);

        this.context = context;
        this.registry = new MemberRegistry(context);
        this.heartbeatInterval = context.getConfig().getInt("cluster.heartbeat.interval");
        this.heartbeatMaximumSilentPeriod = context.getConfig().getInt("cluster.heartbeat.maximum_silent_period");

        String consumerId = String.format("%s-member:%s", JournalName.CLUSTER_EVENTS.getValue(), context.getMember().getId());
        ConsumerConfig config = new ConsumerConfig(consumerId, JournalName.CLUSTER_EVENTS.getValue(), ConsumerConfig.Offset.LATEST);
        this.clusterEventsConsumer = new Consumer(context, config);

        ThreadFactory factory = Thread.ofVirtual().name("kr.membership").factory();
        this.scheduler = new ScheduledThreadPoolExecutor(3, factory);
    }

    private DirectorySubspace openMemberSubspace(Transaction tr, Member member) {
        KronotopDirectoryNode directory = registry.getDirectoryNode(member.getId());
        return DirectoryLayer.getDefault().open(tr, directory.toList()).join();
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
                others.put(member, memberView);
                subspaces.put(member, subspace);
            }
        }
    }

    /**
     * Starts the MembershipService, initializing the member and configuring the necessary components.
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
        scheduler.execute(eventsJournalWatcher);
        eventsJournalWatcher.waitUntilStarted();

        scheduler.execute(new HeartbeatTask());
        scheduler.execute(new FailureDetectionTask());
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
     * Updates the information of a specified member in the registry.
     *
     * @param member The Member object containing the updated information.
     */
    public void updateMember(Transaction tr, Member member) {
        registry.update(tr, member);
    }

    /**
     * Removes a member from the registry if their status is not RUNNING.
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
     * Retrieves a read-only view of the other members in the system and their respective views.
     *
     * @return an unmodifiable map where the keys are Member objects and the values are MemberView objects.
     */
    public Map<Member, MemberView> getOthers() {
        return Collections.unmodifiableMap(others);
    }

    @Override
    public void shutdown() {
        try {
            isShutdown = true;
            keyWatcher.unwatchAll();
            clusterEventsConsumer.stop();
            scheduler.shutdownNow();

            if (!scheduler.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.warn("{} service cannot be stopped gracefully", NAME);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            Member member = context.getMember();
            updateMemberStatusAndLeftCluster(member, MemberStatus.STOPPED);
        }
    }

    /**
     * Triggers the cluster topology watcher by indicating that the cluster topology has changed.
     * This is done by performing a mutation operation within the given transaction that modifies
     * the value associated with the cluster topology change key in the metadata subspace.
     *
     * @param tr The transaction object used to perform the mutation operation indicating a
     *           cluster topology change.
     */
    public void triggerClusterTopologyWatcher(Transaction tr) {
        DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
        byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_TOPOLOGY_CHANGED));
        tr.mutate(MutationType.ADD, key, PLUS_ONE);
    }

    private void updateMemberStatusAndLeftCluster(Member member, MemberStatus status) {
        MemberStatus initialStatus = member.getStatus();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            member.setStatus(status);
            registry.update(tr, member);
            context.getJournal().getPublisher().publish(tr, JournalName.CLUSTER_EVENTS, new MemberLeftEvent(member));
            tr.commit().join();
        } catch (Exception e) {
            // Rollback the internal state
            member.setStatus(initialStatus);
        }
    }

    private void processMemberJoinEvent(byte[] data) {
        MemberJoinEvent event = JSONUtil.readValue(data, MemberJoinEvent.class);
        Member member = registry.findMember(event.memberId());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = openMemberSubspace(tr, member);
            long heartbeat = Heartbeat.get(tr, subspace);
            subspaces.put(member, subspace);
            others.put(member, new MemberView(heartbeat));
        }

        // A new cluster member has joined.
        LOGGER.info("Member join: {}", member.getExternalAddress());
    }

    private void processMemberLeftEvent(byte[] data) {
        MemberLeftEvent event = JSONUtil.readValue(data, MemberLeftEvent.class);
        Member member = registry.findMember(event.memberId());
        subspaces.remove(member);
        others.remove(member);

        context.getInternalConnectionPool().shutdown(member);
        LOGGER.info("Member left: {}", member.getExternalAddress());
    }

    private void processClusterEvent(Event event) {
        BaseBroadcastEvent baseBroadcastEvent = JSONUtil.readValue(event.value(), BaseBroadcastEvent.class);
        LOGGER.debug("Received broadcast event: {}", baseBroadcastEvent.kind());
        switch (baseBroadcastEvent.kind()) {
            case MEMBER_JOIN -> processMemberJoinEvent(event.value());
            case MEMBER_LEFT -> processMemberLeftEvent(event.value());
            default -> throw new KronotopException("Unknown broadcast event: " + baseBroadcastEvent.kind());
        }
    }

    private synchronized void fetchClusterEvents() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            while (true) {
                // Try to consume the latest event.
                Event event = clusterEventsConsumer.consume(tr);
                if (event == null) {
                    break;
                }

                try {
                    processClusterEvent(event);
                    clusterEventsConsumer.setOffset(event);
                } catch (Exception e) {
                    LOGGER.error("Failed to process a broadcast event, passing it", e);
                }
            }
            clusterEventsConsumer.complete(tr);
            tr.commit().join();
        }
        ;
    }

    private class ClusterEventsJournalWatcher implements Runnable {
        private final CountDownLatch latch = new CountDownLatch(1);

        public void waitUntilStarted() {
            try {
                latch.await(); // Wait until the watcher is started
            } catch (InterruptedException e) {
                throw new RuntimeException("Wait interrupted", e);
            }
        }

        @Override
        public void run() {
            if (isShutdown) {
                return;
            }

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
            } finally {
                if (!isShutdown) {
                    scheduler.execute(this);
                }
            }
        }
    }

    private class HeartbeatTask implements Runnable {
        private final DirectorySubspace subspace = subspaces.get(context.getMember());

        @Override
        public void run() {
            if (isShutdown) {
                return;
            }
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Heartbeat.set(tr, subspace);
                tr.commit().join();
            } catch (Exception e) {
                LOGGER.error("Error while updating heartbeat", e);
            } finally {
                if (!isShutdown) {
                    scheduler.schedule(this, heartbeatInterval, TimeUnit.SECONDS);
                }
            }
        }
    }

    private class FailureDetectionTask implements Runnable {
        private final long maxSilentPeriod = (long) Math.ceil((double) heartbeatMaximumSilentPeriod / heartbeatInterval);

        @Override
        public void run() {
            if (isShutdown) {
                return;
            }
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
            } catch (Exception e) {
                LOGGER.error("Error while running failure detection task", e);
            } finally {
                if (!isShutdown) {
                    scheduler.schedule(this, heartbeatInterval, TimeUnit.SECONDS);
                }
            }
        }
    }
}
