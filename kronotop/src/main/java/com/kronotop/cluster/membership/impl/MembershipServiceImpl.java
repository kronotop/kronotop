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

package com.kronotop.cluster.membership.impl;

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.JSONUtils;
import com.kronotop.KeyWatcher;
import com.kronotop.cluster.*;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingTable;
import com.kronotop.cluster.handlers.KrAdminHandler;
import com.kronotop.cluster.membership.*;
import com.kronotop.common.KronotopException;
import com.kronotop.journal.Event;
import com.kronotop.journal.JournalName;
import com.kronotop.server.ServerKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Membership service implements all business logic around cluster membership and health checks.
 */
public class MembershipServiceImpl extends CommandHandlerService implements MembershipService {
    public static final String NAME = "Membership";
    private static final Logger LOGGER = LoggerFactory.getLogger(MembershipServiceImpl.class);
    private final Context context;
    private final MemberRegistry registry;
    private final ScheduledThreadPoolExecutor scheduler;
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final AtomicReference<RoutingTable> routingTable = new AtomicReference<>();
    private final AtomicReference<Member> knownCoordinator = new AtomicReference<>();
    private final AtomicReference<byte[]> latestClusterEventKey = new AtomicReference<>();
    private final int heartbeatInterval;
    private final int heartbeatMaximumSilentPeriod;
    private volatile boolean isShutdown;
    private volatile boolean clusterInitialized;

    public MembershipServiceImpl(Context context) {
        super(context);

        this.context = context;
        this.registry = new MemberRegistry(context);
        this.heartbeatInterval = context.getConfig().getInt("cluster.heartbeat.interval");
        this.heartbeatMaximumSilentPeriod = context.getConfig().getInt("cluster.heartbeat.maximum_silent_period");

        // TODO: CLUSTER-REFACTORING
        RoutingTable table = new RoutingTable();
        int numberOfShards = context.getConfig().getInt("redis.shards");
        for (int i = 0; i < numberOfShards; i++) {
            table.setRoute(i, new Route(context.getMember()));
        }
        routingTable.set(table);
        // TODO: CLUSTER-REFACTORING

        ThreadFactory factory = Thread.ofVirtual().name("kr.membership").factory();
        this.scheduler = new ScheduledThreadPoolExecutor(2, factory);

        handlerMethod(ServerKind.INTERNAL, new KrAdminHandler(this));
    }

    public void start() {
        Member member = context.getMember();
        context.getJournal().getPublisher().publish(JournalName.clusterEvents(), new MemberJoinEvent(member));
        LOGGER.info("Member: {} has been registered", context.getMember().getId());

        byte[] key = context.getFoundationDB().run(tr -> context.getJournal().getConsumer().getLatestEventKey(tr, JournalName.clusterEvents()));
        this.latestClusterEventKey.set(key);

        scheduler.execute(new ClusterEventsJournalWatcher());
        scheduler.execute(new HeartbeatTask());
        scheduler.execute(new FailureDetectionTask());

        findClusterCoordinator();
        Member coordinator = knownCoordinator.get();
        if (coordinator != null) {
            LOGGER.info("Cluster coordinator found: {}", coordinator.getExternalAddress());
        }

        if (isClusterInitialized_internal()) {
            clusterInitialized = true;
        } else {
            scheduler.execute(new ClusterInitializationWatcher());
        }
    }

    public TreeSet<Member> listMembers() {
        return registry.listMembers();
    }

    public boolean isClusterInitialized() {
        return clusterInitialized;
    }

    /**
     * Returns the service name
     *
     * @return service name
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Returns the global context
     *
     * @return global context
     */
    @Override
    public Context getContext() {
        return context;
    }

    /**
     * Shuts down the cluster service. It stops the background services and
     * frees allocated resources before quit.
     */
    @Override
    public void shutdown() {
        isShutdown = true;
        keyWatcher.unwatch(context.getJournal().getJournalMetadata(JournalName.clusterEvents()).getTrigger());
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.warn("{} service cannot be stopped gracefully", NAME);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Retrieves the last heartbeat timestamp for a given member.
     *
     * @param tr     FoundationDB transaction to use for the operation
     * @param member Member for which to retrieve the last heartbeat timestamp
     * @return The last heartbeat timestamp for the member, or 0 if it has not been set
     * @throws NoSuchMemberException if the member does not exist
     */
    private long getLatestHeartbeat(Transaction tr, Member member) {
        long lastHeartbeat = 0;
        try {
            DirectorySubspace subspace = memberSubspaces.get(member);
            byte[] data = tr.get(subspace.pack(Keys.LAST_HEARTBEAT.toString())).join();
            if (data != null) {
                lastHeartbeat = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
            }
        } catch (CompletionException e) {
            if (!(e.getCause() instanceof NoSuchDirectoryException)) {
                throw new NoSuchMemberException(String.format("No such member: %s", member.getExternalAddress()));
            }
        }
        return lastHeartbeat;
    }

    private long getLatestHeartbeat(Member member) {
        return getLatestHeartbeats(member).get(member);
    }

    /**
     * Retrieves the latest heartbeat timestamps for the given members.
     *
     * @param members The members for which to retrieve the latest heartbeat timestamps
     * @return A map containing the members as keys and their latest heartbeat timestamps as values
     * @throws NoSuchMemberException If a member does not exist
     */
    public Map<Member, Long> getLatestHeartbeats(Member... members) {
        Map<Member, Long> result = new HashMap<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (Member member : members) {
                long latestHeartbeat = getLatestHeartbeat(tr, member);
                result.put(member, latestHeartbeat);
            }
        }
        return result;
    }

    /**
     * Tries to find a cluster coordinator.
     */
    private synchronized void findClusterCoordinator() {
        TreeSet<Member> members = registry.listMembers();
        try {
            Member coordinator = members.first();
            Member me = context.getMember();
            if (coordinator.equals(me)) {
                if (knownCoordinator.get() == null || !knownCoordinator.get().equals(me)) {
                    LOGGER.info("Propagating myself as the cluster coordinator");
                    coordinatorService.start();
                }
            }
            knownCoordinator.set(coordinator);
        } catch (NoSuchElementException e) {
            LOGGER.warn("No cluster coordinator found");
        }
    }

    public RoutingTable getRoutingTable() {
        return routingTable.get();
    }

    public Member getKnownCoordinator() {
        return knownCoordinator.get();
    }

    private MemberJoinEvent processMemberJoinEvent(byte[] data) {
        MemberJoinEvent event = JSONUtils.readValue(data, MemberJoinEvent.class);
        Member member = registry.findMember(event.memberId());

        // A new cluster member has joined.
        LOGGER.info("Member join: {}", member.getExternalAddress());
        return event;
    }

    private MemberLeftEvent processMemberLeftEvent(byte[] data) {
        MemberLeftEvent event = JSONUtils.readValue(data, MemberLeftEvent.class);
        Member member = registry.findMember(event.memberId());

        LOGGER.info("Member left: {}", member.getExternalAddress());
        return event;
    }

    private BroadcastEvent processClusterEvent(Event event) {
        BaseBroadcastEvent baseBroadcastEvent = JSONUtils.readValue(event.getValue(), BaseBroadcastEvent.class);
        LOGGER.debug("Received broadcast event: {}", baseBroadcastEvent.kind());
        try {
            return switch (baseBroadcastEvent.kind()) {
                case MEMBER_JOIN -> processMemberJoinEvent(event.getValue());
                case MEMBER_LEFT -> processMemberLeftEvent(event.getValue());
                default -> throw new KronotopException("Unknown broadcast event: " + baseBroadcastEvent.kind());
            };
        } finally {
            if (baseBroadcastEvent.kind().equals(BroadcastEventKind.MEMBER_LEFT)) {
                findClusterCoordinator();
            }
        }
    }

    private void submitBroadcastEvent(BroadcastEvent event) {
        coordinatorService.submitEvent(event);
    }

    /**
     * fetchClusterEvents tries to fetch the latest events from the cluster's global journal and processes them.
     */
    private synchronized void fetchClusterEvents() {
        context.getFoundationDB().run(tr -> {
            while (true) {
                // Try to consume the latest event.
                byte[] key = latestClusterEventKey.get();
                Event event = context.getJournal().getConsumer().consumeNext(tr, JournalName.clusterEvents(), key);
                if (event == null) return null;

                try {
                    BroadcastEvent broadcastEvent = processClusterEvent(event);
                    submitBroadcastEvent(broadcastEvent);
                } catch (Exception e) {
                    LOGGER.error("Failed to process a broadcast event, passing it", e);
                } finally {
                    // Processed the event successfully. Forward the position.
                    latestClusterEventKey.set(event.getKey());
                }
            }
        });
    }

    /**
     * Removes a dead member from the TreeSet of members by recursively pruning dead members.
     *
     * @param members The TreeSet of members
     * @param dead    The dead member to be pruned
     */
    private void pruneDeadMembers(TreeSet<Member> members, Member dead) {
        Member closest = members.higher(dead);
        if (closest == null) {
            return;
        }
        MemberView memberView = knownMembers.get(closest);
        if (!memberView.getAlive()) {
            pruneDeadMembers(members, closest);
        }
    }

    /**
     * Finds the dead coordinator in the given TreeSet of members and recursively prunes dead members.
     *
     * @param members The TreeSet of members
     * @param member  The member to start the search from
     */
    private void findDeadCoordinator(TreeSet<Member> members, Member member) {
        Member closest = members.lower(member);
        if (closest == null) {
            return;
        }

        Member coordinator = knownCoordinator.get();
        MemberView memberView = knownMembers.get(closest);
        if (memberView.getAlive()) {
            return;
        }
        if (coordinator.equals(closest)) {
            // Remove it from FDB.
            pruneDeadMembers(members, closest);
            return;
        }

        findDeadCoordinator(members, closest);
    }

    private boolean isClusterInitialized_internal() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = MembershipUtils.createOrOpenClusterMetadataSubspace(context);
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

    public enum Keys {
        EXTERNAL_ADDRESS, INTERNAL_ADDRESS, PROCESS_ID, LAST_HEARTBEAT, STATUS
    }

    /**
     * This class represents a runnable object that watches the cluster events journal.
     * It fetches the latest events from the journal and processes them.
     */
    private class ClusterEventsJournalWatcher implements Runnable {
        @Override
        public void run() {
            if (isShutdown) {
                return;
            }

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                CompletableFuture<Void> watcher = keyWatcher.watch(tr, context.getJournal().getJournalMetadata(JournalName.clusterEvents()).getTrigger());
                tr.commit().join();
                try {
                    // Try to fetch the latest events before start waiting
                    fetchClusterEvents();

                    watcher.join();
                } catch (CancellationException e) {
                    LOGGER.info("{} watcher has been cancelled", JournalName.clusterEvents());
                    return;
                }
                // A new event is ready to read
                fetchClusterEvents();
            } catch (Exception e) {
                LOGGER.error("Error while watching journal: {}", JournalName.clusterEvents(), e);
            } finally {
                if (!isShutdown) {
                    scheduler.execute(this);
                }
            }
        }
    }

    /**
     * Runnable class representing a heartbeat task.
     * This task is responsible for updating the last heartbeat timestamp of a member in the cluster.
     */
    private class HeartbeatTask implements Runnable {
        private static final byte[] HEARTBEAT_DELTA = new byte[]{1, 0, 0, 0, 0, 0, 0, 0}; // 1, byte order: little-endian

        @Override
        public void run() {
            if (isShutdown) {
                return;
            }
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                DirectorySubspace subspace = memberSubspaces.get(context.getMember());
                tr.mutate(MutationType.ADD, subspace.pack(Keys.LAST_HEARTBEAT.toString()), HEARTBEAT_DELTA);
                tr.commit().join();
            } catch (Exception e) {
                LOGGER.error("Error while running heartbeat task", e);
            } finally {
                if (!isShutdown) {
                    scheduler.schedule(this, heartbeatInterval, TimeUnit.SECONDS);
                }
            }
        }
    }

    /**
     * FailureDetectionTask is a private nested class that implements the Runnable interface.
     * It is responsible for monitoring the heartbeat of cluster members and detecting failures.
     */
    private class FailureDetectionTask implements Runnable {
        private final long maxSilentPeriod = (long) Math.ceil((double) heartbeatMaximumSilentPeriod / heartbeatInterval);

        @Override
        public void run() {
            if (isShutdown) {
                return;
            }
            Member coordinator = knownCoordinator.get();
            boolean isCoordinatorAlive = true;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                for (Member member : knownMembers.keySet()) {
                    long lastHeartbeat = getLatestHeartbeat(tr, member);
                    MemberView view = knownMembers.computeIfPresent(member, (m, memberView) -> {
                        if (memberView.getLastHeartbeat() != lastHeartbeat) {
                            memberView.setLastHeartbeat(lastHeartbeat);
                        } else {
                            memberView.increaseExpectedHeartbeat();
                        }
                        return memberView;
                    });

                    if (view == null) {
                        continue;
                    }

                    long silentPeriod = view.getExpectedHeartbeat() - view.getLastHeartbeat();
                    if (silentPeriod > maxSilentPeriod) {
                        LOGGER.warn("{} has been suspected to be dead", member.getExternalAddress());
                        view.setAlive(false);
                        if (coordinator.equals(member)) {
                            isCoordinatorAlive = false;
                            LOGGER.info("Cluster coordinator is dead {}", coordinator.getExternalAddress());
                        }
                    }
                }

                if (!isCoordinatorAlive) {
                    TreeSet<Member> members = registry.listMembers();
                    findDeadCoordinator(members, context.getMember());
                }

                if (coordinator != null && coordinator.equals(context.getMember())) {
                    for (Member member : knownMembers.keySet()) {
                        MemberView memberView = knownMembers.get(member);
                        if (!memberView.getAlive()) {
                            //unregisterMember(member);
                        }
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

    private class ClusterInitializationWatcher implements Runnable {

        @Override
        public void run() {
            if (isShutdown) {
                return;
            }

            // TODO: Add unwatch logic to shutdown

            DirectorySubspace subspace = MembershipUtils.createOrOpenClusterMetadataSubspace(context);
            byte[] key = subspace.pack(Tuple.from(MembershipConstants.CLUSTER_INITIALIZED));

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                CompletableFuture<Void> watcher = keyWatcher.watch(tr, key);
                tr.commit().join();
                try {
                    clusterInitialized = isClusterInitialized_internal();
                    watcher.join();
                } catch (CancellationException e) {
                    LOGGER.info("Cluster initialization watcher has been cancelled");
                    return;
                }
                clusterInitialized = isClusterInitialized_internal();
            } catch (Exception e) {
                LOGGER.error("Error while waiting for cluster initialization: {}", JournalName.clusterEvents(), e);
            } finally {
                if (!isShutdown && !clusterInitialized) {
                    // Try again
                    scheduler.execute(this);
                }
            }
        }
    }
}
