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
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.*;
import com.kronotop.cluster.handlers.KrAdminHandler;
import com.kronotop.common.KronotopException;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.journal.Event;
import com.kronotop.journal.JournalName;
import com.kronotop.server.ServerKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Membership service implements all business logic around cluster membership and health checks.
 */
public class MembershipService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "Membership";
    private static final Logger LOGGER = LoggerFactory.getLogger(MembershipService.class);
    private final Context context;
    private final MemberRegistry registry;
    private final ScheduledThreadPoolExecutor scheduler;
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final ConcurrentHashMap<Member, DirectorySubspace> subspaces = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Member, MemberView> others = new ConcurrentHashMap<>();
    private final AtomicReference<RoutingTableLegacy> routingTableLegacy = new AtomicReference<>();
    private final AtomicReference<RoutingTable> routingTable = new AtomicReference<>();
    private final AtomicReference<byte[]> latestClusterEventKey = new AtomicReference<>();
    private final int heartbeatInterval;
    private final int heartbeatMaximumSilentPeriod;
    private volatile boolean isShutdown;
    private volatile boolean clusterInitialized;

    public MembershipService(Context context) {
        super(context);

        this.context = context;
        this.registry = new MemberRegistry(context);
        this.heartbeatInterval = context.getConfig().getInt("cluster.heartbeat.interval");
        this.heartbeatMaximumSilentPeriod = context.getConfig().getInt("cluster.heartbeat.maximum_silent_period");

        // TODO: CLUSTER-REFACTORING
        RoutingTableLegacy table = new RoutingTableLegacy();
        int numberOfShards = context.getConfig().getInt("redis.shards");
        for (int i = 0; i < numberOfShards; i++) {
            table.setRoute(i, new RouteLegacy(context.getMember()));
        }
        routingTableLegacy.set(table);
        // TODO: CLUSTER-REFACTORING

        ThreadFactory factory = Thread.ofVirtual().name("kr.membership").factory();
        this.scheduler = new ScheduledThreadPoolExecutor(3, factory);

        handlerMethod(ServerKind.INTERNAL, new KrAdminHandler(this));
    }

    private void configureClusterEventsWatcher() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] key = context.getJournal().getConsumer().getLatestEventKey(tr, JournalName.clusterEvents());
            this.latestClusterEventKey.set(key);
        }
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
        context.getJournal().getPublisher().publish(JournalName.clusterEvents(), new MemberJoinEvent(member));
        configureClusterEventsWatcher();

        scheduler.execute(new ClusterEventsJournalWatcher());
        scheduler.execute(new HeartbeatTask());
        scheduler.execute(new FailureDetectionTask());

        clusterInitialized = isClusterInitialized_internal();
        if (!clusterInitialized) {
            scheduler.execute(new ClusterInitializationWatcher());
        }
    }

    public RoutingTable getRoutingTable() {
        return routingTable.get();
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
    public void updateMember(Member member) {
        registry.update(member);
    }

    /**
     * Removes the member identified by the provided memberId from the system.
     *
     * @param memberId the unique identifier of the member to be removed
     * @throws KronotopException if the member is in RUNNING status and cannot be removed
     */
    public void removeMember(String memberId) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Member member = registry.findMember(tr, memberId);
            if (member.getStatus().equals(MemberStatus.RUNNING)) {
                throw new KronotopException("Member in " + MemberStatus.RUNNING + " status cannot be removed");
            }
            registry.remove(tr, memberId);
            tr.commit().join();
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

    public Map<Member, MemberView> getOthers() {
        return Collections.unmodifiableMap(others);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Context getContext() {
        return context;
    }

    @Override
    public void shutdown() {
        try {
            isShutdown = true;
            keyWatcher.unwatchAll();
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

    private void updateMemberStatusAndLeftCluster(Member member, MemberStatus status) {
        MemberStatus initialStatus = member.getStatus();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            member.setStatus(status);
            registry.update(tr, member);
            context.getJournal().getPublisher().publish(tr, JournalName.clusterEvents(), new MemberLeftEvent(member));
            tr.commit().join();
        } catch (Exception e) {
            // Rollback the internal state
            member.setStatus(initialStatus);
        }
    }

    public RoutingTableLegacy getRoutingTableLegacy() {
        return routingTableLegacy.get();
    }

    private void processMemberJoinEvent(byte[] data) {
        MemberJoinEvent event = JSONUtils.readValue(data, MemberJoinEvent.class);
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
        MemberLeftEvent event = JSONUtils.readValue(data, MemberLeftEvent.class);
        Member member = registry.findMember(event.memberId());
        subspaces.remove(member);
        others.remove(member);

        LOGGER.info("Member left: {}", member.getExternalAddress());
    }

    private void processClusterEvent(Event event) {
        BaseBroadcastEvent baseBroadcastEvent = JSONUtils.readValue(event.getValue(), BaseBroadcastEvent.class);
        LOGGER.debug("Received broadcast event: {}", baseBroadcastEvent.kind());
        switch (baseBroadcastEvent.kind()) {
            case MEMBER_JOIN -> processMemberJoinEvent(event.getValue());
            case MEMBER_LEFT -> processMemberLeftEvent(event.getValue());
            default -> throw new KronotopException("Unknown broadcast event: " + baseBroadcastEvent.kind());
        }
    }

    private synchronized void fetchClusterEvents() {
        context.getFoundationDB().run(tr -> {
            while (true) {
                // Try to consume the latest event.
                byte[] key = latestClusterEventKey.get();
                Event event = context.getJournal().getConsumer().consumeNext(tr, JournalName.clusterEvents(), key);
                if (event == null) return null;

                try {
                    processClusterEvent(event);
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
     * Checks if the cluster has been initialized by verifying a specific key
     * in the FoundationDB cluster metadata subspace.
     *
     * @return true if the cluster is initialized, otherwise false.
     */
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

    private class ClusterInitializationWatcher implements Runnable {

        @Override
        public void run() {
            if (isShutdown) {
                return;
            }

            DirectorySubspace subspace = MembershipUtils.createOrOpenClusterMetadataSubspace(context);
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
