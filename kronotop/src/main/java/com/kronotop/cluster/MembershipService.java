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

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
    private final AtomicReference<RoutingTable> routingTable = new AtomicReference<>();
    private final AtomicReference<Member> coordinator = new AtomicReference<>();
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
                DirectorySubspace subspace = openMemberSubspace(tr, member);
                long heartbeat = Heartbeat.get(tr, subspace);
                MemberView memberView = new MemberView(heartbeat);
                others.put(member, memberView);
                subspaces.put(member, subspace);
            }
        }
    }

    public void start() {
        Member member = context.getMember();
        member.setStatus(MemberStatus.RUNNING);
        if (!registry.isAdded(member.getId())) {
            registry.add(member);
            LOGGER.info("Member: {} has been registered", member.getId());
        } else {
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

        findClusterCoordinator();
    }

    public long getLatestHeartbeat(Member member) {
        MemberView memberView = others.get(member);
        if (memberView == null) {
            throw new IllegalArgumentException("Member: " + member.getId() + " not found");
        }
        return memberView.getLatestHeartbeat();
    }

    public TreeSet<Member> listMembers() {
        return registry.listMembers();
    }

    public boolean isClusterInitialized() {
        return clusterInitialized;
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
            member.setStatus(MemberStatus.STOPPED);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                registry.update(tr, member);
                context.getJournal().getPublisher().publish(tr, JournalName.clusterEvents(), new MemberLeftEvent(member));
                tr.commit().join();
            }
        }
    }

    private synchronized void findClusterCoordinator() {
        TreeSet<Member> members = registry.
                listMembers().
                stream().filter(member -> member.getStatus().equals(MemberStatus.RUNNING)).
                collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(Member::getProcessId))));
        try {
            Member assumedCoordinator = members.first();
            Member thisMember = context.getMember();
            if (assumedCoordinator.equals(thisMember)) {
                if (coordinator.get() == null || !coordinator.get().equals(thisMember)) {
                    LOGGER.info("Propagating myself as the cluster coordinator");
                }
            }
            coordinator.set(assumedCoordinator);
        } catch (NoSuchElementException e) {
            LOGGER.warn("No cluster coordinator found");
        }
    }

    public RoutingTable getRoutingTable() {
        return routingTable.get();
    }

    public Member getCoordinator() {
        return coordinator.get();
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
        try {
            switch (baseBroadcastEvent.kind()) {
                case MEMBER_JOIN -> processMemberJoinEvent(event.getValue());
                case MEMBER_LEFT -> processMemberLeftEvent(event.getValue());
                default -> throw new KronotopException("Unknown broadcast event: " + baseBroadcastEvent.kind());
            }
            ;
        } finally {
            if (baseBroadcastEvent.kind().equals(BroadcastEventKind.MEMBER_LEFT)) {
                findClusterCoordinator();
            }
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

    private void pruneDeadMembers(TreeSet<Member> members, Member dead) {
        Member closest = members.higher(dead);
        if (closest == null) {
            return;
        }
        MemberView memberView = others.get(closest);
        if (!memberView.getAlive()) {
            pruneDeadMembers(members, closest);
        }
    }

    private void findDeadCoordinator(TreeSet<Member> members, Member member) {
        Member closest = members.lower(member);
        if (closest == null) {
            return;
        }

        Member knownCoordinator = coordinator.get();
        MemberView memberView = others.get(closest);
        if (memberView.getAlive()) {
            return;
        }
        if (knownCoordinator.equals(closest)) {
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
                LOGGER.error("Error while running heartbeat task", e);
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
            boolean isCoordinatorAlive = true;
            Member knownCoordinator = coordinator.get();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                for (Member member : others.keySet()) {
                    DirectorySubspace subspace = subspaces.get(member);
                    long lastHeartbeat = Heartbeat.get(tr, subspace);
                    MemberView view = others.computeIfPresent(member, (m, memberView) -> {
                        if (memberView.getLatestHeartbeat() != lastHeartbeat) {
                            memberView.setLastHeartbeat(lastHeartbeat);
                        } else {
                            memberView.increaseExpectedHeartbeat();
                        }
                        return memberView;
                    });

                    if (view == null) {
                        continue;
                    }

                    long silentPeriod = view.getExpectedHeartbeat() - view.getLatestHeartbeat();
                    if (silentPeriod > maxSilentPeriod) {
                        LOGGER.warn("{} has been suspected to be dead", member.getExternalAddress());
                        view.setAlive(false);
                        if (knownCoordinator.equals(member)) {
                            isCoordinatorAlive = false;
                            LOGGER.info("Cluster coordinator is dead {}", knownCoordinator.getExternalAddress());
                        }
                    }
                }

                if (!isCoordinatorAlive) {
                    TreeSet<Member> members = registry.listMembers();
                    findDeadCoordinator(members, context.getMember());
                }

                if (knownCoordinator != null && knownCoordinator.equals(context.getMember())) {
                    for (Member member : others.keySet()) {
                        MemberView memberView = others.get(member);
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
