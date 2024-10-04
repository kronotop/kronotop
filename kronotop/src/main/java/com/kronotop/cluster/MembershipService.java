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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.*;
import com.kronotop.cluster.coordinator.CoordinatorService;
import com.kronotop.cluster.coordinator.Route;
import com.kronotop.cluster.coordinator.RoutingTable;
import com.kronotop.common.KronotopException;
import com.kronotop.journal.Event;
import com.kronotop.journal.JournalName;
import com.kronotop.network.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Membership service implements all business logic around cluster membership and health checks.
 */
public class MembershipService implements KronotopService {
    public static final String NAME = "Membership";
    private static final Logger LOGGER = LoggerFactory.getLogger(MembershipService.class);
    private final Context context;
    private final ScheduledThreadPoolExecutor scheduler;
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final AtomicReference<RoutingTable> routingTable = new AtomicReference<>();
    private final AtomicReference<Member> knownCoordinator = new AtomicReference<>();
    private final ConcurrentHashMap<Member, MemberView> knownMembers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Member, DirectorySubspace> memberSubspaces = new ConcurrentHashMap<>();
    private final AtomicReference<byte[]> latestClusterEventKey = new AtomicReference<>();
    private final CoordinatorService coordinatorService;
    private final int heartbeatInterval;
    private final int heartbeatMaximumSilentPeriod;
    private volatile boolean isShutdown;

    public MembershipService(Context context) {
        this.context = context;
        this.heartbeatInterval = context.getConfig().getInt("cluster.heartbeat.interval");
        this.heartbeatMaximumSilentPeriod = context.getConfig().getInt("cluster.heartbeat.maximum_silent_period");
        this.coordinatorService = context.getService(CoordinatorService.NAME);

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
    }

    private void removeMemberOnFDB(Member member) {
        List<String> subpath = ClusterLayout.getMemberlist(context).addAll(List.of(member.getId())).asList();
        try {
            context.getFoundationDB().run(tr -> DirectoryLayer.getDefault().remove(tr, subpath).join());
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                LOGGER.error("No such member exists {}", member.getId());
                return;
            }
            throw new KronotopException(e);
        }
    }

    /**
     * Unregisters a member from the cluster.
     *
     * @param member the member to unregister
     */
    private void unregisterMember(Member member) {
        removeMemberOnFDB(member);
        context.getJournal().getPublisher().publish(JournalName.clusterEvents(), new MemberLeftEvent(member));
        LOGGER.info("Member: {} has been unregistered", member.getId());
    }

    /**
     * Registers a member in the cluster by creating its directory subspace and storing its process ID.
     * Retries the registration if a retryable FDBException is encountered.
     *
     * @param member the member to be registered
     * @throws MemberAlreadyRegisteredException if the member is already registered or not gracefully stopped
     * @throws KronotopException                if an unexpected exception occurs during the registration process
     */
    private void registerMember_internal(Member member) {
        List<String> subpath = ClusterLayout.getMemberlist(context).addAll(List.of(member.getId())).asList();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {

            DirectorySubspace memberSubspace = DirectoryLayer.getDefault().create(tr, subpath).join();

            byte[] processIDKey = memberSubspace.pack(Keys.PROCESS_ID.toString());
            tr.set(processIDKey, context.getMember().getProcessId().getBytes());

            byte[] externalAddressKey = memberSubspace.pack(Keys.EXTERNAL_ADDRESS.toString());
            tr.set(externalAddressKey, member.getExternalAddress().toString().getBytes());

            byte[] internalAddressKey = memberSubspace.pack(Keys.INTERNAL_ADDRESS.toString());
            tr.set(internalAddressKey, member.getInternalAddress().toString().getBytes());

            tr.commit().join();

            memberSubspaces.put(context.getMember(), memberSubspace);
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new MemberAlreadyRegisteredException(String.format("Member: %s already registered or not gracefully stopped", member.getId()));
            }
            if (e.getCause() instanceof FDBException exception) {
                if (exception.isRetryable()) {
                    registerMember_internal(member);
                    return;
                }
            }
            throw new KronotopException(e);
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
        TreeSet<Member> members = getSortedMembers();
        try {
            Member coordinator = members.first();
            Member myself = context.getMember();
            if (coordinator.equals(myself)) {
                if (knownCoordinator.get() == null || !knownCoordinator.get().equals(myself)) {
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

    private void registerMember() {
        try {
            if (context.getConfig().hasPath("cluster.force_register")) {
                if (context.getConfig().getBoolean("cluster.force_register")) {
                    removeMemberOnFDB(context.getMember());
                }
            }
            registerMember_internal(context.getMember());
        } catch (MemberAlreadyRegisteredException e) {
            LOGGER.warn("Kronotop failed at initialization because Member: {} is already registered by a different cluster member.", context.getMember().getId());
            LOGGER.warn("If Member: {} is a dead member, the cluster coordinator will eventually remove it from the cluster after a grace period.", context.getMember().getId());
            LOGGER.warn("If you want to remove Member: {} from the cluster yourself, set JVM property -Dcluster.force_register=true and start the process again.", context.getMember().getId());
            LOGGER.warn("But this can be risky, you have been warned.");
            throw e;
        }
    }

    public void start() {
        registerMember();

        TreeSet<Member> members = getSortedMembers();
        for (Member member : members) {
            openMemberSubspace(member);
            long lastHeartbeat = getLatestHeartbeat(member);
            knownMembers.putIfAbsent(member, new MemberView(lastHeartbeat));
        }

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
    }

    private Member getMember(String id) {
        List<String> subpath = ClusterLayout.getMemberlist(context).addAll(List.of(id)).asList();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, subpath).join();
            byte[] processIDKey = subspace.pack(Keys.PROCESS_ID.toString());
            byte[] rawProcessID = tr.get(processIDKey).join();
            Versionstamp processID = Versionstamp.fromBytes(rawProcessID);

            byte[] externalAddressKey = subspace.pack(Keys.EXTERNAL_ADDRESS.toString());
            byte[] rawExternalAddress = tr.get(externalAddressKey).join();
            Address externalAddress = Address.parseString(new String(rawExternalAddress));

            byte[] internalAddressKey = subspace.pack(Keys.INTERNAL_ADDRESS.toString());
            byte[] rawInternalAddress = tr.get(internalAddressKey).join();
            Address internalAddress = Address.parseString(new String(rawInternalAddress));

            return new Member(id, externalAddress, internalAddress, processID);
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchMemberException(String.format("No such member: %s", id));
            }
            throw new KronotopException(e);
        } catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns a sorted set of registered cluster members. The members are sorted by process ID.
     * The process IDs are implemented as an atomically increased long integer on FoundationDB.
     *
     * @return sorted set of registered cluster members.
     */
    public TreeSet<Member> getSortedMembers() {
        List<String> memberIds = getMembers();
        TreeSet<Member> members = new TreeSet<>(Comparator.comparing(Member::getProcessId));
        for (String memberId : memberIds) {
            try {
                Member member = getMember(memberId);
                members.add(member);
            } catch (NoSuchMemberException e) {
                // Ignore it, it's just removed itself before we try to get it from FDB.
            }
        }
        return members;
    }

    /**
     * Returns a list of registered cluster members. The current status of member aliveness isn't guaranteed.
     *
     * @return a list of Member ids.
     */
    public List<String> getMembers() {
        List<String> subpath = ClusterLayout.getMemberlist(context).asList();
        try {
            return context.getFoundationDB().run(tr -> DirectoryLayer.getDefault().list(tr, subpath).join());
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                return List.of();
            }
            throw new KronotopException(e);
        }
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

        if (memberSubspaces.containsKey(context.getMember())) {
            unregisterMember(context.getMember());
        }
    }

    public Member getKnownCoordinator() {
        return knownCoordinator.get();
    }

    private void openMemberSubspace(Member member) {
        List<String> subpath = ClusterLayout.getMemberlist(context).addAll(List.of(member.getId())).asList();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, subpath).join();
            memberSubspaces.put(member, subspace);
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchMemberException(String.format("No such member: %s", member.getExternalAddress()));
            }
        }
    }

    private MemberJoinEvent processMemberJoinEvent(byte[] data) throws UnknownHostException {
        MemberJoinEvent event = JSONUtils.readValue(data, MemberJoinEvent.class);
        Member member = getMember(event.memberId());

        // A new cluster member has joined.
        openMemberSubspace(member);
        long lastHeartbeat = getLatestHeartbeat(member);
        knownMembers.putIfAbsent(member, new MemberView(lastHeartbeat));
        LOGGER.info("Member join: {}", member.getExternalAddress());

        return event;
    }

    private MemberLeftEvent processMemberLeftEvent(byte[] data) throws UnknownHostException {
        MemberLeftEvent event = JSONUtils.readValue(data, MemberLeftEvent.class);
        Member member = getMember(event.memberId());

        // A registered cluster member has left the cluster.
        memberSubspaces.remove(member);
        knownMembers.remove(member);
        LOGGER.info("Member left: {}", member.getExternalAddress());
        return event;
    }

    private BroadcastEvent processClusterEvent(Event event) throws UnknownHostException {
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
        unregisterMember(dead);

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

    public enum Keys {
        EXTERNAL_ADDRESS, INTERNAL_ADDRESS, PROCESS_ID, LAST_HEARTBEAT,
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
                    TreeSet<Member> members = getSortedMembers();
                    findDeadCoordinator(members, context.getMember());
                }

                if (coordinator != null && coordinator.equals(context.getMember())) {
                    for (Member member : knownMembers.keySet()) {
                        MemberView memberView = knownMembers.get(member);
                        if (!memberView.getAlive()) {
                            unregisterMember(member);
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
}
