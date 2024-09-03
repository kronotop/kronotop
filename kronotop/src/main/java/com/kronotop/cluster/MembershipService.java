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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.*;
import com.kronotop.cluster.coordinator.Route;
import com.kronotop.cluster.coordinator.RoutingTable;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.ByteUtils;
import com.kronotop.journal.Event;
import com.kronotop.journal.JournalName;
import com.kronotop.network.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicReference<byte[]> lastClusterEventsVersionstamp = new AtomicReference<>();
    private final int heartbeatInterval;
    private final int heartbeatMaximumSilentPeriod;
    private final AtomicBoolean isBootstrapped = new AtomicBoolean();
    private volatile boolean isShutdown;

    public MembershipService(Context context) {
        this.context = context;
        this.heartbeatInterval = context.getConfig().getInt("cluster.heartbeat.interval");
        this.heartbeatMaximumSilentPeriod = context.getConfig().getInt("cluster.heartbeat.maximum_silent_period");

        // TODO: CLUSTER-REFACTORING
        RoutingTable table = new RoutingTable();
        int numberOfShards = context.getConfig().getInt("cluster.number_of_shards");
        for (int i = 0; i < numberOfShards; i++) {
            table.setRoute(i, new Route(context.getMember()));
        }
        routingTable.set(table);
        knownCoordinator.set(context.getMember());
        // TODO: CLUSTER-REFACTORING

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("kr.membership-%d").build();
        this.scheduler = new ScheduledThreadPoolExecutor(2, namedThreadFactory);
    }

    public void waitUntilBootstrapped() throws InterruptedException {
        // TODO: CLUSTER-REFACTORING
        /*synchronized (isBootstrapped) {
            if (isBootstrapped.get()) {
                return;
            }
            LOGGER.info("Waiting to be bootstrapped");
            isBootstrapped.wait();
        }*/
    }

    private void removeMemberOnFDB(Address address) {
        List<String> subpath = ClusterLayout.getMemberlist(context).addAll(List.of(address.toString())).asList();
        try {
            context.getFoundationDB().run(tr -> DirectoryLayer.getDefault().remove(tr, subpath).join());
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                LOGGER.error("No such member exists {}", address);
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
        removeMemberOnFDB(member.getAddress());
        MemberLeftEvent memberLeftEvent = new MemberLeftEvent(member.getAddress().getHost(), member.getAddress().getPort(), member.getProcessId());
        try {
            BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.MEMBER_LEFT, new ObjectMapper().writeValueAsString(memberLeftEvent));
            context.getJournal().getPublisher().publish(JournalName.clusterEvents(), broadcastEvent);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error while creating an event with type: {} for journal: {}", EventTypes.MEMBER_LEFT, JournalName.clusterEvents());
            throw new KronotopException(e);
        }

        LOGGER.info("{} has been unregistered", member.getAddress());
    }

    /**
     * Registers a member to the cluster by creating a directory for the member's address
     * and storing the member ID in the directory.
     *
     * @param address The address of the member to register
     */
    private void registerMember(Address address) {
        List<String> subpath = ClusterLayout.getMemberlist(context).addAll(List.of(address.toString())).asList();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {

            DirectorySubspace directorySubspace = DirectoryLayer.getDefault().create(tr, subpath).join();
            byte[] processIDKey = directorySubspace.pack(Keys.PROCESS_ID.toString());
            tr.set(processIDKey, context.getMember().getProcessId().getBytes());
            tr.commit().join();

            memberSubspaces.put(context.getMember(), directorySubspace);
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new MemberAlreadyRegisteredException(String.format("%s already registered or not gracefully stopped", address));
            }
            if (e.getCause() instanceof FDBException exception) {
                if (exception.isRetryable()) {
                    registerMember(address);
                    return;
                }
            }
            throw new KronotopException(e);
        }
    }

    /**
     * Initializes the cluster by adding the current member, registering the member's address,
     * executing background tasks, and publishing a member join event to the cluster's journal.
     * This method is private and does not return any value.
     */
    private void initialize() {
        Address address = context.getMember().getAddress();
        try {
            if (context.getConfig().hasPath("cluster.force_register")) {
                if (context.getConfig().getBoolean("cluster.force_register")) {
                    removeMemberOnFDB(address);
                }
            }
            registerMember(address);
        } catch (MemberAlreadyRegisteredException e) {
            LOGGER.warn("Kronotop failed at initialization because {} is already registered by a different cluster member.", context.getMember().getAddress());
            LOGGER.warn("If {} is a dead member, the cluster coordinator will eventually remove it from the cluster after a grace period.", context.getMember().getAddress());
            LOGGER.warn("If you want to remove {} from the cluster yourself, set JVM property -Dcluster.force_register=true and start the process again.", context.getMember().getAddress());
            LOGGER.warn("But this can be risky, you have been warned.");
            throw e;
        }

        scheduler.execute(new ClusterEventsJournalWatcher());
        scheduler.execute(new HeartbeatTask());
        scheduler.execute(new FailureDetectionTask());

        Member member = context.getMember();
        MemberJoinEvent memberJoinEvent = new MemberJoinEvent(member.getAddress().getHost(), member.getAddress().getPort(), member.getProcessId());
        try {
            BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.MEMBER_JOIN, new ObjectMapper().writeValueAsString(memberJoinEvent));
            context.getJournal().getPublisher().publish(JournalName.clusterEvents(), broadcastEvent);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error while creating an event with type: {} for journal: {}", EventTypes.MEMBER_JOIN, JournalName.clusterEvents());
            throw new KronotopException(e);
        }

        LOGGER.info("{} has been registered", address);
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
                throw new NoSuchMemberException(String.format("No such member: %s", member.getAddress()));
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
    private synchronized void checkCluster() {
        TreeSet<Member> members = getSortedMembers();
        Member coordinator = members.first();
        Member myself = context.getMember();
        if (coordinator.equals(myself)) {
            if (knownCoordinator.get() == null || !knownCoordinator.get().equals(myself)) {
                LOGGER.info("Propagating myself as the cluster coordinator");
            }
        }
        knownCoordinator.set(coordinator);
    }

    public RoutingTable getRoutingTable() {
        return routingTable.get();
    }

    /**
     * Registers the newly created member on both FoundationDB and local consistent hash ring
     * then initiates all background tasks.
     */
    public void start() {
        byte[] key = context.getFoundationDB().run(tr -> context.getJournal().getConsumer().getLatestEventKey(tr, JournalName.clusterEvents()));
        this.lastClusterEventsVersionstamp.set(key);

        // Register the member on both FoundationDB and the local consistent hash ring.
        initialize();

        // Continue filling the local consistent hash ring and creating a queryable
        // record of alive cluster members.
        TreeSet<Member> members = getSortedMembers();
        for (Member member : members) {
            openMemberSubspace(member);
            long lastHeartbeat = getLatestHeartbeat(member);
            knownMembers.putIfAbsent(member, new MemberView(lastHeartbeat));
        }

        // Schedule the periodic tasks here.
        scheduler.schedule(new CheckClusterTask(), 0, TimeUnit.NANOSECONDS);
    }

    /**
     * Retrieves a member by its address from the cluster.
     *
     * @param address The address of the member.
     * @return The member with the specified address.
     * @throws NoSuchMemberException If the member does not exist.
     */
    private Member getMemberByAddress(Address address) {
        List<String> subpath = ClusterLayout.getMemberlist(context).addAll(List.of(address.toString())).asList();
        try {
            return context.getFoundationDB().run(tr -> {
                DirectorySubspace directorySubspace = DirectoryLayer.getDefault().open(tr, subpath).join();
                byte[] processIDKey = directorySubspace.pack(Keys.PROCESS_ID.toString());
                byte[] rawProcessID = tr.get(processIDKey).join();
                Versionstamp processID = Versionstamp.fromBytes(rawProcessID);
                return new Member(address, processID);
            });
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchMemberException(String.format("No such member: %s", address));
            }
            throw new KronotopException(e);
        }
    }

    /**
     * Retrieves a member by its address and process ID from the cluster.
     *
     * @param address   The address of the member.
     * @param processId The process ID of the member.
     * @return The member with the specified address and process ID.
     * @throws NoSuchMemberException If the member does not exist.
     */
    public Member getMember(Address address, Versionstamp processId) {
        List<String> subpath = ClusterLayout.getMemberlist(context).addAll(List.of(address.toString())).asList();
        try {
            return context.getFoundationDB().run(tr -> {
                DirectorySubspace directorySubspace = DirectoryLayer.getDefault().open(tr, subpath).join();
                byte[] processIDKey = directorySubspace.pack(Keys.PROCESS_ID.toString());
                byte[] rawProcessID = tr.get(processIDKey).join();
                if (!processId.equals(Versionstamp.fromBytes(rawProcessID))) {
                    throw new NoSuchMemberException(String.format("No such member: %s with processId: %s", address, VersionstampUtils.base64Encode(processId)));
                }
                return new Member(address, processId);
            });
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchMemberException(String.format("No such member: %s", address));
            } else if (e.getCause() instanceof NoSuchMemberException) {
                throw new NoSuchMemberException(e.getCause().getMessage());
            }
            throw new KronotopException(e);
        }
    }

    /**
     * Returns a sorted set of registered cluster members. The members are sorted by process ID.
     * The process IDs are implemented as an atomically increased long integer on FoundationDB.
     *
     * @return sorted set of registered cluster members.
     */
    public TreeSet<Member> getSortedMembers() {
        List<String> addresses = getMembers();
        TreeSet<Member> members = new TreeSet<>(Comparator.comparing(Member::getProcessId));
        for (String hostPort : addresses) {
            try {
                Address address = Address.parseString(hostPort);
                // TODO: Use a single transaction
                Member member = getMemberByAddress(address);
                members.add(member);
            } catch (UnknownHostException e) {
                LOGGER.error("Unknown host: {}, {}", hostPort, e.getMessage());
            }
        }
        return members;
    }

    /**
     * Returns a list of registered cluster members. The current status of member aliveness isn't guaranteed.
     *
     * @return a list of string that contains the members in host:port format.
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
                LOGGER.warn("MembershipService cannot be stopped gracefully");
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
        List<String> subpath = ClusterLayout.getMemberlist(context).addAll(List.of(member.getAddress().toString())).asList();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, subpath).join();
            memberSubspaces.put(member, subspace);
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchMemberException(String.format("No such member: %s", member.getAddress()));
            }
        }
    }

    /**
     * Processes a MemberEvent, updating the cluster membership and performing related tasks.
     *
     * @param event the MemberEvent to process
     * @throws UnknownHostException if there is an error with the host address
     */
    private void processMemberEvent(@Nonnull BroadcastEvent event) throws UnknownHostException {
        MemberEvent memberEvent;

        if (event.getType().equals(EventTypes.MEMBER_JOIN)) {
            memberEvent = JSONUtils.readValue(event.getPayload(), MemberJoinEvent.class);
        } else {
            memberEvent = JSONUtils.readValue(event.getPayload(), MemberLeftEvent.class);
        }

        Address address = new Address(memberEvent.getHost(), memberEvent.getPort());
        Member member = new Member(address, memberEvent.getProcessID());

        if (!member.equals(context.getMember())) {
            // A new cluster member has joined.
            if (memberEvent instanceof MemberJoinEvent) {
                openMemberSubspace(member);
                long lastHeartbeat = getLatestHeartbeat(member);
                knownMembers.putIfAbsent(member, new MemberView(lastHeartbeat));
                LOGGER.info("Member join: {}", member.getAddress());
            } else {
                // A registered cluster member has left the cluster.
                memberSubspaces.remove(member);
                knownMembers.remove(member);
                LOGGER.info("Member left: {}", member.getAddress());
            }
        }

        // Try to find a cluster coordinator. If the coordinator is this node itself, propagate
        // the current routing table.
        checkCluster();
        Member coordinator = knownCoordinator.get();
        if (coordinator.equals(context.getMember())) {
            LOGGER.debug("Checking shard ownership...");
        }
    }

    private void processUpdateRoutingTable(@Nonnull BroadcastEvent broadcastEvent) {
        RoutingTable newRoutingTable = JSONUtils.readValue(broadcastEvent.getPayload(), RoutingTable.class);
        if (!isBootstrapped.get()) {
            isBootstrapped.set(true);
            LOGGER.info("Bootstrapped by the cluster coordinator: {}", newRoutingTable.getCoordinator());
            synchronized (isBootstrapped) {
                isBootstrapped.notifyAll();
            }
        }
        RoutingTable oldRoutingTable = routingTable.get();
        routingTable.set(newRoutingTable);
        LOGGER.debug("Routing table has been updated");
    }

    /**
     * fetchClusterEvents tries to fetch the latest events from the cluster's global journal and processes them.
     */
    private synchronized void fetchClusterEvents() {
        context.getFoundationDB().run(tr -> {
            while (true) {
                // Try to consume the latest event.
                Event event = context.getJournal().getConsumer().consumeNext(tr, JournalName.clusterEvents(), lastClusterEventsVersionstamp.get());
                if (event == null) return null;

                try {
                    BroadcastEvent broadcastEvent = JSONUtils.readValue(event.getValue(), BroadcastEvent.class);
                    LOGGER.debug("Received broadcast event: {}", broadcastEvent.getType());

                    switch (broadcastEvent.getType()) {
                        case MEMBER_JOIN:
                        case MEMBER_LEFT:
                            processMemberEvent(broadcastEvent);
                            break;
                        case UPDATE_ROUTING_TABLE:
                            processUpdateRoutingTable(broadcastEvent);
                            break;
                        default:
                            LOGGER.error("Unknown broadcast event type: {}", broadcastEvent.getType());
                    }
                } catch (Exception e) {
                    LOGGER.error("Failed to process a broadcast event, passing it", e);
                }

                // Processed the event successfully. Forward the position.
                lastClusterEventsVersionstamp.set(event.getKey());
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
        PROCESS_ID, LAST_HEARTBEAT,
    }

    /**
     * A private class that implements the Runnable interface.
     * This task is responsible for finding the cluster coordinator.
     */
    private class CheckClusterTask implements Runnable {
        @Override
        public void run() {
            if (isShutdown) {
                return;
            }
            try {
                checkCluster();
            } catch (Exception e) {
                LOGGER.error("Error while check cluster task", e);
            } finally {
                if (!isShutdown) {
                    scheduler.schedule(this, 15, TimeUnit.SECONDS);
                }
            }
        }
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
                        LOGGER.warn("{} has been suspected to be dead", member.getAddress());
                        view.setAlive(false);
                        if (coordinator.equals(member)) {
                            isCoordinatorAlive = false;
                            LOGGER.info("Cluster coordinator is dead {}", coordinator.getAddress());
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
