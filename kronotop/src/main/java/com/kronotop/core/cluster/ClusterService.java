/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.core.cluster;

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.ByteUtils;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import com.kronotop.core.KronotopService;
import com.kronotop.core.cluster.consistent.Consistent;
import com.kronotop.core.cluster.journal.Journal;
import com.kronotop.core.cluster.journal.JournalItem;
import com.kronotop.core.network.Address;
import com.kronotop.redis.storage.LogicalDatabase;
import com.kronotop.redis.storage.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cluster service implements all business logic around cluster membership and health checks.
 */
public class ClusterService implements KronotopService {
    public static final String NAME = "Cluster";
    private static final Logger logger = LoggerFactory.getLogger(ClusterService.class);
    private final Context context;
    private final ScheduledThreadPoolExecutor scheduler;
    private final AtomicReference<CompletableFuture<Void>> currentWatcher = new AtomicReference<>();
    private final Consistent consistent;
    private final Journal journal;
    private final RoutingTable routingTable = new RoutingTable();
    private final AtomicLong lastOffset;
    private final AtomicReference<Member> knownCoordinator = new AtomicReference<>();
    private final ConcurrentHashMap<Member, MemberView> knownMembers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Member, DirectorySubspace> subspaces = new ConcurrentHashMap<>();
    private final int heartbeatInterval;
    private final int heartbeatMaximumSilentPeriod;
    private final AtomicBoolean isBootstrapped = new AtomicBoolean();
    private final LinkedBlockingQueue<PartitionEvent> partitionEventQueue = new LinkedBlockingQueue<>();
    private volatile boolean isShutdown;

    public ClusterService(Context context) {
        this.context = context;
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("kr.cluster-%d").build();
        this.scheduler = new ScheduledThreadPoolExecutor(2, namedThreadFactory);
        this.consistent = new Consistent(context.getConfig());
        this.journal = new Journal(context, "cluster-events");
        this.lastOffset = new AtomicLong(this.journal.getLastIndex());
        this.heartbeatInterval = context.getConfig().getInt("cluster.heartbeat.interval");
        this.heartbeatMaximumSilentPeriod = context.getConfig().getInt("cluster.heartbeat.maximum_silent_period");
    }

    public LinkedBlockingQueue<PartitionEvent> getPartitionEventQueue() {
        return partitionEventQueue;
    }

    public void waitUntilBootstrapped() throws InterruptedException {
        synchronized (isBootstrapped) {
            if (isBootstrapped.get()) {
                return;
            }
            logger.info("Waiting to be bootstrapped");
            isBootstrapped.wait();
        }
    }

    private DirectoryLayout getLayout(Address address) {
        List<String> list = Collections.singletonList(address.toString());
        return DirectoryLayout.Builder.
                clusterName(context.getClusterName()).
                internal().
                cluster().
                memberlist().
                addAll(list);
    }

    private void unregisterMember(Member member) {
        Address address = member.getAddress();
        List<String> subpath = getLayout(address).asList();
        try {
            context.getFoundationDB().run(tr -> DirectoryLayer.getDefault().remove(tr, subpath).join());
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                logger.error("No such member exists {}", address);
                return;
            }
            throw new KronotopException(e);
        }

        MemberLeftEvent memberLeftEvent = new MemberLeftEvent(
                member.getAddress().getHost(),
                member.getAddress().getPort(),
                member.getProcessId()
        );
        try {
            BroadcastEvent broadcastEvent =
                    new BroadcastEvent(
                            EventTypes.MEMBER_LEFT,
                            new ObjectMapper().writeValueAsString(memberLeftEvent)
                    );
            byte[] event = new ObjectMapper().writeValueAsBytes(broadcastEvent);
            journal.publish(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        logger.info("{} has been unregistered", address);
    }

    private void registerMember() {
        consistent.addMember(context.getMember());
        Address address = context.getMember().getAddress();
        List<String> subpath = getLayout(address).asList();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace directorySubspace = DirectoryLayer.getDefault().create(tr, subpath).join();
            byte[] processIDKey = directorySubspace.pack(Keys.PROCESS_ID.toString());
            tr.set(processIDKey, ByteUtils.fromLong(context.getMember().getProcessId()));
            tr.commit().join();
            subspaces.put(context.getMember(), directorySubspace);
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new MemberAlreadyRegisteredException(String.format("%s already registered or not gracefully stopped", address));
            }
            throw new KronotopException(e);
        }

        scheduler.execute(new BroadcastListener());
        scheduler.execute(new HeartbeatTask());
        scheduler.execute(new FailureDetectionTask());

        Member member = context.getMember();
        MemberJoinEvent memberJoinEvent = new MemberJoinEvent(
                member.getAddress().getHost(),
                member.getAddress().getPort(),
                member.getProcessId()
        );
        try {
            BroadcastEvent broadcastEvent =
                    new BroadcastEvent(
                            EventTypes.MEMBER_JOIN,
                            new ObjectMapper().writeValueAsString(memberJoinEvent)
                    );
            byte[] event = new ObjectMapper().writeValueAsBytes(broadcastEvent);
            journal.publish(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        logger.info("{} has been registered", address);
    }

    private synchronized void propagateRoutingTable() {
        HashMap<Integer, Member> newRoutingTable = new HashMap<>();
        int partitionCount = context.getConfig().getInt("cluster.partition_count");
        for (int partId = 0; partId < partitionCount; partId++) {
            Member owner = consistent.getPartitionOwner(partId);
            newRoutingTable.put(partId, owner);
        }

        UpdateRoutingTableEvent updateRoutingTableEvent = new UpdateRoutingTableEvent(context.getMember(), newRoutingTable);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            BroadcastEvent broadcastEvent = new BroadcastEvent(
                    EventTypes.UPDATE_ROUTING_TABLE,
                    objectMapper.writeValueAsString(updateRoutingTableEvent)
            );
            byte[] encodedEvent = objectMapper.writeValueAsBytes(broadcastEvent);
            journal.publish(encodedEvent);
            logger.debug("Routing table has been published");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private long getLong(byte[] val) {
        ByteBuffer b = ByteBuffer.allocate(8);
        b.order(ByteOrder.LITTLE_ENDIAN);
        b.put(val);
        return b.getLong(0);
    }

    private long getLastHeartbeat(Transaction tr, Member member) {
        long lastHeartbeat = 0;
        try {
            DirectorySubspace subspace = subspaces.get(member);
            byte[] rawLastHeartbeat = tr.get(subspace.pack(Keys.LAST_HEARTBEAT.toString())).join();
            lastHeartbeat = getLong(rawLastHeartbeat);
        } catch (CompletionException e) {
            if (!(e.getCause() instanceof NoSuchDirectoryException)) {
                throw new NoSuchMemberException(String.format("No such member: %s", member.getAddress()));
            }
        }
        return lastHeartbeat;
    }

    private long getLastHeartbeat(Member member) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return getLastHeartbeat(tr, member);
        }
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
                logger.info("Propagating myself as the cluster coordinator");
            }
        }
        knownCoordinator.set(coordinator);
    }

    public RoutingTable getRoutingTable() {
        return routingTable;
    }

    /**
     * Registers the newly created member on both FoundationDB and local consistent hash ring
     * then initiates all background tasks.
     */
    public void start() {
        // Register the member on both FoundationDB and the local consistent hash ring.
        registerMember();

        // Continue filling the local consistent hash ring and creating a queryable
        // record of alive cluster members.
        TreeSet<Member> members = getSortedMembers();
        for (Member member : members) {
            openMemberSubspace(member);
            long lastHeartbeat = getLastHeartbeat(member);
            knownMembers.putIfAbsent(member, new MemberView(lastHeartbeat));
            consistent.addMember(member);
        }

        // Schedule the periodic tasks here.
        scheduler.schedule(new CheckClusterTask(), 0, TimeUnit.NANOSECONDS);
    }

    private Member getMemberByAddress(Address address) {
        List<String> subpath = getLayout(address).asList();
        try {
            return context.getFoundationDB().run(tr -> {
                DirectorySubspace directorySubspace = DirectoryLayer.getDefault().open(tr, subpath).join();
                byte[] processIDKey = directorySubspace.pack(Keys.PROCESS_ID.toString());
                byte[] rawProcessID = tr.get(processIDKey).join();
                long processID = ByteUtils.toLong(rawProcessID);
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
     * Returns a sorted set of registered cluster members. The members are sorted by process ID.
     * The process IDs are implemented as an atomically increased long integer on FoundationDB.
     *
     * @return sorted set of registered cluster members.
     */
    private TreeSet<Member> getSortedMembers() {
        List<String> addresses = getMembers();
        TreeSet<Member> members = new TreeSet<>(Comparator.comparingLong(Member::getProcessId));
        for (String hostPort : addresses) {
            try {
                Address address = Address.parseString(hostPort);
                Member member = getMemberByAddress(address);
                members.add(member);
            } catch (UnknownHostException e) {
                logger.error("Unknown host: {}, {}", hostPort, e.getMessage());
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
        List<String> subpath = DirectoryLayout.Builder.
                clusterName(context.getClusterName()).
                internal().
                cluster().
                memberlist().asList();
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
        currentWatcher.get().cancel(true);
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(6, TimeUnit.SECONDS)) {
                logger.warn("ClusterService cannot be stopped gracefully");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (subspaces.containsKey(context.getMember())) {
            unregisterMember(context.getMember());
        }
    }

    private void openMemberSubspace(Member member) {
        List<String> subpath = getLayout(member.getAddress()).asList();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, subpath).join();
            subspaces.put(member, subspace);
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchMemberException(String.format("No such member: %s", member.getAddress()));
            }
        }
    }

    private void processMemberEvent(BroadcastEvent event) throws JsonProcessingException, UnknownHostException {
        ObjectMapper objectMapper = new ObjectMapper();
        MemberEvent memberEvent;
        if (event.getType().equals(EventTypes.MEMBER_JOIN)) {
            memberEvent = objectMapper.readValue(event.getPayload(), MemberJoinEvent.class);
        } else {
            memberEvent = objectMapper.readValue(event.getPayload(), MemberLeftEvent.class);
        }

        Address address = new Address(memberEvent.getHost(), memberEvent.getPort());
        Member member = new Member(address, memberEvent.getProcessID());

        if (!member.equals(context.getMember())) {
            // A new cluster member has joined.
            if (memberEvent instanceof MemberJoinEvent) {
                openMemberSubspace(member);
                long lastHeartbeat = getLastHeartbeat(member);
                knownMembers.putIfAbsent(member, new MemberView(lastHeartbeat));
                consistent.addMember(member);
                logger.info("Member join: {}", member.getAddress());
            } else {
                // A registered cluster member has left the cluster.
                subspaces.remove(member);
                knownMembers.remove(member);
                consistent.removeMember(member);
                logger.info("Member left: {}", member.getAddress());
            }
        }

        // Try to find a cluster coordinator. If the coordinator is this node itself, propagate
        // the current routing table.
        checkCluster();
        Member coordinator = knownCoordinator.get();
        if (coordinator.equals(context.getMember())) {
            propagateRoutingTable();
        }
    }

    private void checkPartitionOwnership() {
        int partitionCount = context.getConfig().getInt("cluster.partition_count");
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            Member owner = routingTable.getPartitionOwner(partitionId);
            Partition partition = context.getLogicalDatabase().getPartitions().get(partitionId);
            if (partition == null) {
                if (owner.equals(context.getMember())) {
                    // Take over the partition
                    partitionEventQueue.add(new PartitionEvent(LogicalDatabase.NAME, partitionId));
                }
                continue;
            }

            // Potential previous owner
            Member knownOwner = partition.getOwner();
            if (!knownOwner.equals(context.getMember())) {
                continue;
            }
            // Membership changed
            if (knownOwner.equals(owner)) {
                partitionEventQueue.add(new PartitionEvent(LogicalDatabase.NAME, partitionId));
            }
        }
    }

    /**
     * fetchBroadcastEvents tries to fetch the latest events from the cluster's global journal and processes them.
     */
    private synchronized void fetchBroadcastEvents() {
        context.getFoundationDB().run(tr -> {
            while (true) {
                // Try to consume the latest event.
                JournalItem journalItem = journal.consume(lastOffset.get() + 1);
                if (journalItem == null)
                    // There is nothing to process
                    return null;

                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    BroadcastEvent event = objectMapper.readValue(journalItem.getValue(), BroadcastEvent.class);
                    logger.debug("Received broadcast event: {}", event.getType());
                    if (event.getType().equals(EventTypes.MEMBER_JOIN) || event.getType().equals(EventTypes.MEMBER_LEFT)) {
                        processMemberEvent(event);
                    } else if (event.getType().equals(EventTypes.UPDATE_ROUTING_TABLE)) {
                        UpdateRoutingTableEvent updateRoutingTableEvent = objectMapper.readValue(event.getPayload(), UpdateRoutingTableEvent.class);
                        if (!isBootstrapped.get()) {
                            isBootstrapped.set(true);
                            logger.info("Bootstrapped by the cluster coordinator: {}", updateRoutingTableEvent.getCoordinator().getAddress());
                            synchronized (isBootstrapped) {
                                isBootstrapped.notifyAll();
                            }
                        }

                        routingTable.setRoutingTable(updateRoutingTableEvent.getRoutingTable());
                        checkPartitionOwnership();
                        logger.debug("Routing table has been updated");
                    }
                } catch (Exception e) {
                    logger.error("Failed to process a broadcast event", e);
                }

                // Processed the event successfully. Forward the offset.
                lastOffset.set(journalItem.getOffset());
            }
        });
    }

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
        PROCESS_ID,
        LAST_HEARTBEAT,
    }

    private class CheckClusterTask implements Runnable {
        @Override
        public void run() {
            if (isShutdown) {
                return;
            }
            checkCluster();
            scheduler.schedule(this, 15, TimeUnit.SECONDS);
        }
    }

    private class BroadcastListener implements Runnable {
        @Override
        public void run() {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                if (isShutdown) {
                    return;
                }
                CompletableFuture<Void> watcher = tr.watch(journal.getBroadcastKey());
                tr.commit().join();
                currentWatcher.set(watcher);
                try {
                    watcher.join();
                } catch (CancellationException e) {
                    logger.debug("Broadcast watcher has been cancelled");
                    return;
                }

                scheduler.execute(this);
                fetchBroadcastEvents();
            }
        }
    }

    private class HeartbeatTask implements Runnable {
        @Override
        public void run() {
            if (isShutdown) {
                return;
            }
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ByteBuffer b = ByteBuffer.allocate(8);
                b.order(ByteOrder.LITTLE_ENDIAN);
                b.putLong(1L);

                DirectorySubspace subspace = subspaces.get(context.getMember());
                tr.mutate(MutationType.ADD, subspace.pack(Keys.LAST_HEARTBEAT.toString()), b.array());
                tr.commit().join();
            }
            scheduler.schedule(this, heartbeatInterval, TimeUnit.SECONDS);
        }
    }

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
                    long lastHeartbeat = getLastHeartbeat(tr, member);
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
                        logger.warn("{} has been suspected to be dead", member.getAddress());
                        view.setAlive(false);
                        if (coordinator.equals(member)) {
                            isCoordinatorAlive = false;
                            logger.info("Cluster coordinator is dead {}", coordinator.getAddress());
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
                logger.error("Error while running failure detection task: {}", e.getMessage());
                throw e;
            }
            scheduler.schedule(this, heartbeatInterval, TimeUnit.SECONDS);
        }
    }
}
