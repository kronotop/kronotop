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

package com.kronotop.cluster.coordinator;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.ClusterLayout;
import com.kronotop.Context;
import com.kronotop.KeyWatcher;
import com.kronotop.KronotopService;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MembershipService;
import com.kronotop.cluster.NoSuchMemberException;
import com.kronotop.cluster.consistent.Consistent;
import com.kronotop.cluster.coordinator.events.BaseCoordinatorEvent;
import com.kronotop.cluster.coordinator.events.CoordinatorEventType;
import com.kronotop.cluster.coordinator.events.TaskCompletedEvent;
import com.kronotop.cluster.coordinator.tasks.AssignShardTask;
import com.kronotop.cluster.coordinator.tasks.ReassignShardTask;
import com.kronotop.cluster.coordinator.tasks.TaskType;
import com.kronotop.cluster.sharding.ShardMetadata;
import com.kronotop.cluster.sharding.ShardOwner;
import com.kronotop.common.KronotopException;
import com.kronotop.journal.Event;
import com.kronotop.journal.JournalMetadata;
import com.kronotop.journal.JournalName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The CoordinatorService class represents a service that manages coordination and routing in the system.
 */
public class CoordinatorService implements KronotopService {
    public static final String NAME = "Coordinator";
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorService.class);
    private final Context context;
    private final Consistent consistent;
    private final RoutingTable routingTable = new RoutingTable();
    private final KeyWatcher keyWatcher = new KeyWatcher();
    private final AtomicReference<byte[]> latestCoordinatorEventsEventKey = new AtomicReference<>();
    private final HashMap<Integer, DirectorySubspace> shardsSubspaces = new HashMap<>();
    private final ScheduledThreadPoolExecutor scheduler;
    private final CountDownLatch latch = new CountDownLatch(2);
    private final ReentrantLock lock = new ReentrantLock();
    private volatile boolean isShutdown;

    public CoordinatorService(Context context) {
        this.context = context;
        this.consistent = new Consistent(context.getConfig());
        this.scheduler = new ScheduledThreadPoolExecutor(2,
                new ThreadFactoryBuilder().setNameFormat("kr.coordinator-%d").build()
        );
    }

    /**
     * Creates or opens shard subspaces in the FoundationDB database.
     *
     * @param tr the transaction object used to interact with the database
     */
    private void createOrOpenShardSubspaces(Transaction tr) {
        // TODO: This will be removed
        List<String> root = ClusterLayout.getShards(context).asList();
        int numberOfShards = context.getConfig().getInt("cluster.number_of_shards");
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            List<String> shardPath = new ArrayList<>(root);
            shardPath.add(Integer.toString(shardId));
            shardsSubspaces.computeIfAbsent(shardId, (k) -> DirectoryLayer.getDefault().createOrOpen(tr, shardPath).join());
        }
    }

    /**
     * Sets the latest event key for the coordinator events journal.
     *
     * @param tr the ReadTransaction object used to interact with the database.
     */
    private void setLatestEventKey(ReadTransaction tr) {
        byte[] key = context.getJournal().getConsumer().getLatestEventKey(tr, JournalName.coordinatorEvents());
        latestCoordinatorEventsEventKey.set(key);
    }

    /**
     * Rebuilds the routing table by reading shard metadata from FoundationDB.
     *
     * @param tr the read transaction object used to read from the database
     */
    private void rebuildRoutingTable(ReadTransaction tr) {
        int numberOfShards = context.getConfig().getInt("cluster.number_of_shards");
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            try {
                ShardMetadata shardMetadata = loadShardMetadata(tr, shardId);
                if (shardMetadata == null) {
                    continue;
                }
                Member member = new Member(shardMetadata.getOwner().getAddress(), shardMetadata.getOwner().getProcessId());
                Route route = new Route(member);
                routingTable.setRoute(shardId, route);
            } catch (IOException e) {
                LOGGER.error("Failed to load metadata for ShardId: {}", shardId);
            }
        }
        routingTable.updateCoordinator(context.getMember());
        LOGGER.info("Routing table has been rebuilt from FoundationDB");
    }

    /**
     * Starts the Coordinator service.
     * This method performs the following steps:
     * 1. Creates or opens shard subspaces in the FoundationDB database.
     * 2. Sets the latest event key for the coordinator events journal.
     * 3. Rebuilds the routing table by reading shard metadata from FoundationDB.
     * 4. Executes background threads for checking shards periodically and watching the coordinator events journal.
     * 5. Waits for a latch countdown to ensure that the background threads have started.
     * 6. Throws a KronotopException if the latch countdown does not complete within the specified time.
     * 7. Logs a success message after the service has been started.
     */
    public void start() {
        // TODO: remove this
        context.getFoundationDB().run(tr -> {
            createOrOpenShardSubspaces(tr);
            return null;
        });

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            setLatestEventKey(tr);
            rebuildRoutingTable(tr);
        }

        scheduler.execute(new CheckShardsPeriodically());
        scheduler.execute(new CoordinatorEventsJournalWatcher());
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                LOGGER.error("Coordinator service has failed to start background threads");
                throw new KronotopException("Coordinator cannot be started");
            }
        } catch (InterruptedException e) {
            throw new KronotopException(e);
        }
        LOGGER.info("Coordinator service has been started");
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Context getContext() {
        return context;
    }

    /**
     * Shuts down the Coordinator service. This method performs the following steps:
     * 1. Sets the 'isShutdown' flag to true.
     * 2. Cancels the current watcher if it exists.
     * 3. Shuts down the scheduler.
     * 4. Waits for a maximum of 1 second for the scheduler to terminate.
     * 5. Logs a warning message if the scheduler does not terminate gracefully within 1 second.
     * 6. Throws a RuntimeException if the waiting is interrupted.
     */
    @Override
    public void shutdown() {
        isShutdown = true;
        keyWatcher.unwatch(context.getJournal().getJournalMetadata(JournalName.coordinatorEvents()).getTrigger());
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                LOGGER.warn(String.format("%s cannot be stopped gracefully", NAME));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Adds a new member to the consistent hash circle.
     *
     * @param member the member to be added to the consistent hash circle
     */
    public void addMember(Member member) {
        consistent.addMember(member);
    }

    /**
     * Removes a member from the consistent hash circle.
     *
     * @param member the member to be removed
     */
    public void removeMember(Member member) {
        consistent.removeMember(member);
    }

    /**
     * Checks if the shard owner is dead.
     *
     * @param membershipService the membership service
     * @param shardOwner        the shard owner to check
     * @return true if the shard owner is dead, false otherwise
     */
    private boolean isShardOwnerDead(MembershipService membershipService, ShardOwner shardOwner) {
        try {
            membershipService.getMember(shardOwner.getAddress(), shardOwner.getProcessId());
        } catch (NoSuchMemberException e) {
            return true;
        }

        return false;
    }

    /**
     * Checks a task in the given shard metadata and performs appropriate actions based on the task type and deadline.
     *
     * @param taskId        the identifier of the task
     * @param shardId       the identifier of the shard
     * @param shardMetadata the metadata object for the shard
     * @return true if the task is dropped or failed, false otherwise
     */
    private boolean checkTask(String taskId, int shardId, ShardMetadata shardMetadata) {
        MembershipService membershipService = context.getService(MembershipService.NAME);
        ShardMetadata.Task task = shardMetadata.getTasks().get(taskId);

        // Check the deadline and remove it if the deadline is exceeded.
        long elapsedTime = Instant.now().toEpochMilli() - task.getBaseTask().getCreatedAt();
        if (elapsedTime > TimeUnit.SECONDS.toMillis(5)) {
            shardMetadata.getTasks().remove(taskId);
            LOGGER.info("{} has been dropped due to inactivity.", task.getBaseTask());
            return true;
        } else {
            TaskType taskType = task.getBaseTask().getType();
            if (taskType.equals(TaskType.ASSIGN_SHARD)) {
                if (isShardOwnerDead(membershipService, shardMetadata.getOwner())) {
                    shardMetadata.getTasks().remove(taskId);
                    LOGGER.info("{} has failed due to a member failure. Owner: {}", task.getBaseTask(), shardMetadata.getOwner());
                    return true;
                }
            } else if (taskType.equals(TaskType.REASSIGN_SHARD)) {
                if (isShardOwnerDead(membershipService, task.getReassignShardTask().getNextOwner())) {
                    shardMetadata.getTasks().remove(taskId);
                    LOGGER.info("{} has failed due to a member failure. Owner {}", task.getBaseTask(), task.getReassignShardTask().getNextOwner());
                    return true;
                }
            } else {
                LOGGER.error("{} unknown task", task.getBaseTask());
                shardMetadata.getTasks().remove(taskId);
                return true;
            }
        }

        return false;
    }

    /**
     * Checks all tasks in the given shard metadata and performs appropriate actions based on the task type and deadline.
     *
     * @param tr            the transaction object used to interact with the database
     * @param shardId       the identifier of the shard
     * @param shardMetadata the metadata object for the shard
     * @throws JsonProcessingException if an error occurs while serializing the shard metadata to JSON
     */
    private void checkTasks(Transaction tr, int shardId, ShardMetadata shardMetadata) throws JsonProcessingException {
        boolean shardMetadataModified = false;
        for (String taskId : shardMetadata.getTasks().keySet()) {
            if (checkTask(taskId, shardId, shardMetadata)) {
                shardMetadataModified = true;
            }
        }

        if (shardMetadataModified) {
            DirectorySubspace shardSubspace = shardsSubspaces.get(shardId);
            tr.set(shardSubspace.pack(shardId), new ObjectMapper().writeValueAsBytes(shardMetadata));
        }
    }

    /**
     * Reassigns the ownership of a shard from the current owner to the next owner.
     *
     * @param tr            the transaction object used to interact with the database
     * @param shardMetadata the metadata object for the shard
     * @param currentOwner  the current owner of the shard
     * @param nextOwner     the next owner to whom the shard will be reassigned
     * @param shardId       the identifier of the shard
     * @throws JsonProcessingException if an error occurs while serializing the shard metadata to JSON
     */
    private void reassignShardOwnership(Transaction tr, ShardMetadata shardMetadata, Member currentOwner, Member nextOwner, int shardId) throws JsonProcessingException {
        ShardOwner nextShardOwner = new ShardOwner(nextOwner.getAddress(), nextOwner.getProcessId());
        ReassignShardTask reassignShardTask = new ReassignShardTask(nextShardOwner, shardId);

        String journalName = JournalName.shardEvents(currentOwner);
        context.getJournal().getPublisher().publish(tr, journalName, reassignShardTask);

        shardMetadata.getTasks().put(reassignShardTask.getTaskId(), new ShardMetadata.Task(reassignShardTask));
        DirectorySubspace shardSubspace = shardsSubspaces.get(shardId);
        tr.set(shardSubspace.pack(shardId), new ObjectMapper().writeValueAsBytes(shardMetadata));
    }

    /**
     * Assigns the ownership of a shard to a member.
     *
     * @param tr      the transaction object used to interact with the database
     * @param shardId the identifier of the shard
     * @throws JsonProcessingException if an error occurs while serializing the shard metadata to JSON
     */
    private void assignShardOwnership(Transaction tr, int shardId) throws JsonProcessingException {
        Member owner = consistent.getShardOwner(shardId);
        String journalName = JournalName.shardEvents(owner);

        AssignShardTask assignShardTask = new AssignShardTask(shardId);
        context.getJournal().getPublisher().publish(tr, journalName, assignShardTask);

        ShardMetadata shardMetadata = new ShardMetadata(owner.getAddress(), owner.getProcessId());
        shardMetadata.getTasks().put(assignShardTask.getTaskId(), new ShardMetadata.Task(assignShardTask));

        DirectorySubspace shardSubspace = shardsSubspaces.get(shardId);
        tr.set(shardSubspace.pack(shardId), new ObjectMapper().writeValueAsBytes(shardMetadata));
    }

    /**
     * Loads the metadata for a shard from FoundationDB.
     *
     * @param tr      the read transaction object used to read from the database
     * @param shardId the identifier of the shard
     * @return the ShardMetadata object representing the metadata of the shard, or null if no metadata is found
     * @throws IOException if an error occurs while loading the metadata
     */
    private ShardMetadata loadShardMetadata(ReadTransaction tr, int shardId) throws IOException {
        DirectorySubspace shardSubspace = shardsSubspaces.get(shardId);
        byte[] rawShardMetadata = tr.get(shardSubspace.pack(shardId)).join();

        // No metadata found. Start from scratch.
        if (rawShardMetadata == null) {
            return null;
        }

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper.readValue(rawShardMetadata, ShardMetadata.class);
    }

    /**
     * Checks the ownership of a shard and performs appropriate actions based on the current owner and tasks associated with the shard.
     *
     * @param tr      the transaction object used to interact with the database
     * @param shardId the identifier of the shard
     * @throws IOException if an error occurs while loading the shard metadata
     */
    private void checkShardOwnership(Transaction tr, int shardId) throws IOException {
        ShardMetadata shardMetadata = loadShardMetadata(tr, shardId);
        if (shardMetadata == null) {
            assignShardOwnership(tr, shardId);
            return;
        }

        MembershipService membershipService = context.getService(MembershipService.NAME);
        if (isShardOwnerDead(membershipService, shardMetadata.getOwner())) {
            assignShardOwnership(tr, shardId);
            return;
        }

        checkTasks(tr, shardId, shardMetadata);
        if (shardMetadata.getTasks().isEmpty()) {
            Member nextOwner = consistent.getShardOwner(shardId);
            Member currentOwner = new Member(shardMetadata.getOwner().getAddress(), shardMetadata.getOwner().getProcessId());
            if (!currentOwner.equals(nextOwner)) {
                reassignShardOwnership(tr, shardMetadata, currentOwner, nextOwner, shardId);
            }
        }
    }

    public void checkShardOwnerships() {
        lock.lock();
        try {
            context.getFoundationDB().run(tr -> {
                try {
                    int numberOfShards = context.getConfig().getInt("cluster.number_of_shards");
                    for (int shardId = 0; shardId < numberOfShards; shardId++) {
                        checkShardOwnership(tr, shardId);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error while checking shard ownership", e);
                    throw new RuntimeException(e);
                }
                return null;
            });
        } finally {
            lock.unlock();
        }
    }

    /**
     * Propagates the routing table by updating the route for a specific shard with the provided owner information.
     *
     * @param tr      the transaction object used to interact with the database
     * @param shardId the identifier of the shard to update the route for
     * @param owner   the owner of the shard
     * @throws JsonProcessingException if an error occurs while serializing the routing table to JSON
     */
    private void propagateRoutingTable(Transaction tr, int shardId, ShardOwner owner) throws JsonProcessingException {
        Member member = new Member(owner.getAddress(), owner.getProcessId());
        Route route = new Route(member);
        routingTable.setRoute(shardId, route);

        ObjectMapper objectMapper = new ObjectMapper();
        String payload = objectMapper.writeValueAsString(routingTable);
        UpdateRoutingTableEvent updateRoutingTableEvent = new UpdateRoutingTableEvent(payload);
        context.getJournal().getPublisher().publish(tr, JournalName.clusterEvents(), updateRoutingTableEvent);
    }

    /**
     * Completes the assignment of a shard task by updating the shard metadata and propagating the routing table.
     *
     * @param tr                 the transaction object used to interact with the database
     * @param shardMetadata      the metadata object for the shard
     * @param taskCompletedEvent the completed task event
     * @throws JsonProcessingException if an error occurs while serializing the shard metadata to JSON
     */
    private void completeAssignShardTask(Transaction tr, ShardMetadata shardMetadata, TaskCompletedEvent taskCompletedEvent) throws JsonProcessingException {
        shardMetadata.getTasks().remove(taskCompletedEvent.getTaskId());
        propagateRoutingTable(tr, taskCompletedEvent.getShardId(), shardMetadata.getOwner());
    }

    /**
     * Completes the reassignment of a shard task by updating the shard metadata and publishing the assignment to the journal.
     *
     * @param tr                 the transaction object used to interact with the database
     * @param shardMetadata      the metadata object for the shard
     * @param taskCompletedEvent the completed task event
     * @throws JsonProcessingException if an error occurs while serializing the shard metadata to JSON
     */
    private void completeReassignShardTask(Transaction tr, ShardMetadata shardMetadata, TaskCompletedEvent taskCompletedEvent) throws JsonProcessingException {
        ShardMetadata.Task task = shardMetadata.getTasks().get(taskCompletedEvent.getTaskId());
        ReassignShardTask reassignShardTask = task.getReassignShardTask();

        Member owner = new Member(
                reassignShardTask.getNextOwner().getAddress(),
                reassignShardTask.getNextOwner().getProcessId()
        );

        String journalName = JournalName.shardEvents(owner);
        AssignShardTask assignShardTask = new AssignShardTask(reassignShardTask.getShardId());
        context.getJournal().getPublisher().publish(tr, journalName, assignShardTask);

        shardMetadata.setOwner(reassignShardTask.getNextOwner());
        shardMetadata.getTasks().put(assignShardTask.getTaskId(), new ShardMetadata.Task(assignShardTask));
        shardMetadata.getTasks().remove(taskCompletedEvent.getTaskId());

        DirectorySubspace shardSubspace = shardsSubspaces.get(reassignShardTask.getShardId());
        tr.set(shardSubspace.pack(reassignShardTask.getShardId()), new ObjectMapper().writeValueAsBytes(shardMetadata));
    }

    /**
     * Processes a task completed event by updating the shard metadata and propagating the routing table.
     *
     * @param taskCompletedEvent the TaskCompletedEvent object representing the completed task event
     */
    private void processTaskCompletedEvent(TaskCompletedEvent taskCompletedEvent) {
        context.getFoundationDB().run(tr -> {
            try {
                ShardMetadata shardMetadata = loadShardMetadata(tr, taskCompletedEvent.getShardId());
                if (shardMetadata == null) {
                    LOGGER.error("Shard metadata could not be loaded for ShardId: {}", taskCompletedEvent.getShardId());
                    return null;
                }
                ShardMetadata.Task task = shardMetadata.getTasks().get(taskCompletedEvent.getTaskId());
                if (task == null) {
                    LOGGER.warn("TaskId: {} could not be found", taskCompletedEvent.getTaskId());
                    return null;
                }

                if (task.getBaseTask().getType().equals(TaskType.ASSIGN_SHARD)) {
                    completeAssignShardTask(tr, shardMetadata, taskCompletedEvent);
                } else if (task.getBaseTask().getType().equals(TaskType.REASSIGN_SHARD)) {
                    completeReassignShardTask(tr, shardMetadata, taskCompletedEvent);
                } else {
                    LOGGER.error("Unsupported task type: {}", taskCompletedEvent.getType());
                    return null;
                }

                DirectorySubspace shardSubspace = shardsSubspaces.get(taskCompletedEvent.getShardId());
                tr.set(shardSubspace.pack(taskCompletedEvent.getShardId()), new ObjectMapper().writeValueAsBytes(shardMetadata));
            } catch (Exception e) {
                LOGGER.error("Error while checking shard ownership. ShardId: {}", taskCompletedEvent.getShardId(), e);
                throw new RuntimeException(e);
            }
            return null;
        });

        LOGGER.info("{} has been processed", taskCompletedEvent);
    }

    /**
     * Processes a coordinator event by consuming events from the coordinator events journal,
     * deserializing them into coordinator event objects, and performing appropriate actions based on the event type.
     */
    private void processCoordinatorEvent() {
        context.getFoundationDB().run(tr -> {
            while (true) {
                // Try to consume the latest event.
                Event event = context.getJournal().getConsumer().consumeNext(tr, JournalName.coordinatorEvents(), latestCoordinatorEventsEventKey.get());
                if (event == null)
                    return null;

                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                try {
                    BaseCoordinatorEvent baseCoordinatorEvent = objectMapper.readValue(event.getValue(), BaseCoordinatorEvent.class);
                    if (baseCoordinatorEvent.getType() == CoordinatorEventType.TASK_COMPLETED) {
                        TaskCompletedEvent taskCompletedEvent = objectMapper.readValue(event.getValue(), TaskCompletedEvent.class);
                        processTaskCompletedEvent(taskCompletedEvent);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                // Processed the event successfully. Forward the position.
                latestCoordinatorEventsEventKey.set(event.getKey());
            }
        });
    }

    /**
     * Runnable class that watches the coordinator events journal and processes coordinator events.
     */
    private class CoordinatorEventsJournalWatcher implements Runnable {
        @Override
        public void run() {
            if (isShutdown) {
                return;
            }

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                JournalMetadata journalMetadata = context.getJournal().getJournalMetadata(JournalName.coordinatorEvents());
                CompletableFuture<Void> watcher = keyWatcher.watch(tr, journalMetadata.getTrigger());
                tr.commit().join();
                latch.countDown();
                try {
                    // Try to fetch the latest events before start waiting
                    processCoordinatorEvent();
                    watcher.join();
                } catch (CancellationException e) {
                    LOGGER.debug("{} watcher has been cancelled", JournalName.coordinatorEvents());
                    return;
                }

                processCoordinatorEvent();
            } catch (Exception e) {
                LOGGER.error("Error while listening journal: {}", JournalName.coordinatorEvents(), e);
            } finally {
                if (!isShutdown) {
                    scheduler.execute(this);
                }
            }
        }
    }

    /**
     * This private class implements the Runnable interface and is responsible for periodically checking the ownership
     * of shards.
     */
    private class CheckShardsPeriodically implements Runnable {
        @Override
        public void run() {
            if (isShutdown) {
                return;
            }

            latch.countDown();
            try {
                checkShardOwnerships();
            } finally {
                if (!isShutdown) {
                    scheduler.schedule(this, 5, TimeUnit.SECONDS);
                }
            }
        }
    }
}
