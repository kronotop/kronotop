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

package com.kronotop.core.cluster.coordinator;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.Context;
import com.kronotop.core.KronotopService;
import com.kronotop.core.cluster.Member;
import com.kronotop.core.cluster.MembershipService;
import com.kronotop.core.cluster.NoSuchMemberException;
import com.kronotop.core.cluster.consistent.Consistent;
import com.kronotop.core.cluster.coordinator.events.BaseCoordinatorEvent;
import com.kronotop.core.cluster.coordinator.events.CoordinatorEventType;
import com.kronotop.core.cluster.coordinator.events.TaskCompletedEvent;
import com.kronotop.core.cluster.coordinator.tasks.AssignShardTask;
import com.kronotop.core.cluster.coordinator.tasks.ReassignShardTask;
import com.kronotop.core.cluster.coordinator.tasks.TaskType;
import com.kronotop.core.cluster.journal.Event;
import com.kronotop.core.cluster.journal.Journal;
import com.kronotop.core.cluster.journal.JournalMetadata;
import com.kronotop.core.cluster.journal.JournalName;
import com.kronotop.core.cluster.sharding.ShardMetadata;
import com.kronotop.core.cluster.sharding.ShardOwner;
import com.kronotop.core.cluster.sharding.ShardStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class CoordinatorService implements KronotopService {
    public static final String NAME = "Coordinator";
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorService.class);
    private final Context context;
    private final Journal journal;
    private final Consistent consistent;
    private final RoutingTable routingTable = new RoutingTable();
    private final AtomicReference<CompletableFuture<Void>> currentWatcher = new AtomicReference<>();
    private final AtomicLong lastOffset;
    private final HashMap<Integer, DirectorySubspace> shardsSubspaces = new HashMap<>();
    private final ScheduledThreadPoolExecutor scheduler;
    private final CountDownLatch latch = new CountDownLatch(2);
    private final ReentrantLock lock = new ReentrantLock();
    private volatile boolean isShutdown;

    public CoordinatorService(Context context) {
        this.context = context;
        this.journal = new Journal(context);
        this.lastOffset = new AtomicLong(this.journal.getConsumer().getLatestIndex(JournalName.coordinatorEvents()));
        this.consistent = new Consistent(context.getConfig());
        this.scheduler = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryBuilder().setNameFormat("kr.coordinator-service-%d").build()
        );
        this.routingTable.updateCoordinator(context.getMember());
    }

    public void addMember(Member member) {
        consistent.addMember(member);
    }

    public void removeMember(Member member) {
        consistent.removeMember(member);
    }

    private String getTaskId(String journal, long offset) {
        return String.format("%s:%d", journal, offset);
    }

    private void createOrOpenShardSubspaces() {
        List<String> root = DirectoryLayout.Builder.clusterName(context.getClusterName()).internal().shards().asList();
        int numberOfShards = context.getConfig().getInt("cluster.number_of_shards");
        context.getFoundationDB().run(tr -> {
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                List<String> shardPath = new ArrayList<>(root);
                shardPath.add(Integer.toString(shardId));
                shardsSubspaces.computeIfAbsent(shardId, (k) -> DirectoryLayer.getDefault().createOrOpen(tr, shardPath).join());
            }
            return null;
        });
    }

    private boolean isShardOwnerDead(MembershipService membershipService, ShardOwner shardOwner) {
        try {
            membershipService.getMember(shardOwner.getAddress(), shardOwner.getProcessId());
        } catch (NoSuchMemberException e) {
            return true;
        }

        return false;
    }

    private boolean checkTask(String taskId, int shardId, ShardMetadata shardMetadata) {
        MembershipService membershipService = context.getService(MembershipService.NAME);
        ShardMetadata.Task task = shardMetadata.getTasks().get(taskId);

        // Check the deadline and remove it if the deadline is exceeded.
        long elapsedTime = Instant.now().toEpochMilli() - task.getBaseTask().getCreatedAt();
        if (elapsedTime > TimeUnit.SECONDS.toMillis(5)) {
            shardMetadata.getTasks().remove(taskId);
            LOGGER.info("Task has been dropped due to inactivity. ShardId: {}, TaskType: {}", shardId, task.getBaseTask().getType());
            return true;
        } else {
            TaskType taskType = task.getBaseTask().getType();
            if (taskType.equals(TaskType.ASSIGN_SHARD)) {
                if (isShardOwnerDead(membershipService, shardMetadata.getOwner())) {
                    shardMetadata.getTasks().remove(taskId);
                    LOGGER.info("Shard initialization has failed due to a member failure. ShardId: {}, Owner: {}", shardId, shardMetadata.getOwner());
                    return true;
                }
            } else if (taskType.equals(TaskType.REASSIGN_SHARD)) {
                if (isShardOwnerDead(membershipService, task.getReassignShardTask().getNextOwner())) {
                    shardMetadata.getTasks().remove(taskId);
                    LOGGER.info("Shard reassignment has failed due to a member failure. ShardId: {}, Owner {}", shardId, task.getReassignShardTask().getNextOwner());
                    return true;
                }
            } else {
                LOGGER.error("Unknown task. ShardId: {}, TaskType: {}, TaskId: {}", shardId, task.getBaseTask().getType(), taskId);
                shardMetadata.getTasks().remove(taskId);
                return true;
            }
        }

        return false;
    }

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

    private void reassignShardOwnership(Transaction tr, ShardMetadata shardMetadata, Member currentOwner, Member nextOwner, int shardId) throws JsonProcessingException {
        ShardOwner nextShardOwner = new ShardOwner(nextOwner.getAddress(), nextOwner.getProcessId());
        ReassignShardTask reassignShardTask = new ReassignShardTask(nextShardOwner, shardId);

        String journalName = JournalName.shardEvents(currentOwner);
        long offset = journal.getPublisher().publish(tr, journalName, reassignShardTask);
        String taskId = getTaskId(journalName, offset);

        shardMetadata.getTasks().put(taskId, new ShardMetadata.Task(reassignShardTask));
        shardMetadata.setStatus(ShardStatus.REASSIGNING);

        DirectorySubspace shardSubspace = shardsSubspaces.get(shardId);
        tr.set(shardSubspace.pack(shardId), new ObjectMapper().writeValueAsBytes(shardMetadata));
    }

    private void assignShardOwnership(Transaction tr, int shardId) throws JsonProcessingException {
        Member owner = consistent.getShardOwner(shardId);
        String journalName = JournalName.shardEvents(owner);

        AssignShardTask assignShardTask = new AssignShardTask(shardId);
        long offset = journal.getPublisher().publish(tr, journalName, assignShardTask);
        String taskId = getTaskId(journalName, offset);

        ShardMetadata shardMetadata = new ShardMetadata(owner.getAddress(), owner.getProcessId());
        shardMetadata.setStatus(ShardStatus.ASSIGNING);
        shardMetadata.getTasks().put(taskId, new ShardMetadata.Task(assignShardTask));

        DirectorySubspace shardSubspace = shardsSubspaces.get(shardId);
        tr.set(shardSubspace.pack(shardId), new ObjectMapper().writeValueAsBytes(shardMetadata));
    }

    private ShardMetadata loadShardMetadata(Transaction tr, int shardId) throws IOException {
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

    private void checkShardOwnership(Transaction tr, int shardId) throws IOException {
        ShardMetadata shardMetadata = loadShardMetadata(tr, shardId);
        if (shardMetadata == null) {
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

    public void start() {
        createOrOpenShardSubspaces();
        scheduler.execute(new CheckShardsPeriodically());
        scheduler.execute(new CoordinatorEventsJournalWatcher());
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                LOGGER.error("Coordinator service has failed to start background threads");
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

    @Override
    public void shutdown() {
        isShutdown = true;
        currentWatcher.get().cancel(true);
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(6, TimeUnit.SECONDS)) {
                LOGGER.warn(String.format("%s cannot be stopped gracefully", NAME));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void propagateRoutingTable(Transaction tr, int shardId, ShardOwner owner) throws JsonProcessingException {
        Member member = new Member(owner.getAddress(), owner.getProcessId());
        Route route = new Route(member);
        routingTable.setRoute(shardId, route);

        ObjectMapper objectMapper = new ObjectMapper();
        String payload = objectMapper.writeValueAsString(routingTable);
        UpdateRoutingTableEvent updateRoutingTableEvent = new UpdateRoutingTableEvent(payload);
        journal.getPublisher().publish(tr, JournalName.clusterEvents(), updateRoutingTableEvent);
    }

    private void completeAssignShardTask(Transaction tr, ShardMetadata shardMetadata, TaskCompletedEvent taskCompletedEvent) throws JsonProcessingException {
        shardMetadata.getTasks().remove(taskCompletedEvent.getTaskId());
        shardMetadata.setStatus(ShardStatus.OPERABLE);
        propagateRoutingTable(tr, taskCompletedEvent.getShardId(), shardMetadata.getOwner());
    }

    private void processTaskCompletedEvent(TaskCompletedEvent taskCompletedEvent) {
        context.getFoundationDB().run(tr -> {
            try {
                ShardMetadata shardMetadata = loadShardMetadata(tr, taskCompletedEvent.getShardId());
                if (shardMetadata == null) {
                    LOGGER.error("ShardMetadata could not be loaded for ShardId: {}", taskCompletedEvent.getShardId());
                    return null;
                }
                ShardMetadata.Task task = shardMetadata.getTasks().get(taskCompletedEvent.getTaskId());
                if (task.getBaseTask().getType().equals(TaskType.ASSIGN_SHARD)) {
                    completeAssignShardTask(tr, shardMetadata, taskCompletedEvent);
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

        LOGGER.info("ShardId: {}, TaskType: {}, TaskId: {} has been completed",
                taskCompletedEvent.getShardId(),
                taskCompletedEvent.getType(),
                taskCompletedEvent.getTaskId()
        );
    }

    private void processCoordinatorEvent() {
        context.getFoundationDB().run(tr -> {
            while (true) {
                // Try to consume the latest event.
                Event event = journal.getConsumer().consumeEvent(tr, JournalName.coordinatorEvents(), lastOffset.get() + 1);
                if (event == null)
                    // There is nothing to process
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

                // Processed the event successfully. Forward the offset.
                lastOffset.set(event.getOffset());
            }
        });
    }

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

    private class CoordinatorEventsJournalWatcher implements Runnable {
        @Override
        public void run() {
            if (isShutdown) {
                return;
            }

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                JournalMetadata journalMetadata = journal.getConsumer().getJournalMetadata(JournalName.coordinatorEvents());
                CompletableFuture<Void> watcher = tr.watch(journalMetadata.getJournalKey());
                tr.commit().join();
                currentWatcher.set(watcher);
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
}
