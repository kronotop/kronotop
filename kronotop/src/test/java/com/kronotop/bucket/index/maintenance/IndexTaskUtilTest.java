/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.index.maintenance;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for IndexTaskUtil covering task subspace management and task counter operations.
 */
class IndexTaskUtilTest extends BaseBucketHandlerTest {

    @Test
    void testOpenTasksSubspace() {
        // Test opening task subspace for a valid shard
        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);

        assertNotNull(taskSubspace, "Task subspace should not be null");
        assertNotNull(taskSubspace.getKey(), "Task subspace key should not be null");
        assertTrue(taskSubspace.getKey().length > 0, "Task subspace key should not be empty");
    }

    @Test
    void testOpenTasksSubspaceForMultipleShards() {
        // Test opening task subspaces for different shards
        DirectorySubspace subspace0 = IndexTaskUtil.openTasksSubspace(context, 0);
        DirectorySubspace subspace1 = IndexTaskUtil.openTasksSubspace(context, 1);
        DirectorySubspace subspace2 = IndexTaskUtil.openTasksSubspace(context, 2);

        assertNotNull(subspace0);
        assertNotNull(subspace1);
        assertNotNull(subspace2);

        // Each shard should have a different subspace
        assertNotEquals(subspace0.getKey(), subspace1.getKey(),
                "Different shards should have different subspaces");
        assertNotEquals(subspace1.getKey(), subspace2.getKey(),
                "Different shards should have different subspaces");
        assertNotEquals(subspace0.getKey(), subspace2.getKey(),
                "Different shards should have different subspaces");
    }

    @Test
    void testOpenTasksSubspaceConsistency() {
        // Test that opening the same subspace multiple times returns the same result
        DirectorySubspace subspace1 = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        DirectorySubspace subspace2 = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);

        assertNotNull(subspace1);
        assertNotNull(subspace2);
        assertArrayEquals(subspace1.getKey(), subspace2.getKey(),
                "Opening the same shard subspace should return consistent results");
    }

    @Test
    void testModifyTaskCounterIncrement() {
        Versionstamp taskId = Versionstamp.complete(new byte[10], 0);

        // Increment counter by 1
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 1);
            tr.commit().join();
        }

        // Read and verify
        int counter = IndexTaskUtil.readTaskCounter(context, taskId);
        assertEquals(1, counter, "Counter should be 1 after incrementing by 1");
    }

    @Test
    void testModifyTaskCounterMultipleIncrements() {
        Versionstamp taskId = Versionstamp.complete(new byte[10], 1);

        // Increment counter multiple times
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 1);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 1);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 1);
            tr.commit().join();
        }

        // Read and verify
        int counter = IndexTaskUtil.readTaskCounter(context, taskId);
        assertEquals(3, counter, "Counter should be 3 after three increments");
    }

    @Test
    void testModifyTaskCounterDecrement() {
        Versionstamp taskId = Versionstamp.complete(new byte[10], 2);

        // Initialize counter to 10
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 10);
            tr.commit().join();
        }

        // Decrement by 3
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, -3);
            tr.commit().join();
        }

        // Read and verify
        int counter = IndexTaskUtil.readTaskCounter(context, taskId);
        assertEquals(7, counter, "Counter should be 7 after initializing to 10 and decrementing by 3");
    }

    @Test
    void testModifyTaskCounterLargeDelta() {
        Versionstamp taskId = Versionstamp.complete(new byte[10], 3);

        // Use a large delta value
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 1000);
            tr.commit().join();
        }

        // Read and verify
        int counter = IndexTaskUtil.readTaskCounter(context, taskId);
        assertEquals(1000, counter, "Counter should handle large delta values");
    }

    @Test
    void testModifyTaskCounterNegativeResult() {
        Versionstamp taskId = Versionstamp.complete(new byte[10], 4);

        // Initialize to 5
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 5);
            tr.commit().join();
        }

        // Decrement by 10 to get negative result
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, -10);
            tr.commit().join();
        }

        // Read and verify
        int counter = IndexTaskUtil.readTaskCounter(context, taskId);
        assertEquals(-5, counter, "Counter should support negative values");
    }

    @Test
    void testModifyTaskCounterInSingleTransaction() {
        Versionstamp taskId = Versionstamp.complete(new byte[10], 5);

        // Multiple modifications in a single transaction
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 10);
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 5);
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, -3);
            tr.commit().join();
        }

        // Read and verify
        int counter = IndexTaskUtil.readTaskCounter(context, taskId);
        assertEquals(12, counter, "Counter should be 12 after multiple modifications (10 + 5 - 3)");
    }

    @Test
    void testModifyTaskCounterConcurrentModifications() throws InterruptedException {
        Versionstamp taskId = Versionstamp.complete(new byte[10], 6);
        int numberOfThreads = 10;
        int incrementsPerThread = 5;

        // Simulate concurrent modifications from multiple shards
        Thread[] threads = new Thread[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    try (Transaction tr = context.getFoundationDB().createTransaction()) {
                        IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 1);
                        tr.commit().join();
                    }
                }
            });
            threads[i].start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Read and verify
        int counter = IndexTaskUtil.readTaskCounter(context, taskId);
        assertEquals(numberOfThreads * incrementsPerThread, counter,
                "Counter should correctly handle concurrent modifications");
    }

    @Test
    void testReadTaskCounterForNonExistentTask() {
        Versionstamp taskId = Versionstamp.complete(new byte[10], 999);

        // Reading a counter that doesn't exist should return 0 (default value for integers in FDB)
        int counter = IndexTaskUtil.readTaskCounter(context, taskId);
        assertEquals(0, counter, "Counter for non-existent task should be 0");
    }

    @Test
    void testTaskCounterWithDifferentTaskIds() {
        Versionstamp taskId1 = Versionstamp.complete(new byte[10], 100);
        Versionstamp taskId2 = Versionstamp.complete(new byte[10], 200);

        // Set different counters for different tasks
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId1, 42);
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId2, 99);
            tr.commit().join();
        }

        // Verify each counter independently
        int counter1 = IndexTaskUtil.readTaskCounter(context, taskId1);
        int counter2 = IndexTaskUtil.readTaskCounter(context, taskId2);

        assertEquals(42, counter1, "First task counter should be 42");
        assertEquals(99, counter2, "Second task counter should be 99");
    }

    @Test
    void testTaskCounterSimulatingShardCompletion() {
        Versionstamp taskId = Versionstamp.complete(new byte[10], 7);
        int totalShards = 4;

        // Simulate each shard completing its portion of the task
        for (int shardId = 0; shardId < totalShards; shardId++) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 1);
                tr.commit().join();
            }
        }

        // Verify all shards completed
        int counter = IndexTaskUtil.readTaskCounter(context, taskId);
        assertEquals(totalShards, counter,
                "Counter should equal total shards after all shards complete");
    }

    @Test
    void testTaskCounterZeroDelta() {
        Versionstamp taskId = Versionstamp.complete(new byte[10], 8);

        // Initialize counter
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 10);
            tr.commit().join();
        }

        // Add zero delta
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 0);
            tr.commit().join();
        }

        // Verify counter unchanged
        int counter = IndexTaskUtil.readTaskCounter(context, taskId);
        assertEquals(10, counter, "Counter should remain unchanged after adding zero");
    }

    @Test
    void testModifyAndReadInSameTransaction() {
        Versionstamp taskId = Versionstamp.complete(new byte[10], 9);

        // Modify and read in the same transaction
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 50);
            tr.commit().join();
        }

        // Read in a new transaction
        int counter = IndexTaskUtil.readTaskCounter(context, taskId);
        assertEquals(50, counter, "Counter should be readable after commit");
    }

    @Test
    void testTaskCounterOverflow() {
        Versionstamp taskId = Versionstamp.complete(new byte[10], 10);

        // Set counter to near max int value
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, Integer.MAX_VALUE - 10);
            tr.commit().join();
        }

        // Increment beyond max int (should overflow to negative)
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexTaskUtil.modifyTaskCounter(context, tr, taskId, 20);
            tr.commit().join();
        }

        // Read and verify overflow behavior
        int counter = IndexTaskUtil.readTaskCounter(context, taskId);
        assertTrue(counter < 0, "Counter should overflow to negative value");
    }
}
