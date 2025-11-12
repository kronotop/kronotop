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

package com.kronotop.internal.task;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.bucket.index.maintenance.IndexBuildingTask;
import com.kronotop.internal.JSONUtil;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class TaskStorageTest extends BaseStandaloneInstanceTest {
    final String TEST_SUBSPACE_NAME = "test-task-subspace";

    @Test
    void test_create_with_context() {
        IndexBuildingTask task = createIndexBuildingTask(1);
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        Versionstamp taskId = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task));

        assertNotNull(taskId);
        assertTrue(taskId.isComplete());
    }

    @Test
    void test_create_with_transaction() {
        IndexBuildingTask task = createIndexBuildingTask(1);
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        Versionstamp taskId;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            CompletableFuture<byte[]> future = TaskStorage.create(tr, 0, subspace, JSONUtil.writeValueAsBytes(task));
            tr.commit().join();

            byte[] versionstampBytes = future.join();
            assertNotNull(versionstampBytes);
            assertEquals(10, versionstampBytes.length);

            taskId = Versionstamp.complete(versionstampBytes);
            assertTrue(taskId.isComplete());
        }

        // Verify the task was created
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            assertNotNull(definition);

            IndexBuildingTask retrievedTask = JSONUtil.readValue(definition, IndexBuildingTask.class);
            assertEquals(task.getNamespace(), retrievedTask.getNamespace());
            assertEquals(task.getBucket(), retrievedTask.getBucket());
            assertEquals(task.getIndexId(), retrievedTask.getIndexId());
        }
    }

    @Test
    void test_create_with_userVersion() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        // Create tasks with different user versions in the same transaction
        // Note: Both tasks will have the same transaction version but different user versions
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTask task1 = createIndexBuildingTask(1);
            IndexBuildingTask task2 = createIndexBuildingTask(2);

            CompletableFuture<byte[]> future1 = TaskStorage.create(tr, 1, subspace, JSONUtil.writeValueAsBytes(task1));
            CompletableFuture<byte[]> future2 = TaskStorage.create(tr, 2, subspace, JSONUtil.writeValueAsBytes(task2));

            tr.commit().join();

            // Both futures return the same transaction version (10 bytes)
            byte[] trVersion1 = future1.join();
            byte[] trVersion2 = future2.join();

            assertNotNull(trVersion1);
            assertNotNull(trVersion2);
            assertEquals(10, trVersion1.length);
            assertEquals(10, trVersion2.length);

            // The transaction versions are identical since they're in the same transaction
            assertArrayEquals(trVersion1, trVersion2);
        }
    }

    @Test
    void test_getDefinition() {
        IndexBuildingTask task = createIndexBuildingTask(123);
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        Versionstamp taskId = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            assertNotNull(definition);

            IndexBuildingTask retrievedTask = JSONUtil.readValue(definition, IndexBuildingTask.class);
            assertEquals(TEST_NAMESPACE, retrievedTask.getNamespace());
            assertEquals(TEST_BUCKET, retrievedTask.getBucket());
            assertEquals(123, retrievedTask.getIndexId());
        }
    }

    @Test
    void test_getDefinition_nonExistent() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        // Create a fake versionstamp
        Versionstamp fakeTaskId = Versionstamp.complete(new byte[10]);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, subspace, fakeTaskId);
            assertNull(definition);
        }
    }

    @Test
    void test_drop() {
        IndexBuildingTask task = createIndexBuildingTask(1);
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        Versionstamp taskId = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task));

        // Add some state fields
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.setStateField(tr, subspace, taskId, "status", "running".getBytes(StandardCharsets.UTF_8));
            TaskStorage.setStateField(tr, subspace, taskId, "progress", "50%".getBytes(StandardCharsets.UTF_8));
            tr.commit().join();
        }

        // Verify task and state exist
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            assertNotNull(definition);

            Map<String, byte[]> stateFields = TaskStorage.getStateFields(tr, subspace, taskId);
            assertEquals(2, stateFields.size());
        }

        // Drop the task
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.drop(tr, subspace, taskId);
            tr.commit().join();
        }

        // Verify task and all state are deleted
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] definition = TaskStorage.getDefinition(tr, subspace, taskId);
            assertNull(definition);

            Map<String, byte[]> stateFields = TaskStorage.getStateFields(tr, subspace, taskId);
            assertTrue(stateFields.isEmpty());
        }
    }

    @Test
    void test_setStateField() {
        IndexBuildingTask task = createIndexBuildingTask(1);
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        Versionstamp taskId = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task));

        String fieldName = "status";
        String fieldValue = "processing";

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.setStateField(tr, subspace, taskId, fieldName, fieldValue.getBytes(StandardCharsets.UTF_8));
            tr.commit().join();
        }

        // Verify the state field was set
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] value = TaskStorage.getStateField(tr, subspace, taskId, fieldName);
            assertNotNull(value);
            assertEquals(fieldValue, new String(value, StandardCharsets.UTF_8));
        }
    }

    @Test
    void test_setStateField_update() {
        IndexBuildingTask task = createIndexBuildingTask(1);
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        Versionstamp taskId = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task));

        String fieldName = "status";

        // Set initial value
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.setStateField(tr, subspace, taskId, fieldName, "running".getBytes(StandardCharsets.UTF_8));
            tr.commit().join();
        }

        // Update the value
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.setStateField(tr, subspace, taskId, fieldName, "completed".getBytes(StandardCharsets.UTF_8));
            tr.commit().join();
        }

        // Verify the updated value
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] value = TaskStorage.getStateField(tr, subspace, taskId, fieldName);
            assertEquals("completed", new String(value, StandardCharsets.UTF_8));
        }
    }

    @Test
    void test_getStateField() {
        IndexBuildingTask task = createIndexBuildingTask(1);
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        Versionstamp taskId = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task));

        String fieldName = "progress";
        String fieldValue = "75%";

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.setStateField(tr, subspace, taskId, fieldName, fieldValue.getBytes(StandardCharsets.UTF_8));
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] value = TaskStorage.getStateField(tr, subspace, taskId, fieldName);
            assertNotNull(value);
            assertEquals(fieldValue, new String(value, StandardCharsets.UTF_8));
        }
    }

    @Test
    void test_getStateField_nonExistent() {
        IndexBuildingTask task = createIndexBuildingTask(1);
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        Versionstamp taskId = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] value = TaskStorage.getStateField(tr, subspace, taskId, "nonexistent");
            assertNull(value);
        }
    }

    @Test
    void test_getStateFields() {
        IndexBuildingTask task = createIndexBuildingTask(1);
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        Versionstamp taskId = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task));

        // Set multiple state fields
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.setStateField(tr, subspace, taskId, "status", "running".getBytes(StandardCharsets.UTF_8));
            TaskStorage.setStateField(tr, subspace, taskId, "progress", "50%".getBytes(StandardCharsets.UTF_8));
            TaskStorage.setStateField(tr, subspace, taskId, "error", "none".getBytes(StandardCharsets.UTF_8));
            tr.commit().join();
        }

        // Retrieve all state fields
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<String, byte[]> stateFields = TaskStorage.getStateFields(tr, subspace, taskId);

            assertEquals(3, stateFields.size());
            assertTrue(stateFields.containsKey("status"));
            assertTrue(stateFields.containsKey("progress"));
            assertTrue(stateFields.containsKey("error"));

            assertEquals("running", new String(stateFields.get("status"), StandardCharsets.UTF_8));
            assertEquals("50%", new String(stateFields.get("progress"), StandardCharsets.UTF_8));
            assertEquals("none", new String(stateFields.get("error"), StandardCharsets.UTF_8));
        }
    }

    @Test
    void test_getStateFields_empty() {
        IndexBuildingTask task = createIndexBuildingTask(1);
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        Versionstamp taskId = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<String, byte[]> stateFields = TaskStorage.getStateFields(tr, subspace, taskId);
            assertNotNull(stateFields);
            assertTrue(stateFields.isEmpty());
        }
    }

    @Test
    void test_trigger() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        byte[] triggerKey = TaskStorage.trigger(subspace);
        assertNotNull(triggerKey);
        assertTrue(triggerKey.length > 0);
    }

    @Test
    void test_trigger_increments_on_create() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);
        byte[] triggerKey = TaskStorage.trigger(subspace);

        // Get initial trigger value
        long initialValue;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] value = tr.get(triggerKey).join();
            initialValue = value != null ? bytesToLong(value) : 0;
        }

        // Create a task
        IndexBuildingTask task = createIndexBuildingTask(1);
        TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task));

        // Verify trigger was incremented
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] value = tr.get(triggerKey).join();
            assertNotNull(value);
            long newValue = bytesToLong(value);
            assertEquals(initialValue + 1, newValue);
        }
    }

    @Test
    void test_multiple_tasks_in_same_subspace() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        // Create multiple tasks
        IndexBuildingTask task1 = createIndexBuildingTask(1);
        IndexBuildingTask task2 = createIndexBuildingTask(2);
        IndexBuildingTask task3 = createIndexBuildingTask(3);

        Versionstamp taskId1 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task1));
        Versionstamp taskId2 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task2));
        Versionstamp taskId3 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task3));

        // Verify all tasks are unique
        assertNotEquals(taskId1, taskId2);
        assertNotEquals(taskId2, taskId3);
        assertNotEquals(taskId1, taskId3);

        // Verify all tasks can be retrieved
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] def1 = TaskStorage.getDefinition(tr, subspace, taskId1);
            byte[] def2 = TaskStorage.getDefinition(tr, subspace, taskId2);
            byte[] def3 = TaskStorage.getDefinition(tr, subspace, taskId3);

            assertNotNull(def1);
            assertNotNull(def2);
            assertNotNull(def3);

            IndexBuildingTask retrieved1 = JSONUtil.readValue(def1, IndexBuildingTask.class);
            IndexBuildingTask retrieved2 = JSONUtil.readValue(def2, IndexBuildingTask.class);
            IndexBuildingTask retrieved3 = JSONUtil.readValue(def3, IndexBuildingTask.class);

            assertEquals(1, retrieved1.getIndexId());
            assertEquals(2, retrieved2.getIndexId());
            assertEquals(3, retrieved3.getIndexId());
        }
    }

    @Test
    void test_task_state_isolation() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        // Create two tasks
        IndexBuildingTask task1 = createIndexBuildingTask(1);
        IndexBuildingTask task2 = createIndexBuildingTask(2);

        Versionstamp taskId1 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task1));
        Versionstamp taskId2 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task2));

        // Set different state for each task
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.setStateField(tr, subspace, taskId1, "status", "running".getBytes(StandardCharsets.UTF_8));
            TaskStorage.setStateField(tr, subspace, taskId2, "status", "completed".getBytes(StandardCharsets.UTF_8));
            tr.commit().join();
        }

        // Verify state isolation
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] status1 = TaskStorage.getStateField(tr, subspace, taskId1, "status");
            byte[] status2 = TaskStorage.getStateField(tr, subspace, taskId2, "status");

            assertEquals("running", new String(status1, StandardCharsets.UTF_8));
            assertEquals("completed", new String(status2, StandardCharsets.UTF_8));
        }
    }

    @Test
    void test_tasks_iterate_all() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        // Create multiple tasks
        IndexBuildingTask task1 = createIndexBuildingTask(1);
        IndexBuildingTask task2 = createIndexBuildingTask(2);
        IndexBuildingTask task3 = createIndexBuildingTask(3);

        Versionstamp taskId1 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task1));
        Versionstamp taskId2 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task2));
        Versionstamp taskId3 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task3));

        // Iterate over all tasks
        java.util.List<Versionstamp> foundTasks = new java.util.ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.tasks(tr, subspace, taskId -> {
                foundTasks.add(taskId);
                return true; // Continue iteration
            });
        }

        // Verify all tasks were found
        assertEquals(3, foundTasks.size());
        assertTrue(foundTasks.contains(taskId1));
        assertTrue(foundTasks.contains(taskId2));
        assertTrue(foundTasks.contains(taskId3));
    }

    @Test
    void test_tasks_chronological_order() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        // Create tasks sequentially
        IndexBuildingTask task1 = createIndexBuildingTask(1);
        IndexBuildingTask task2 = createIndexBuildingTask(2);
        IndexBuildingTask task3 = createIndexBuildingTask(3);

        Versionstamp taskId1 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task1));
        Versionstamp taskId2 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task2));
        Versionstamp taskId3 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task3));

        // Collect tasks in iteration order
        java.util.List<Versionstamp> foundTasks = new java.util.ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.tasks(tr, subspace, taskId -> {
                foundTasks.add(taskId);
                return true;
            });
        }

        // Verify tasks are in chronological order
        assertEquals(3, foundTasks.size());
        assertEquals(taskId1, foundTasks.get(0));
        assertEquals(taskId2, foundTasks.get(1));
        assertEquals(taskId3, foundTasks.get(2));
    }

    @Test
    void test_tasks_break_after_first() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        // Create multiple tasks
        IndexBuildingTask task1 = createIndexBuildingTask(1);
        IndexBuildingTask task2 = createIndexBuildingTask(2);
        IndexBuildingTask task3 = createIndexBuildingTask(3);

        Versionstamp taskId1 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task1));
        TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task2));
        TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task3));

        // Iterate but break after first task
        java.util.List<Versionstamp> foundTasks = new java.util.ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.tasks(tr, subspace, taskId -> {
                foundTasks.add(taskId);
                return false; // Break immediately
            });
        }

        // Verify only first task was processed
        assertEquals(1, foundTasks.size());
        assertEquals(taskId1, foundTasks.get(0));
    }

    @Test
    void test_tasks_break_after_second() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        // Create multiple tasks
        IndexBuildingTask task1 = createIndexBuildingTask(1);
        IndexBuildingTask task2 = createIndexBuildingTask(2);
        IndexBuildingTask task3 = createIndexBuildingTask(3);

        Versionstamp taskId1 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task1));
        Versionstamp taskId2 = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task2));
        TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task3));

        // Iterate but break after second task
        java.util.List<Versionstamp> foundTasks = new java.util.ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.tasks(tr, subspace, taskId -> {
                foundTasks.add(taskId);
                return foundTasks.size() < 2; // Break after second task
            });
        }

        // Verify only first two tasks were processed
        assertEquals(2, foundTasks.size());
        assertEquals(taskId1, foundTasks.get(0));
        assertEquals(taskId2, foundTasks.get(1));
    }

    @Test
    void test_tasks_find_specific_task() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        // Create multiple tasks
        IndexBuildingTask task1 = createIndexBuildingTask(1);
        IndexBuildingTask task2 = createIndexBuildingTask(2);
        IndexBuildingTask task3 = createIndexBuildingTask(3);

        TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task1));
        Versionstamp targetTaskId = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task2));
        TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task3));

        // Find specific task and break
        java.util.concurrent.atomic.AtomicReference<Versionstamp> foundTask = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicInteger iterationCount = new java.util.concurrent.atomic.AtomicInteger(0);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.tasks(tr, subspace, taskId -> {
                iterationCount.incrementAndGet();
                if (taskId.equals(targetTaskId)) {
                    foundTask.set(taskId);
                    return false; // Found it, stop iteration
                }
                return true; // Continue searching
            });
        }

        // Verify we found the task and stopped early
        assertNotNull(foundTask.get());
        assertEquals(targetTaskId, foundTask.get());
        assertEquals(2, iterationCount.get()); // Should have processed 2 tasks before breaking
    }

    @Test
    void test_tasks_empty_subspace() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        // Don't create any tasks
        java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger(0);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.tasks(tr, subspace, taskId -> {
                callCount.incrementAndGet();
                return true;
            });
        }

        // Verify action was never called
        assertEquals(0, callCount.get());
    }

    @Test
    void test_tasks_ignores_state_entries() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        // Create a task with state fields
        IndexBuildingTask task = createIndexBuildingTask(1);
        Versionstamp taskId = TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task));

        // Add multiple state fields
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.setStateField(tr, subspace, taskId, "status", "running".getBytes(StandardCharsets.UTF_8));
            TaskStorage.setStateField(tr, subspace, taskId, "progress", "50%".getBytes(StandardCharsets.UTF_8));
            TaskStorage.setStateField(tr, subspace, taskId, "error", "none".getBytes(StandardCharsets.UTF_8));
            tr.commit().join();
        }

        // Iterate over tasks
        java.util.List<Versionstamp> foundTasks = new java.util.ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.tasks(tr, subspace, foundTaskId -> {
                foundTasks.add(foundTaskId);
                return true;
            });
        }

        // Verify only one task is found (not duplicated for each state field)
        assertEquals(1, foundTasks.size());
        assertEquals(taskId, foundTasks.get(0));
    }

    @Test
    void test_tasks_conditional_processing() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster(TEST_SUBSPACE_NAME);

        // Create multiple tasks
        IndexBuildingTask task1 = createIndexBuildingTask(1);
        IndexBuildingTask task2 = createIndexBuildingTask(2);
        IndexBuildingTask task3 = createIndexBuildingTask(3);
        IndexBuildingTask task4 = createIndexBuildingTask(4);
        IndexBuildingTask task5 = createIndexBuildingTask(5);

        TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task1));
        TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task2));
        TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task3));
        TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task4));
        TaskStorage.create(context, subspace, JSONUtil.writeValueAsBytes(task5));

        // Process only first 3 tasks
        java.util.List<Versionstamp> processedTasks = new java.util.ArrayList<>();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TaskStorage.tasks(tr, subspace, taskId -> {
                processedTasks.add(taskId);
                return processedTasks.size() < 3; // Continue until we have 3 tasks
            });
        }

        // Verify exactly 3 tasks were processed
        assertEquals(3, processedTasks.size());
    }

    /**
     * Helper method to convert little-endian byte array to long
     */
    private long bytesToLong(byte[] bytes) {
        long result = 0;
        for (int i = 0; i < 8 && i < bytes.length; i++) {
            result |= ((long) (bytes[i] & 0xFF)) << (8 * i);
        }
        return result;
    }
}