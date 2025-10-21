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
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for IndexBuildingTaskState covering state persistence, loading, and terminal status checks.
 */
class IndexBuildingTaskStateTest extends BaseBucketHandlerTest {
    private DirectorySubspace taskSubspace;
    private Versionstamp taskId;

    @BeforeEach
    void setupTask() {
        taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexBuildingTask task = new IndexBuildingTask(TEST_NAMESPACE, TEST_BUCKET, 12345);
        taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));
    }

    @Test
    void testLoadWithDefaultValues() {
        // Load state without setting any fields - should return defaults
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);

            assertNotNull(state);
            assertNull(state.cursorVersionstamp(), "Default cursor should be null");
            assertNull(state.highestVersionstamp(), "Default highest versionstamp should be null");
            assertEquals(IndexTaskStatus.WAITING, state.status(), "Default status should be WAITING");
            assertNull(state.error(), "Default error should be null");
        }
    }

    @Test
    void testSetAndLoadCursorVersionstamp() {
        Versionstamp cursorValue = Versionstamp.complete(new byte[10], 0);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setCursorVersionstamp(tr, taskSubspace, taskId, cursorValue);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);

            assertNotNull(state.cursorVersionstamp());
            assertEquals(cursorValue, state.cursorVersionstamp());
        }
    }

    @Test
    void testSetAndLoadHighestVersionstamp() {
        Versionstamp highestValue = Versionstamp.complete(new byte[10], 100);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setHighestVersionstamp(tr, taskSubspace, taskId, highestValue);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);

            assertNotNull(state.highestVersionstamp());
            assertEquals(highestValue, state.highestVersionstamp());
        }
    }

    @Test
    void testSetAndLoadError() {
        String errorMessage = "Test error: index build failed due to invalid document";

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setError(tr, taskSubspace, taskId, errorMessage);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);

            assertNotNull(state.error());
            assertEquals(errorMessage, state.error());
        }
    }

    @Test
    void testSetAndLoadStatus() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.RUNNING);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);

            assertEquals(IndexTaskStatus.RUNNING, state.status());
        }
    }

    @Test
    void testCompleteTaskLifecycle() {
        Versionstamp cursorValue = Versionstamp.complete(new byte[10], 0);
        Versionstamp highestValue = Versionstamp.complete(new byte[10], 100);

        // Initial state: set highest versionstamp when task starts
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setHighestVersionstamp(tr, taskSubspace, taskId, highestValue);
            IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.RUNNING);
            tr.commit().join();
        }

        // Intermediate state: update cursor during processing
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setCursorVersionstamp(tr, taskSubspace, taskId, cursorValue);
            tr.commit().join();
        }

        // Final state: mark as completed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setCursorVersionstamp(tr, taskSubspace, taskId, highestValue);
            IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.COMPLETED);
            tr.commit().join();
        }

        // Verify final state
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);

            assertEquals(highestValue, state.cursorVersionstamp());
            assertEquals(highestValue, state.highestVersionstamp());
            assertEquals(IndexTaskStatus.COMPLETED, state.status());
            assertNull(state.error());
        }
    }

    @Test
    void testFailedTaskWithError() {
        String errorMessage = "Index build failed: document validation error";

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.RUNNING);
            tr.commit().join();
        }

        // Task fails during execution
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setError(tr, taskSubspace, taskId, errorMessage);
            IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.FAILED);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);

            assertEquals(IndexTaskStatus.FAILED, state.status());
            assertEquals(errorMessage, state.error());
            assertTrue(IndexBuildingTaskState.isTerminal(state.status()));
        }
    }

    @Test
    void testIsTerminalForCompletedStatus() {
        assertTrue(IndexBuildingTaskState.isTerminal(IndexTaskStatus.COMPLETED),
                "COMPLETED should be a terminal status");
    }

    @Test
    void testIsTerminalForFailedStatus() {
        assertTrue(IndexBuildingTaskState.isTerminal(IndexTaskStatus.FAILED),
                "FAILED should be a terminal status");
    }

    @Test
    void testIsTerminalForStoppedStatus() {
        assertTrue(IndexBuildingTaskState.isTerminal(IndexTaskStatus.STOPPED),
                "STOPPED should be a terminal status");
    }

    @Test
    void testIsNotTerminalForWaitingStatus() {
        assertFalse(IndexBuildingTaskState.isTerminal(IndexTaskStatus.WAITING),
                "WAITING should not be a terminal status");
    }

    @Test
    void testIsNotTerminalForRunningStatus() {
        assertFalse(IndexBuildingTaskState.isTerminal(IndexTaskStatus.RUNNING),
                "RUNNING should not be a terminal status");
    }

    @Test
    void testMultipleFieldUpdatesInSingleTransaction() {
        Versionstamp cursorValue = Versionstamp.complete(new byte[10], 50);
        Versionstamp highestValue = Versionstamp.complete(new byte[10], 200);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setCursorVersionstamp(tr, taskSubspace, taskId, cursorValue);
            IndexBuildingTaskState.setHighestVersionstamp(tr, taskSubspace, taskId, highestValue);
            IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.RUNNING);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);

            assertEquals(cursorValue, state.cursorVersionstamp());
            assertEquals(highestValue, state.highestVersionstamp());
            assertEquals(IndexTaskStatus.RUNNING, state.status());
        }
    }

    @Test
    void testStatusTransitionFromWaitingToRunning() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState initialState = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
            assertEquals(IndexTaskStatus.WAITING, initialState.status());
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.RUNNING);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState updatedState = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
            assertEquals(IndexTaskStatus.RUNNING, updatedState.status());
            assertFalse(IndexBuildingTaskState.isTerminal(updatedState.status()));
        }
    }

    @Test
    void testStatusTransitionFromRunningToCompleted() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.RUNNING);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.COMPLETED);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
            assertEquals(IndexTaskStatus.COMPLETED, state.status());
            assertTrue(IndexBuildingTaskState.isTerminal(state.status()));
        }
    }

    @Test
    void testCursorProgressTracking() {
        Versionstamp highestValue = Versionstamp.complete(new byte[10], 1000);
        Versionstamp cursor1 = Versionstamp.complete(new byte[10], 100);
        Versionstamp cursor2 = Versionstamp.complete(new byte[10], 500);
        Versionstamp cursor3 = Versionstamp.complete(new byte[10], 1000);

        // Set highest versionstamp
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setHighestVersionstamp(tr, taskSubspace, taskId, highestValue);
            IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.RUNNING);
            tr.commit().join();
        }

        // Progress update 1
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setCursorVersionstamp(tr, taskSubspace, taskId, cursor1);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
            assertEquals(cursor1, state.cursorVersionstamp());
            assertNotEquals(state.cursorVersionstamp(), state.highestVersionstamp());
        }

        // Progress update 2
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setCursorVersionstamp(tr, taskSubspace, taskId, cursor2);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
            assertEquals(cursor2, state.cursorVersionstamp());
        }

        // Final progress update - cursor reaches highest
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setCursorVersionstamp(tr, taskSubspace, taskId, cursor3);
            IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.COMPLETED);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
            assertEquals(cursor3, state.cursorVersionstamp());
            assertEquals(state.cursorVersionstamp(), state.highestVersionstamp());
            assertEquals(IndexTaskStatus.COMPLETED, state.status());
        }
    }

    @Test
    void testErrorMessageWithUnicodeCharacters() {
        String errorMessage = "Index build failed: 文档验证错误 (Document validation error) - エラー";

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setError(tr, taskSubspace, taskId, errorMessage);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
            assertEquals(errorMessage, state.error());
        }
    }

    @Test
    void testStoppedTaskLifecycle() {
        Versionstamp cursorValue = Versionstamp.complete(new byte[10], 50);
        Versionstamp highestValue = Versionstamp.complete(new byte[10], 100);

        // Task starts running
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setHighestVersionstamp(tr, taskSubspace, taskId, highestValue);
            IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.RUNNING);
            tr.commit().join();
        }

        // Task makes some progress
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setCursorVersionstamp(tr, taskSubspace, taskId, cursorValue);
            tr.commit().join();
        }

        // Task is manually stopped
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState.setStatus(tr, taskSubspace, taskId, IndexTaskStatus.STOPPED);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);

            assertEquals(IndexTaskStatus.STOPPED, state.status());
            assertEquals(cursorValue, state.cursorVersionstamp());
            assertNotEquals(state.cursorVersionstamp(), state.highestVersionstamp());
            assertTrue(IndexBuildingTaskState.isTerminal(state.status()));
        }
    }
}
