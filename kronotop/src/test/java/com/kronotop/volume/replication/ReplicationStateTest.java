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

package com.kronotop.volume.replication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationStateTest extends BaseStandaloneInstanceTest {

    @Test
    void shouldSetAndReadPosition() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-set-position");
        long segmentId = 1L;
        long expectedPosition = 12345L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setPosition(tr, subspace, segmentId, expectedPosition);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationCursor cursor = ReplicationState.readCursor(tr, subspace);
            assertEquals(segmentId, cursor.segmentId());
            assertEquals(expectedPosition, cursor.position());
        }
    }

    @Test
    void shouldUpdatePositionForExistingSegment() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-update-position");
        long segmentId = 2L;
        long initialPosition = 100L;
        long updatedPosition = 500L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setPosition(tr, subspace, segmentId, initialPosition);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setPosition(tr, subspace, segmentId, updatedPosition);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationCursor cursor = ReplicationState.readCursor(tr, subspace);
            assertEquals(segmentId, cursor.segmentId());
            assertEquals(updatedPosition, cursor.position());
        }
    }

    @Test
    void shouldReturnLatestSegmentWhenMultipleSegmentsExist() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-multiple-segments");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setPosition(tr, subspace, 1L, 100L);
            ReplicationState.setPosition(tr, subspace, 2L, 200L);
            ReplicationState.setPosition(tr, subspace, 3L, 300L);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationCursor cursor = ReplicationState.readCursor(tr, subspace);
            assertEquals(3L, cursor.segmentId());
            assertEquals(300L, cursor.position());
        }
    }

    @Test
    void shouldReturnDefaultCursorWhenNoSegmentsExist() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-empty-segments");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationCursor cursor = ReplicationState.readCursor(tr, subspace);
            assertEquals(0L, cursor.segmentId());
            assertEquals(0L, cursor.position());
        }
    }

    @Test
    void shouldSetAndReadErrorMessage() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-error-message");
        long segmentId = 5L;
        String expectedMessage = "Replication error occurred";

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setErrorMessage(tr, subspace, segmentId, expectedMessage);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String actualMessage = ReplicationState.readErrorMessage(tr, subspace, segmentId);
            assertEquals(expectedMessage, actualMessage);
        }
    }

    @Test
    void shouldUpdateErrorMessageForExistingSegment() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-update-error");
        long segmentId = 6L;
        String initialMessage = "Initial error";
        String updatedMessage = "Updated error message";

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setErrorMessage(tr, subspace, segmentId, initialMessage);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setErrorMessage(tr, subspace, segmentId, updatedMessage);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String actualMessage = ReplicationState.readErrorMessage(tr, subspace, segmentId);
            assertEquals(updatedMessage, actualMessage);
        }
    }

    @Test
    void shouldReturnNullWhenErrorMessageNotExists() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-no-error");
        long segmentId = 7L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String message = ReplicationState.readErrorMessage(tr, subspace, segmentId);
            assertNull(message);
        }
    }

    @Test
    void shouldClearErrorMessage() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-clear-error");
        long segmentId = 100L;
        String errorMessage = "Some error occurred";

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setErrorMessage(tr, subspace, segmentId, errorMessage);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String message = ReplicationState.readErrorMessage(tr, subspace, segmentId);
            assertEquals(errorMessage, message);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.clearErrorMessage(tr, subspace, segmentId);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String message = ReplicationState.readErrorMessage(tr, subspace, segmentId);
            assertNull(message);
        }
    }

    @Test
    void shouldHandleMultipleErrorMessagesForDifferentSegments() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-multiple-errors");
        long segmentId1 = 8L;
        long segmentId2 = 9L;
        String message1 = "Error for segment 8";
        String message2 = "Error for segment 9";

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setErrorMessage(tr, subspace, segmentId1, message1);
            ReplicationState.setErrorMessage(tr, subspace, segmentId2, message2);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String actualMessage1 = ReplicationState.readErrorMessage(tr, subspace, segmentId1);
            String actualMessage2 = ReplicationState.readErrorMessage(tr, subspace, segmentId2);
            assertEquals(message1, actualMessage1);
            assertEquals(message2, actualMessage2);
        }
    }

    @Test
    void shouldHandleLargePositionValues() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-large-position");
        long segmentId = 10L;
        long largePosition = Long.MAX_VALUE;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setPosition(tr, subspace, segmentId, largePosition);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationCursor cursor = ReplicationState.readCursor(tr, subspace);
            assertEquals(segmentId, cursor.segmentId());
            assertEquals(largePosition, cursor.position());
        }
    }

    @Test
    void shouldIsolateDataAcrossDifferentSubspaces() {
        DirectorySubspace subspace1 = createOrOpenSubspaceUnderCluster("test-isolation-1");
        DirectorySubspace subspace2 = createOrOpenSubspaceUnderCluster("test-isolation-2");
        long segmentId = 11L;
        long position1 = 1000L;
        long position2 = 2000L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setPosition(tr, subspace1, segmentId, position1);
            ReplicationState.setPosition(tr, subspace2, segmentId, position2);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationCursor cursor1 = ReplicationState.readCursor(tr, subspace1);
            ReplicationCursor cursor2 = ReplicationState.readCursor(tr, subspace2);
            assertEquals(position1, cursor1.position());
            assertEquals(position2, cursor2.position());
        }
    }

    @Test
    void shouldSetAndReadStatus() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-status");
        long segmentId = 12L;
        ReplicationStatus expectedStatus = ReplicationStatus.RUNNING;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setStatus(tr, subspace, segmentId, expectedStatus);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationStatus actualStatus = ReplicationState.readStatus(tr, subspace, segmentId);
            assertEquals(expectedStatus, actualStatus);
        }
    }

    @Test
    void shouldUpdateStatusForExistingSegment() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-update-status");
        long segmentId = 13L;
        ReplicationStatus initialStatus = ReplicationStatus.WAITING;
        ReplicationStatus updatedStatus = ReplicationStatus.DONE;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setStatus(tr, subspace, segmentId, initialStatus);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setStatus(tr, subspace, segmentId, updatedStatus);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationStatus actualStatus = ReplicationState.readStatus(tr, subspace, segmentId);
            assertEquals(updatedStatus, actualStatus);
        }
    }

    @Test
    void shouldReturnWaitingWhenStatusNotExists() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-no-status");
        long segmentId = 14L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationStatus status = ReplicationState.readStatus(tr, subspace, segmentId);
            assertEquals(ReplicationStatus.WAITING, status);
        }
    }

    @Test
    void shouldHandleAllStatusValues() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-all-status");

        ReplicationStatus[] allStatuses = ReplicationStatus.values();
        for (int i = 0; i < allStatuses.length; i++) {
            long segmentId = 15L + i;
            ReplicationStatus status = allStatuses[i];

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReplicationState.setStatus(tr, subspace, segmentId, status);
                tr.commit().join();
            }

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReplicationStatus actualStatus = ReplicationState.readStatus(tr, subspace, segmentId);
                assertEquals(status, actualStatus);
            }
        }
    }

    @Test
    void shouldHandleMultipleStatusesForDifferentSegments() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-multiple-status");
        long segmentId1 = 20L;
        long segmentId2 = 21L;
        ReplicationStatus status1 = ReplicationStatus.RUNNING;
        ReplicationStatus status2 = ReplicationStatus.FAILED;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setStatus(tr, subspace, segmentId1, status1);
            ReplicationState.setStatus(tr, subspace, segmentId2, status2);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationStatus actualStatus1 = ReplicationState.readStatus(tr, subspace, segmentId1);
            ReplicationStatus actualStatus2 = ReplicationState.readStatus(tr, subspace, segmentId2);
            assertEquals(status1, actualStatus1);
            assertEquals(status2, actualStatus2);
        }
    }

    @Test
    void shouldSetAndReadTailPointer() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-tail-pointer");
        long segmentId = 50L;
        long sequenceNumber = 12345L;
        long nextPosition = 67890L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setTailPointer(tr, subspace, segmentId, sequenceNumber, nextPosition);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var result = ReplicationState.readTailPointer(tr, subspace, segmentId);
            assertEquals(2, result.size());
            assertEquals(sequenceNumber, result.get(0));
            assertEquals(nextPosition, result.get(1));
        }
    }

    @Test
    void shouldReturnEmptyListWhenTailPointerNotExists() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-no-tail-pointer");
        long segmentId = 51L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var result = ReplicationState.readTailPointer(tr, subspace, segmentId);
            assertTrue(result.isEmpty());
        }
    }

    @Test
    void shouldUpdateTailPointerForExistingSegment() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-update-tail-pointer");
        long segmentId = 52L;
        long initialSequenceNumber = 100L;
        long initialNextPosition = 200L;
        long updatedSequenceNumber = 500L;
        long updatedNextPosition = 600L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setTailPointer(tr, subspace, segmentId, initialSequenceNumber, initialNextPosition);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setTailPointer(tr, subspace, segmentId, updatedSequenceNumber, updatedNextPosition);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var result = ReplicationState.readTailPointer(tr, subspace, segmentId);
            assertEquals(2, result.size());
            assertEquals(updatedSequenceNumber, result.get(0));
            assertEquals(updatedNextPosition, result.get(1));
        }
    }

    @Test
    void shouldSetAndReadStage() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-stage");
        long segmentId = 60L;
        Stage expectedStage = Stage.SEGMENT_REPLICATION;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setStage(tr, subspace, segmentId, expectedStage);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Stage actualStage = ReplicationState.readStage(tr, subspace, segmentId);
            assertEquals(expectedStage, actualStage);
        }
    }

    @Test
    void shouldReturnNullWhenStageNotExists() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-no-stage");
        long segmentId = 61L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Stage stage = ReplicationState.readStage(tr, subspace, segmentId);
            assertNull(stage);
        }
    }

    @Test
    void shouldUpdateStageForExistingSegment() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-update-stage");
        long segmentId = 62L;
        Stage initialStage = Stage.SEGMENT_REPLICATION;
        Stage updatedStage = Stage.CHANGE_DATA_CAPTURE;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setStage(tr, subspace, segmentId, initialStage);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setStage(tr, subspace, segmentId, updatedStage);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Stage actualStage = ReplicationState.readStage(tr, subspace, segmentId);
            assertEquals(updatedStage, actualStage);
        }
    }

    @Test
    void shouldSetAndReadSequenceNumber() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-sequence-number");
        long segmentId = 70L;
        long expectedSequenceNumber = 12345L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setSequenceNumber(tr, subspace, segmentId, expectedSequenceNumber);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long actualSequenceNumber = ReplicationState.readSequenceNumber(tr, subspace, segmentId);
            assertEquals(expectedSequenceNumber, actualSequenceNumber);
        }
    }

    @Test
    void shouldReturnZeroWhenSequenceNumberNotExists() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-no-sequence-number");
        long segmentId = 71L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long sequenceNumber = ReplicationState.readSequenceNumber(tr, subspace, segmentId);
            assertEquals(0L, sequenceNumber);
        }
    }

    @Test
    void shouldUpdateSequenceNumberForExistingSegment() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-update-sequence-number");
        long segmentId = 72L;
        long initialSequenceNumber = 100L;
        long updatedSequenceNumber = 500L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setSequenceNumber(tr, subspace, segmentId, initialSequenceNumber);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationState.setSequenceNumber(tr, subspace, segmentId, updatedSequenceNumber);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long actualSequenceNumber = ReplicationState.readSequenceNumber(tr, subspace, segmentId);
            assertEquals(updatedSequenceNumber, actualSequenceNumber);
        }
    }
}