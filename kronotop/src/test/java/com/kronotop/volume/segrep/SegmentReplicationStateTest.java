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

package com.kronotop.volume.segrep;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SegmentReplicationStateTest extends BaseStandaloneInstanceTest {

    @Test
    void shouldSetAndReadPosition() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-set-position");
        long segmentId = 1L;
        long expectedPosition = 12345L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SegmentReplicationState.setPosition(tr, subspace, segmentId, expectedPosition);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationCursor cursor = SegmentReplicationState.readCursor(tr, subspace);
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
            SegmentReplicationState.setPosition(tr, subspace, segmentId, initialPosition);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SegmentReplicationState.setPosition(tr, subspace, segmentId, updatedPosition);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationCursor cursor = SegmentReplicationState.readCursor(tr, subspace);
            assertEquals(segmentId, cursor.segmentId());
            assertEquals(updatedPosition, cursor.position());
        }
    }

    @Test
    void shouldReturnLatestSegmentWhenMultipleSegmentsExist() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-multiple-segments");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SegmentReplicationState.setPosition(tr, subspace, 1L, 100L);
            SegmentReplicationState.setPosition(tr, subspace, 2L, 200L);
            SegmentReplicationState.setPosition(tr, subspace, 3L, 300L);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationCursor cursor = SegmentReplicationState.readCursor(tr, subspace);
            assertEquals(3L, cursor.segmentId());
            assertEquals(300L, cursor.position());
        }
    }

    @Test
    void shouldReturnDefaultCursorWhenNoSegmentsExist() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-empty-segments");

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationCursor cursor = SegmentReplicationState.readCursor(tr, subspace);
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
            SegmentReplicationState.setErrorMessage(tr, subspace, segmentId, expectedMessage);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String actualMessage = SegmentReplicationState.readErrorMessage(tr, subspace, segmentId);
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
            SegmentReplicationState.setErrorMessage(tr, subspace, segmentId, initialMessage);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            SegmentReplicationState.setErrorMessage(tr, subspace, segmentId, updatedMessage);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String actualMessage = SegmentReplicationState.readErrorMessage(tr, subspace, segmentId);
            assertEquals(updatedMessage, actualMessage);
        }
    }

    @Test
    void shouldReturnNullWhenErrorMessageNotExists() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-no-error");
        long segmentId = 7L;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String message = SegmentReplicationState.readErrorMessage(tr, subspace, segmentId);
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
            SegmentReplicationState.setErrorMessage(tr, subspace, segmentId1, message1);
            SegmentReplicationState.setErrorMessage(tr, subspace, segmentId2, message2);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            String actualMessage1 = SegmentReplicationState.readErrorMessage(tr, subspace, segmentId1);
            String actualMessage2 = SegmentReplicationState.readErrorMessage(tr, subspace, segmentId2);
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
            SegmentReplicationState.setPosition(tr, subspace, segmentId, largePosition);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationCursor cursor = SegmentReplicationState.readCursor(tr, subspace);
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
            SegmentReplicationState.setPosition(tr, subspace1, segmentId, position1);
            SegmentReplicationState.setPosition(tr, subspace2, segmentId, position2);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationCursor cursor1 = SegmentReplicationState.readCursor(tr, subspace1);
            ReplicationCursor cursor2 = SegmentReplicationState.readCursor(tr, subspace2);
            assertEquals(position1, cursor1.position());
            assertEquals(position2, cursor2.position());
        }
    }
}