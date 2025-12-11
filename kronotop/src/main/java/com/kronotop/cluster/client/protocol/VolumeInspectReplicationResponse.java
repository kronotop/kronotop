/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.cluster.client.protocol;

/**
 * Represents the response from the VOLUME.INSPECT REPLICATION command.
 */
public record VolumeInspectReplicationResponse(
        String stage,
        Cursor cursor,
        String status,
        String errorMessage,
        CDCStage cdcStage,
        SegmentReplicationStage segmentReplicationStage
) {

    /**
     * Cursor position in the replication stream.
     */
    public record Cursor(
            long segmentId,
            long position
    ) {
    }

    /**
     * CDC (Change Data Capture) stage information.
     */
    public record CDCStage(
            long sequenceNumber,
            long position
    ) {
    }

    /**
     * Segment replication stage information.
     */
    public record SegmentReplicationStage(
            long tailSequenceNumber,
            long tailNextPosition
    ) {
    }
}
