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
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.volume.SegmentTailPointer;
import com.kronotop.volume.SegmentUtil;
import com.kronotop.volume.VolumeNames;
import com.kronotop.volume.changelog.ChangeLog;

import java.util.List;

public class ReplicationUtil {

    public static DirectorySubspace openStandbySubspace(Context context, Transaction tr, String volume, String standbyId) {
        KronotopDirectoryNode node = KronotopDirectory.kronotop().
                cluster(context.getClusterName()).
                metadata().
                volumes().
                bucket().
                volume(volume).
                standby(standbyId);
        return DirectoryLayer.getDefault().open(tr, node.toList()).join();
    }

    public static void assertStandbyCaughtUp(Context context,
                                             Transaction tr,
                                             Member standby,
                                             DirectorySubspace primarySubspace,
                                             ShardKind shardKind,
                                             int shardId
    ) {
        long activeSegmentId = SegmentUtil.findActiveSegmentId(tr, primarySubspace);
        SegmentTailPointer pointer = SegmentUtil.locateTailPointer(tr, primarySubspace, activeSegmentId);
        long sequenceNumber = ChangeLog.resolveTailSequenceNumber(tr, primarySubspace, activeSegmentId, pointer);

        DirectorySubspace standbySubspace = openStandbySubspace(context, tr, VolumeNames.format(shardKind, shardId), standby.getId());
        Stage standbyStage = ReplicationState.readStage(tr, standbySubspace, activeSegmentId);
        if (standbyStage != Stage.CHANGE_DATA_CAPTURE) {
            throw new KronotopException("Standby is not caught up: stage is " + standbyStage);
        }

        ReplicationCursor cursor = ReplicationState.readCursor(tr, standbySubspace);
        if (cursor.segmentId() != activeSegmentId) {
            throw new KronotopException(
                    "Standby is not caught up: cursor segmentId=" + cursor.segmentId()
                            + " but activeSegmentId=" + activeSegmentId
            );
        }
        if (cursor.position() != pointer.nextPosition()) {
            throw new KronotopException(
                    "Standby is not caught up: cursor position=" + cursor.position()
                            + " but expected=" + pointer.nextPosition()
            );
        }

        long standbySequenceNumber = ReplicationState.readSequenceNumber(tr, standbySubspace, cursor.segmentId());
        if (sequenceNumber != standbySequenceNumber) {
            throw new KronotopException(
                    "Standby is not caught up: sequenceNumber=" + sequenceNumber
                            + " but standbySequenceNumber=" + standbySequenceNumber
            );
        }
    }

    public static ReplicationStatusInfo readReplicationStatusInfo(Transaction tr, DirectorySubspace standbySubspace) {
        // Root level - cursor first to extract segmentId
        ReplicationCursor cursor = ReplicationState.readCursor(tr, standbySubspace);
        long segmentId = cursor.segmentId();

        Stage stage = ReplicationState.readStage(tr, standbySubspace, segmentId);
        ReplicationStatus status = ReplicationState.readStatus(tr, standbySubspace, segmentId);
        String errorMessage = ReplicationState.readErrorMessage(tr, standbySubspace, segmentId);

        // CDC stage
        long sequenceNumber = ReplicationState.readSequenceNumber(tr, standbySubspace, segmentId);
        long position = ReplicationState.readPosition(tr, standbySubspace, segmentId);
        ReplicationStatusInfo.ChangeDataCaptureStage cdcStageInfo = new ReplicationStatusInfo.ChangeDataCaptureStage(sequenceNumber, position);

        // Segment replication
        List<Long> tailPointer = ReplicationState.readTailPointer(tr, standbySubspace, segmentId);
        ReplicationStatusInfo.SegmentReplicationStage segmentReplicationInfo;
        if (tailPointer.isEmpty()) {
            segmentReplicationInfo = new ReplicationStatusInfo.SegmentReplicationStage(0, 0);
        } else {
            segmentReplicationInfo = new ReplicationStatusInfo.SegmentReplicationStage(tailPointer.get(0), tailPointer.get(1));
        }

        return new ReplicationStatusInfo(stage, cursor, status, errorMessage, cdcStageInfo, segmentReplicationInfo);
    }
}
