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

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.IntegerListOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;

import java.util.List;

import static io.lettuce.core.protocol.CommandType.PING;

public class InternalCommandBuilder<K, V> extends BaseInternalCommandBuilder<K, V> {
    public InternalCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    public Command<K, V, List<Object>> segmentrange(String volume, long segmentId, List<SegmentRange> ranges) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(volume).add(segmentId);
        for (SegmentRange range : ranges) {
            args.add(range.position());
            args.add(range.length());
        }
        return createCommand(ReplicationCommandType.SEGMENTRANGE, new ArrayOutput<>(codec), args);
    }

    public Command<K, V, String> segmentinsert(String volume, long segmentId, PackedEntry... entries) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(volume).add(segmentId);
        for (PackedEntry entry : entries) {
            args.add(entry.position());
            args.add(entry.data());
        }
        return createCommand(ReplicationCommandType.SEGMENTINSERT, new StatusOutput<>(codec), args);
    }

    public Command<K, V, List<Long>> segmentTailPointer(String volumeName, long segmentId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(volumeName).
                add(segmentId);
        return createCommand(ReplicationCommandType.SEGMENTTAILPOINTER, new IntegerListOutput<>(codec), args);
    }

    public Command<K, V, List<Long>> listSegments(String volumeName) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(VolumeAdminCommandType.LIST_SEGMENTS).
                add(volumeName);
        return createCommand(VolumeAdminCommandType.VOLUME_ADMIN, new IntegerListOutput<>(codec), args);
    }

    public Command<K, V, Long> changelogWatch(String volume, long sequenceNumber) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(volume).add(sequenceNumber);
        return createCommand(ReplicationCommandType.CHANGELOGWATCH, new IntegerOutput<>(codec), args);
    }

    public Command<K, V, VolumeInspectCursorResponse> volumeInspectCursor(String volume) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(VolumeInspectCommandType.CURSOR).add(volume);
        return createCommand(VolumeInspectCommandType.VOLUME_INSPECT, new VolumeInspectCursorOutput<>(codec), args);
    }

    public Command<K, V, VolumeInspectReplicationResponse> volumeInspectReplication(String shardKind, int shardId, String standbyId) {
        CommandArgs<K, V> args = new CommandArgs<>(codec)
                .add(VolumeInspectCommandType.REPLICATION)
                .add(shardKind)
                .add(shardId)
                .add(standbyId);
        return createCommand(VolumeInspectCommandType.VOLUME_INSPECT, new VolumeInspectReplicationOutput<>(codec), args);
    }

    public Command<K, V, List<ChangeLogEntryResponse>> changelogRange(String volume,
                                                                      String parentOperationKind,
                                                                      String start,
                                                                      String end,
                                                                      ChangeLogRangeArgs changelogArgs
    ) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).
                add(volume).
                add(parentOperationKind).
                add(start).
                add(end);
        if (changelogArgs != null) {
            changelogArgs.build(args);
        }
        return new Command(ReplicationCommandType.CHANGELOGRANGE, new ChangeLogRangeOutput<>(codec), args);
    }

    public Command<K, V, String> ping() {
        return createCommand(PING, new StatusOutput<>(codec));
    }
}
