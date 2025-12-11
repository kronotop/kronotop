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

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.RedisCommand;

import java.util.List;

public abstract class AbstractInternalAsyncCommands<K, V> implements InternalAsyncCommands<K, V> {
    private final InternalCommandBuilder<K, V> commandBuilder;
    private final StatefulConnection<K, V> connection;

    protected AbstractInternalAsyncCommands(StatefulConnection<K, V> connection, RedisCodec<K, V> codec) {
        this.commandBuilder = new InternalCommandBuilder<>(codec);
        this.connection = connection;
    }

    @Override
    public RedisFuture<String> ping() {
        return dispatch(commandBuilder.ping());
    }

    @Override
    public RedisFuture<List<Object>> segmentrange(String volume, long segmentId, List<SegmentRange> ranges) {
        return dispatch(commandBuilder.segmentrange(volume, segmentId, ranges));
    }

    @Override
    public RedisFuture<String> segmentinsert(String volume, long segmentId, PackedEntry... entries) {
        return dispatch(commandBuilder.segmentinsert(volume, segmentId, entries));
    }

    @Override
    public RedisFuture<List<Long>> segmentTailPointer(String volume, long segmentId) {
        return dispatch(commandBuilder.segmentTailPointer(volume, segmentId));
    }

    @Override
    public RedisFuture<List<Long>> listSegments(String volume) {
        return dispatch(commandBuilder.listSegments(volume));
    }

    @Override
    public RedisFuture<Long> changelogWatch(String volume, long sequenceNumber) {
        return dispatch(commandBuilder.changelogWatch(volume, sequenceNumber));
    }

    @Override
    public RedisFuture<List<ChangeLogEntryResponse>> changelogRange(String volume,
                                                                    String parentOperationKind,
                                                                    String start,
                                                                    String end,
                                                                    ChangeLogRangeArgs changelogArgs
    ) {
        return dispatch(commandBuilder.changelogRange(volume, parentOperationKind, start, end, changelogArgs));
    }

    @Override
    public RedisFuture<VolumeInspectCursorResponse> volumeInspectCursor(String volume) {
        return dispatch(commandBuilder.volumeInspectCursor(volume));
    }

    @Override
    public RedisFuture<VolumeInspectReplicationResponse> volumeInspectReplication(String shardKind, int shardId, String standbyId) {
        return dispatch(commandBuilder.volumeInspectReplication(shardKind, shardId, standbyId));
    }

    private <T> AsyncCommand<K, V, T> dispatch(RedisCommand<K, V, T> cmd) {
        AsyncCommand<K, V, T> asyncCommand = new AsyncCommand<>(cmd);
        RedisCommand<K, V, T> dispatched = connection.dispatch(asyncCommand);
        if (dispatched instanceof AsyncCommand) {
            return (AsyncCommand<K, V, T>) dispatched;
        }
        return asyncCommand;
    }

}