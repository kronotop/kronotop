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

import java.util.List;

public interface InternalAsyncCommands<K, V> {
    RedisFuture<List<Object>> segmentrange(String volume, long segmentId, List<SegmentRange> ranges);

    RedisFuture<String> segmentinsert(String volume, long segmentId, PackedEntry... entries);

    RedisFuture<List<Long>> segmentTailPointer(String volume, long segmentId);

    RedisFuture<List<Long>> listSegments(String volume);

    RedisFuture<Long> changelogWatch(String volume, long sequenceNumber);

    RedisFuture<VolumeInspectCursorResponse> volumeInspectCursor(String volume);

    RedisFuture<VolumeInspectReplicationResponse> volumeInspectReplication(String shardKind, int shardId, String standbyId);

    RedisFuture<List<ChangeLogEntryResponse>> changelogRange(String volume, String parentOperationKind, String start, String end, ChangeLogRangeArgs changelogArgs);

    RedisFuture<String> ping();
}
