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

import java.util.List;
import java.util.Map;

public interface InternalCommands<K, V> {
    List<Object> segmentrange(String volume, long segmentId, List<SegmentRange> ranges);

    String segmentinsert(String volume, long segmentId, PackedEntry... entries);

    List<Long> listSegments(String volume);

    List<Long> segmentTailPointer(String volume, long segmentId);

    Long changelogWatch(String volume, long sequenceNumber);

    VolumeInspectCursorResponse volumeInspectCursor(String volume);

    List<ChangeLogEntryResponse> changelogRange(String volume, String parentOperationKind, String start, String end, ChangeLogRangeArgs changelogArgs);

    String ping();
}
