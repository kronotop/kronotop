/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.volume;

// SegmentLog
// <segment-name><versionstamped-key><epoch> = <operation><position><length>

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;

import java.time.Instant;

import static com.kronotop.volume.Prefixes.SEGMENT_LOG_PREFIX;

class SegmentLog {
    private final Context context;
    private final Segment segment;
    private final VolumeConfig config;

    SegmentLog(Context context, Segment segment, VolumeConfig config) {
        this.context = context;
        this.segment = segment;
        this.config = config;
    }

    void append(Session session, int userVersion, SegmentLogValue value) {
        Tuple preKey = Tuple.from(
                SEGMENT_LOG_PREFIX,
                segment.getName(),
                Versionstamp.incomplete(userVersion),
                Instant.now().toEpochMilli()
        );
        byte[] key = config.subspace().packWithVersionstamp(preKey);
        session.getTransaction().mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, value.encode().array());
    }
}
