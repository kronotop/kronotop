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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

import static com.kronotop.volume.Prefixes.SEGMENT_LOG_CARDINALITY_PREFIX;
import static com.kronotop.volume.Prefixes.SEGMENT_LOG_PREFIX;

class SegmentLog {
    private static final byte[] CARDINALITY_INCREASE_DELTA = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian
    private final Segment segment;
    private final VolumeConfig config;
    private final byte[] cardinalityKey;

    SegmentLog(Segment segment, VolumeConfig config) {
        this.segment = segment;
        this.config = config;
        Tuple key = Tuple.from(
                SEGMENT_LOG_CARDINALITY_PREFIX,
                segment.getName()
        );
        this.cardinalityKey = config.subspace().pack(key);
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
        session.getTransaction().mutate(MutationType.ADD, cardinalityKey, CARDINALITY_INCREASE_DELTA);
    }

    int getCardinality(Session session) {
        byte[] data = session.getTransaction().get(cardinalityKey).join();
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }
}
