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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class SegmentMetadata {
    private final byte[] cardinalityKey;
    private final byte[] usedBytesKey;

    SegmentMetadata(VolumeSubspace subspace, String name) {
        this.cardinalityKey = subspace.packSegmentCardinalityKey(name);
        this.usedBytesKey = subspace.packSegmentUsedBytesKey(name);
    }

    void addCardinality(Transaction tr, byte[] delta) {
        tr.mutate(MutationType.ADD, cardinalityKey, delta);
    }

    int getCardinality(Transaction tr) {
        byte[] data = tr.get(cardinalityKey).join();
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    void addUsedBytes(Transaction tr, long length) {
        byte[] delta = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(length).array();
        tr.mutate(MutationType.ADD, usedBytesKey, delta);
    }

    long getUsedBytes(Transaction tr) {
        byte[] data = tr.get(usedBytesKey).join();
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }
}
