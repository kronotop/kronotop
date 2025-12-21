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

package com.kronotop.cluster;

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class Heartbeat {
    private static final byte[] HEARTBEAT_DELTA = new byte[]{1, 0, 0, 0, 0, 0, 0, 0}; // 1, byte order: little-endian

    static long get(@Nonnull Transaction tr, @Nonnull DirectorySubspace subspace) {
        long heartbeat = 0;
        byte[] data = tr.get(subspace.pack(MemberSubspace.HEARTBEAT.getValue())).join();
        if (data != null) {
            heartbeat = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
        }
        return heartbeat;
    }

    static void set(@Nonnull Transaction tr, @Nonnull DirectorySubspace subspace) {
        byte[] key = subspace.pack(MemberSubspace.HEARTBEAT.getValue());
        tr.mutate(MutationType.ADD, key, HEARTBEAT_DELTA);
    }
}
