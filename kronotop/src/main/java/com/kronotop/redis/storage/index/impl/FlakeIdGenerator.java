/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.redis.storage.index.impl;

public class FlakeIdGenerator {
    private static final int PARTITION_ID_BITS = 14;
    private static final int SEQUENCE_BITS = 50;
    private static final long maxSequence = (1L << SEQUENCE_BITS) - 1;
    private final long partitionId;
    private volatile long sequence = 0L;

    public FlakeIdGenerator(long partitionId) {
        this.partitionId = partitionId;
    }

    public static long[] parse(long id) {
        long maskPartitionId = ((1L << PARTITION_ID_BITS) - 1) << SEQUENCE_BITS;
        long maskSequence = (1L << SEQUENCE_BITS) - 1;
        long partitionId = (id & maskPartitionId) >> SEQUENCE_BITS;
        long sequence = id & maskSequence;

        return new long[]{partitionId, sequence};
    }

    public synchronized long nextId() {
        sequence = (sequence + 1) & maxSequence;
        return (partitionId << SEQUENCE_BITS) | sequence;
    }
}
