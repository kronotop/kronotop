/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.cluster.sharding;

import io.lettuce.core.cluster.SlotHash;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps Redis Cluster hash slots to Stash shard IDs. The slot of a key or channel is computed with
 * CRC16, and the resulting slot is distributed across the configured number of shards. Keeping this
 * logic in one place guarantees that every component routing by slot agrees on the same mapping.
 */
public final class HashSlots {
    public static final int NUM_HASH_SLOTS = 16384;

    private HashSlots() {
    }

    /**
     * Computes the hash slot for the given key or channel.
     *
     * @param value the key or channel name
     * @return the hash slot in the range [0, NUM_HASH_SLOTS)
     */
    public static int slot(String value) {
        return SlotHash.getSlot(value);
    }

    /**
     * Distributes the hash slots evenly across the given number of shards.
     *
     * @param numberOfShards the number of shards to distribute slots over
     * @return an unmodifiable map where the key is the hash slot and the value is the shard ID
     */
    public static Map<Integer, Integer> distribute(int numberOfShards) {
        HashMap<Integer, Integer> result = new HashMap<>();
        int hashSlotsPerShard = NUM_HASH_SLOTS / numberOfShards;
        int counter = 1;
        int partId = 0;
        for (int hashSlot = 0; hashSlot < NUM_HASH_SLOTS; hashSlot++) {
            if (counter >= hashSlotsPerShard) {
                counter = 1;
                partId++;
                if (partId >= numberOfShards) {
                    partId = 0;
                }
            }
            result.put(hashSlot, partId);
            counter++;
        }
        return Collections.unmodifiableMap(result);
    }
}
