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

package com.kronotop.bucket.index;

/**
 * First tuple element of every key under an index subspace. It names the kind of record that follows.
 */
public enum IndexSubspaceMagic {
    /**
     * Index entries that queries scan, keyed by the indexed value and the ObjectId.
     */
    ENTRIES((byte) 0x01),

    /**
     * Maps an ObjectId to its indexed values so update and delete can find its entries.
     */
    BACK_POINTER((byte) 0x02),

    /**
     * Primary index only. Orders ObjectIds by the versionstamp of their Volume write.
     */
    VOLUME_POINTER((byte) 0x03),

    /**
     * Primary index only. Maps an ObjectId to its Volume versionstamp so its volume pointer can be removed.
     */
    VOLUME_POINTER_BACK_REF((byte) 0x04),

    /**
     * Sampled ObjectIds that the analyze routine uses as histogram pivots.
     */
    STAT_HINTS((byte) 0x05),

    /**
     * Back pointers from the index to its maintenance tasks, keyed by task versionstamp.
     */
    TASKS((byte) 0x06),

    /**
     * Vector insert, update and delete log used for crash recovery, keyed by generation and versionstamp.
     */
    VECTOR_MUTATION_LOG((byte) 0x07),

    /**
     * Ordinals deleted from an on-disk vector graph.
     */
    GRAPH_DELETED_ORDINALS((byte) 0x08),

    /**
     * Marks an on-heap vector graph as flushed to the local disk.
     */
    GRAPH_FLUSHED((byte) 0x09),

    /**
     * Marks a flushed vector graph as written to Volume.
     */
    GRAPH_SYNCED((byte) 0x0A),

    /**
     * Binds the Volume prefix that holds the chunks of a flushed vector graph.
     */
    GRAPH_PREFIX_BINDING_KEY((byte) 0x0B);

    public final byte value;

    IndexSubspaceMagic(byte kind) {
        value = kind;
    }

    public byte getValue() {
        return value;
    }
}
