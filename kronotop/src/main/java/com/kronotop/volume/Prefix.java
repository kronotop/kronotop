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

package com.kronotop.volume;

import com.google.common.hash.HashCode;

import static com.google.common.hash.Hashing.sipHash24;

/**
 * An 8-byte identifier derived by hashing input bytes (typically a UUID) through SipHash-2-4.
 *
 * <h2>Design Note</h2>
 * <p>
 * SipHash-2-4 compresses arbitrary input into a 64-bit (8-byte) output. The 64-bit hash space
 * gives a birthday-bound collision probability of ~50% at ~4.3 billion prefixes; for realistic
 * workloads (millions of buckets/indexes), collision probability is negligible (~1 in 37 million
 * at one million prefixes).
 * <p>
 * The 8-byte representation is chosen for two reasons:
 * <ul>
 *   <li><b>Space efficiency:</b> 8 bytes vs 16-byte raw UUID saves storage in every FDB key
 *       and EntryMetadata record (40 bytes instead of 48), which compounds across millions of entries
 *       in both FDB storage and network bandwidth during range scans.</li>
 *   <li><b>FDB key distribution:</b> SipHash output bytes are uniformly distributed, which aligns
 *       well with FDB's range-based sharding. In ENTRY_SUBSPACE, prefix is the first key component,
 *       so different buckets naturally spread across FDB shards. In ENTRY_METADATA_SUBSPACE, prefix
 *       is the second component (after segmentId), enabling efficient per-prefix scans within a segment.</li>
 * </ul>
 * <p>
 * Identity comparison uses the full 64-bit {@code asLong} value. The 32-bit {@code hashCode} is
 * only used for Java collection bucketing and does not affect correctness.
 */
public class Prefix {
    private final byte[] asBytes;
    private final long asLong;
    private final int hashCode;

    public Prefix(HashCode hashCode) {
        this.asBytes = hashCode.asBytes();
        this.asLong = hashCode.asLong();
        this.hashCode = hashCode.asInt();
    }

    public Prefix(String prefix) {
        this(prefix.getBytes());
    }

    public Prefix(byte[] prefix) {
        HashCode hashCode = sipHash24().hashBytes(prefix);
        this.asBytes = hashCode.asBytes();
        this.asLong = hashCode.asLong();
        this.hashCode = hashCode.asInt();
    }

    public static Prefix fromBytes(byte[] bytes) {
        HashCode hashCode = HashCode.fromBytes(bytes);
        return new Prefix(hashCode);
    }

    public static Prefix fromLong(long hash) {
        HashCode hashCode = HashCode.fromLong(hash);
        return new Prefix(hashCode);
    }

    public long asLong() {
        return asLong;
    }

    public byte[] asBytes() {
        return asBytes;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Prefix prefix)) {
            return false;
        }
        return prefix.asLong == this.asLong;
    }
}
