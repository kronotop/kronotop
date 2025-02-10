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

package com.kronotop.volume;

import com.google.common.hash.HashCode;

import static com.google.common.hash.Hashing.sipHash24;

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
        return prefix.hashCode == this.hashCode;
    }
}
