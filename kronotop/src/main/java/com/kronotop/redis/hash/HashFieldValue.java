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

package com.kronotop.redis.hash;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.redis.storage.persistence.RedisValue;

public class HashFieldValue implements RedisValue {
    private byte[] value;
    private Versionstamp versionstamp;
    private Long ttl;

    public HashFieldValue(byte[] value) {
        this(value, 0);
    }

    public HashFieldValue(byte[] value, long ttl) {
        this(value, ttl, null);
    }

    public HashFieldValue(byte[] value, long ttl, Versionstamp versionstamp) {
        this.value = value;
        this.ttl = ttl;
        this.versionstamp = versionstamp;
    }

    public Long ttl() {
        return ttl;
    }

    public void setTtl(Long ttl) {
        this.ttl = ttl;
    }

    @Override
    public byte[] value() {
        return value;
    }

    @Override
    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public Versionstamp versionstamp() {
        return versionstamp;
    }

    @Override
    public void setVersionstamp(Versionstamp versionstamp) {
        this.versionstamp = versionstamp;
    }
}