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

import com.kronotop.redis.storage.persistence.BaseRedisValue;

public class HashFieldValue extends BaseRedisValue<byte[]> {
    private long ttl;

    public HashFieldValue(byte[] value) {
        super(value);
    }

    public HashFieldValue(byte[] value, long ttl) {
        this(value);
        this.ttl = ttl;
    }

    public long ttl() {
        return ttl;
    }

    public void setTTL(long ttl) {
        this.ttl = ttl;
    }
}