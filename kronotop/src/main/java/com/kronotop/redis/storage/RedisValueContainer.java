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

package com.kronotop.redis.storage;

import com.kronotop.redis.handlers.hash.HashFieldValue;
import com.kronotop.redis.handlers.hash.HashValue;
import com.kronotop.redis.handlers.string.StringValue;

public class RedisValueContainer {
    private final RedisValueKind kind;
    private StringValue stringValue;
    private HashValue hashValue;
    private HashFieldValue hashFieldValue;

    public RedisValueContainer(StringValue value) {
        this.kind = RedisValueKind.STRING;
        this.stringValue = value;
    }

    public RedisValueContainer(HashValue value) {
        this.kind = RedisValueKind.HASH;
        this.hashValue = value;
    }

    public RedisValueContainer(HashFieldValue value) {
        this.kind = RedisValueKind.HASH_FIELD;
        this.hashFieldValue = value;
    }

    public RedisValueKind kind() {
        return kind;
    }

    public StringValue string() {
        return stringValue;
    }

    public HashValue hash() {
        return hashValue;
    }

    public HashFieldValue hashField() {
        return hashFieldValue;
    }

    public BaseRedisValue<?> baseRedisValue() {
        return switch (kind) {
            case STRING -> string();
            case HASH_FIELD -> hashFieldValue;
            default -> throw new UnsupportedOperationException();
        };
    }
}
