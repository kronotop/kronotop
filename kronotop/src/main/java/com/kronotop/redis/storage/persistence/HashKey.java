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

package com.kronotop.redis.storage.persistence;

public class HashKey implements Key {
    private final String data;
    private final String field;
    private final int hashCode;

    public HashKey(String data, String field) {
        this.data = data;
        this.field = field;
        this.hashCode = hashCodeInternal();
    }

    @Override
    public KeyKind kind() {
        return KeyKind.HASH;
    }

    @Override
    public String data() {
        return data;
    }

    public String getField() {
        return field;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private int hashCodeInternal() {
        int result = data.hashCode();
        result = 31 * result + field.hashCode();
        return result;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
