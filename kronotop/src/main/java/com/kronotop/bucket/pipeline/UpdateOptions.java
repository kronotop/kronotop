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

package com.kronotop.bucket.pipeline;

import org.bson.BsonValue;

import java.util.*;

public final class UpdateOptions {
    public static final String SET = "$set";
    public static final String UNSET = "$unset";
    private final Map<String, BsonValue> setOps;
    private final Set<String> unsetOps;

    private UpdateOptions(Builder b) {
        this.setOps = Collections.unmodifiableMap(new LinkedHashMap<>(b.setOps));
        this.unsetOps = Collections.unmodifiableSet(new LinkedHashSet<>(b.unsetOps));
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, BsonValue> setOps() {
        return setOps;
    }

    public Set<String> unsetOps() {
        return unsetOps;
    }

    public static final class Builder {
        private final Map<String, BsonValue> setOps = new LinkedHashMap<>();
        private final Set<String> unsetOps = new LinkedHashSet<>();

        private static void requireField(String f) {
            if (f == null || f.isBlank()) {
                throw new IllegalArgumentException("field is null/blank");
            }
        }

        // --- fluent ops ---
        public Builder set(String field, BsonValue value) {
            requireField(field);
            setOps.put(field, value);
            return this;
        }

        public Builder unset(String field) {
            requireField(field);
            unsetOps.add(field);
            return this;
        }

        // --- build ---
        public UpdateOptions build() {
            if (!unsetOps.isEmpty()) {
                unsetOps.forEach(setOps::remove);
            }
            return new UpdateOptions(this);
        }
    }
}
