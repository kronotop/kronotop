/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.handlers.protocol;

import java.util.HashMap;
import java.util.Map;

public enum CreateArgumentKey {
    SHARDS("shards"),
    INDEXES("indexes"),
    COLLATION("collation"),
    IF_NOT_EXISTS("if-not-exists");

    private static final Map<String, CreateArgumentKey> LOOKUP = new HashMap<>();

    static {
        for (CreateArgumentKey key : values()) {
            LOOKUP.put(key.name.toLowerCase(), key);
        }
    }

    private final String name;

    CreateArgumentKey(String name) {
        this.name = name;
    }

    public static CreateArgumentKey findByName(String raw) {
        return LOOKUP.get(raw.toLowerCase());
    }
}
