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

package com.kronotop.bucket.handlers.protocol;

import java.util.HashMap;
import java.util.Map;

/**
 * Keyword arguments accepted by BUCKET.VECTOR after the positional arguments.
 */
public enum VectorArgumentKey {
    FILTER("FILTER"),
    TOP("TOP"),
    THRESHOLD("THRESHOLD"),
    MAX_SCAN_CANDIDATES("MAX-SCAN-CANDIDATES"),
    OVERQUERY("OVERQUERY"),
    PROJECTION("PROJECTION"),
    COLLATION("COLLATION");

    private static final Map<String, VectorArgumentKey> LOOKUP = new HashMap<>();

    static {
        for (VectorArgumentKey key : values()) {
            LOOKUP.put(key.value, key);
        }
    }

    private final String value;

    VectorArgumentKey(String value) {
        this.value = value;
    }

    /**
     * Returns the key for an upper-case keyword, or {@code null} if the keyword is unknown.
     */
    public static VectorArgumentKey findByValue(String value) {
        return LOOKUP.get(value);
    }

    public String getValue() {
        return value;
    }
}
