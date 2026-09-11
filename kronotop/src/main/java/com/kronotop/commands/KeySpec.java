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
package com.kronotop.commands;

import java.util.List;

/**
 * Describes where the keys of a command are found in its arguments.
 */
public record KeySpec(
        String notes,
        List<KeySpecFlag> flags,
        BeginSearch beginSearch,
        FindKeys findKeys
) {
    public KeySpec {
        if (beginSearch == null || findKeys == null) {
            throw new IllegalArgumentException("key spec requires begin_search and find_keys");
        }
        flags = flags == null ? List.of() : List.copyOf(flags);
    }

    /**
     * True when the spec has both an index begin_search and a range find_keys.
     */
    public boolean isIndexRange() {
        return beginSearch.type() == BeginSearch.Type.INDEX && findKeys.type() == FindKeys.Type.RANGE;
    }
}
