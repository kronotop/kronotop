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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Where the search for keys begins. Exactly one of index or keyword is set.
 * When both are null the spec type is unknown.
 */
@JsonIgnoreProperties({"unknown"})
public record BeginSearch(Index index, Keyword keyword) {
    public BeginSearch(Index index) {
        this(index, null);
    }

    public BeginSearch(Keyword keyword) {
        this(null, keyword);
    }

    public Type type() {
        if (index != null) {
            return Type.INDEX;
        }
        if (keyword != null) {
            return Type.KEYWORD;
        }
        return Type.UNKNOWN;
    }

    public enum Type {
        INDEX, KEYWORD, UNKNOWN;

        public String replyName() {
            return name().toLowerCase();
        }
    }
}
