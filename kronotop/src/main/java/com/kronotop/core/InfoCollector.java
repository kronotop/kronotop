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

package com.kronotop.core;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Collects INFO sections as ordered key-value pairs and renders them in the
 * "# Section" and "key:value" text format. Several writers may add fields to
 * the same section. Insertion order is kept for both sections and fields.
 */
public class InfoCollector {
    private static final String CRLF = "\r\n";

    private final LinkedHashMap<String, LinkedHashMap<String, String>> sections = new LinkedHashMap<>();

    /**
     * Adds a field to the given section. The section is created on first use.
     */
    public void put(String section, String key, Object value) {
        sections.computeIfAbsent(section, ignored -> new LinkedHashMap<>()).put(key, String.valueOf(value));
    }

    /**
     * Renders all sections.
     */
    public String render() {
        return render(null);
    }

    /**
     * Renders the sections whose lower-case name is in the filter. A null filter
     * renders everything. Sections are separated by an empty line.
     */
    public String render(Collection<String> filter) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, LinkedHashMap<String, String>> section : sections.entrySet()) {
            if (filter != null && !filter.contains(section.getKey().toLowerCase())) {
                continue;
            }
            if (!sb.isEmpty()) {
                sb.append(CRLF);
            }
            sb.append("# ").append(section.getKey()).append(CRLF);
            for (Map.Entry<String, String> field : section.getValue().entrySet()) {
                sb.append(field.getKey()).append(':').append(field.getValue()).append(CRLF);
            }
        }
        return sb.toString();
    }
}
