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

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InfoCollectorTest {

    @Test
    void shouldRenderSectionsInInsertionOrder() {
        // Behavior: sections and fields are rendered in the order they were added,
        // separated by an empty line
        InfoCollector collector = new InfoCollector();
        collector.put("Server", "a", 1);
        collector.put("Server", "b", "x");
        collector.put("Cluster", "c", 2);

        assertEquals("# Server\r\na:1\r\nb:x\r\n\r\n# Cluster\r\nc:2\r\n", collector.render());
    }

    @Test
    void shouldMergeFieldsFromMultipleWriters() {
        // Behavior: a second put to an existing section appends to that section
        // instead of creating a new one
        InfoCollector collector = new InfoCollector();
        collector.put("Cluster", "a", 1);
        collector.put("Server", "b", 2);
        collector.put("Cluster", "c", 3);

        assertEquals("# Cluster\r\na:1\r\nc:3\r\n\r\n# Server\r\nb:2\r\n", collector.render());
    }

    @Test
    void shouldFilterSectionsCaseInsensitively() {
        // Behavior: render with a filter keeps only the sections whose lower-case
        // name is in the filter
        InfoCollector collector = new InfoCollector();
        collector.put("Server", "a", 1);
        collector.put("Cluster", "b", 2);

        assertEquals("# Cluster\r\nb:2\r\n", collector.render(Set.of("cluster")));
    }

    @Test
    void shouldRenderEmptyForUnknownFilter() {
        // Behavior: a filter that matches no section renders an empty string
        InfoCollector collector = new InfoCollector();
        collector.put("Server", "a", 1);

        assertEquals("", collector.render(Set.of("nope")));
    }
}
