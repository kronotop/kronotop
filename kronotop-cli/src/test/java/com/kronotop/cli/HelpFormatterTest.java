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

package com.kronotop.cli;

import com.kronotop.cli.CommandDocsCatalog.CommandDoc;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HelpFormatterTest {

    @Test
    void shouldRenderNameUsageAndFields() {
        // Behavior: plain output has a leading blank line, the name with usage, then one line per field
        CommandDoc doc = new CommandDoc(List.of("bucket.query"), "BUCKET.QUERY",
                List.of("bucket", "query", "[LIMIT", "limit]"), "Queries documents.", "2026.06-1", "bucket");
        String expected = """

                  BUCKET.QUERY bucket query [LIMIT limit]
                  summary: Queries documents.
                  since: 2026.06-1
                  group: bucket
                """;
        assertEquals(expected, HelpFormatter.format(doc, null, true));
    }

    @Test
    void shouldSkipGroupLineWhenNotRequested() {
        // Behavior: a group listing hides the group line of each entry
        CommandDoc doc = new CommandDoc(List.of("ping"), "PING", List.of(), "Pings the server.", "1.0", "connection");
        String expected = """

                  PING
                  summary: Pings the server.
                  since: 1.0
                """;
        assertEquals(expected, HelpFormatter.format(doc, null, false));
    }

    @Test
    void shouldSkipEmptyFieldsAndUsage() {
        // Behavior: a command without arguments shows only its name, empty fields are not printed
        CommandDoc doc = new CommandDoc(List.of("ping"), "PING", List.of(), "Pings the server.", "", "");
        String expected = """

                  PING
                  summary: Pings the server.
                """;
        assertEquals(expected, HelpFormatter.format(doc, null, true));
    }

    @Test
    void shouldReplaceNonBreakingSpaceInUsage() {
        // Behavior: the non-breaking space used inside hint tokens becomes a normal space in help output
        CommandDoc doc = new CommandDoc(List.of("x"), "X", List.of("[SORTBY", "field", "ASC|DESC]", "a b"),
                "", "", "");
        assertEquals("\n  X [SORTBY field ASC|DESC] a b\n", HelpFormatter.format(doc, null, true));
    }

    @Test
    void shouldStartGenericHelpWithVersionLine() {
        // Behavior: generic help starts with the given version line and lists the help topics and :set options
        String text = HelpFormatter.generic("kronotop-cli 1.0");
        assertTrue(text.startsWith("kronotop-cli 1.0\n"));
        assertTrue(text.contains("\"help @<group>\""));
        assertTrue(text.contains("\"help <command>\""));
        assertTrue(text.contains("\"help <tab>\""));
        assertTrue(text.contains("\":set nohints\""));
        assertFalse(text.endsWith("\n"));
    }
}
