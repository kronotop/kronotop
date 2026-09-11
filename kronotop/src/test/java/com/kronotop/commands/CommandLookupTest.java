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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CommandLookupTest {

    private static CommandMetadata metadata(Map<String, CommandMetadata> subcommands) {
        return new CommandMetadata("Summary", null, CommandGroup.CONNECTION, "2026.06-1", -2, null,
                null, null, null, null, null, null, null, null, null, null, subcommands);
    }

    private static Map<String, CommandMetadata> commands() {
        CommandMetadata setname = metadata(Map.of());
        return Map.of("CLIENT", metadata(Map.of("SETNAME", setname)), "PING", metadata(Map.of()));
    }

    @Test
    void shouldFindTopLevelCommand() {
        // Behavior: a plain name resolves to the top-level command with a lowercase full name
        CommandLookup.Match match = CommandLookup.find(commands(), "Ping");

        assertNotNull(match);
        assertEquals("ping", match.fullName());
    }

    @Test
    void shouldFindSubcommandByPipeName() {
        // Behavior: "container|sub" resolves to the subcommand, without regard to case
        Map<String, CommandMetadata> commands = commands();
        CommandLookup.Match lower = CommandLookup.find(commands, "client|setname");
        CommandLookup.Match upper = CommandLookup.find(commands, "CLIENT|SETNAME");

        assertNotNull(lower);
        assertNotNull(upper);
        assertEquals("client|setname", lower.fullName());
        assertEquals("client|setname", upper.fullName());
        assertSame(lower.metadata(), upper.metadata());
    }

    @Test
    void shouldReturnNullForUnknownName() {
        // Behavior: an unknown top-level name gives null
        assertNull(CommandLookup.find(commands(), "nope"));
    }

    @Test
    void shouldReturnNullForUnknownSubcommand() {
        // Behavior: a known container with an unknown subcommand gives null
        assertNull(CommandLookup.find(commands(), "client|nope"));
        assertNull(CommandLookup.find(commands(), "ping|x"));
    }

    @Test
    void shouldReturnNullForMoreThanTwoParts() {
        // Behavior: only one level of subcommands is supported
        assertNull(CommandLookup.find(commands(), "client|setname|x"));
    }
}
