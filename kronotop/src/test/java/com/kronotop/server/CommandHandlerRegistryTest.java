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

package com.kronotop.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class CommandHandlerRegistryTest {

    private CommandHandlerRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new CommandHandlerRegistry();
    }

    // Behavior: Verifies that a handler can be registered and retrieved by CommandType.
    @Test
    void shouldRegisterAndRetrieveByCommandType() {
        Handler handler = new TestHandler();
        registry.register(CommandType.PING, handler, 0, 1, "Test");

        HandlerEntry entry = registry.get(CommandType.PING);

        assertNotNull(entry);
        assertSame(handler, entry.handler());
        assertEquals(CommandType.PING, entry.commandType());
        assertEquals(0, entry.minimumArgumentCount());
        assertEquals(1, entry.maximumArgumentCount());
    }

    // Behavior: Verifies that a handler can be retrieved by command string.
    @Test
    void shouldRetrieveByCommandString() {
        Handler handler = new TestHandler();
        registry.register(CommandType.ZGET, handler, 1, 1, "Test");

        HandlerEntry entry = registry.get("ZGET");

        assertNotNull(entry);
        assertSame(handler, entry.handler());
    }

    // Behavior: Verifies that dot notation commands work correctly.
    @Test
    void shouldHandleDotNotationCommands() {
        Handler handler = new TestHandler();
        registry.register(CommandType.BUCKET_INSERT, handler, 2, -1, "Test");

        HandlerEntry entry = registry.get("BUCKET.INSERT");

        assertNotNull(entry);
        assertSame(handler, entry.handler());
        assertEquals("BUCKET.INSERT", entry.commandType().getCommandName());
    }

    // Behavior: Verifies that registering a duplicate command throws an exception.
    @Test
    void shouldThrowOnDuplicateRegistration() {
        Handler handler1 = new TestHandler();
        Handler handler2 = new TestHandler();

        registry.register(CommandType.ZSET, handler1, 2, 2, "Test");

        assertThrows(CommandAlreadyRegisteredException.class,
                () -> registry.register(CommandType.ZSET, handler2, 2, 2, "Test"));
    }

    // Behavior: Verifies that getting an unknown command throws CommandNotFoundException.
    @Test
    void shouldThrowOnUnknownCommand() {
        assertThrows(CommandNotFoundException.class, () -> registry.get("UNKNOWN"));
    }

    // Behavior: Verifies that getting a null CommandType throws CommandNotFoundException.
    @Test
    void shouldThrowOnNullCommandType() {
        assertThrows(CommandNotFoundException.class, () -> registry.get((CommandType) null));
    }

    // Behavior: Verifies that getCommands returns all registered command names.
    @Test
    void shouldReturnAllRegisteredCommands() {
        registry.register(CommandType.PING, new TestHandler(), 0, 1, "Test");
        registry.register(CommandType.ZGET, new TestHandler(), 1, 1, "Test");
        registry.register(CommandType.BUCKET_INSERT, new TestHandler(), 2, -1, "Test");

        Set<String> commands = registry.getCommands();

        assertEquals(3, commands.size());
        assertTrue(commands.contains("PING"));
        assertTrue(commands.contains("ZGET"));
        assertTrue(commands.contains("BUCKET.INSERT"));
    }

    // Behavior: Verifies HandlerEntry constraint helpers work correctly.
    @Test
    void shouldCorrectlyReportConstraintPresence() {
        registry.register(CommandType.PING, new TestHandler(), HandlerEntry.NO_CONSTRAINT, HandlerEntry.NO_CONSTRAINT, "Test");
        registry.register(CommandType.ZGET, new TestHandler(), 1, HandlerEntry.NO_CONSTRAINT, "Test");
        registry.register(CommandType.ZSET, new TestHandler(), HandlerEntry.NO_CONSTRAINT, 5, "Test");
        registry.register(CommandType.ZDEL, new TestHandler(), 1, 10, "Test");

        HandlerEntry pingEntry = registry.get(CommandType.PING);
        assertFalse(pingEntry.hasMinimumArgumentCount());
        assertFalse(pingEntry.hasMaximumArgumentCount());

        HandlerEntry getEntry = registry.get(CommandType.ZGET);
        assertTrue(getEntry.hasMinimumArgumentCount());
        assertFalse(getEntry.hasMaximumArgumentCount());

        HandlerEntry setEntry = registry.get(CommandType.ZSET);
        assertFalse(setEntry.hasMinimumArgumentCount());
        assertTrue(setEntry.hasMaximumArgumentCount());

        HandlerEntry delEntry = registry.get(CommandType.ZDEL);
        assertTrue(delEntry.hasMinimumArgumentCount());
        assertTrue(delEntry.hasMaximumArgumentCount());
    }

    private static class TestHandler implements Handler {
        @Override
        public void beforeExecute(Request request) {
        }

        @Override
        public void execute(Request request, Response response) {
        }

        @Override
        public boolean requiresClusterInitialization() {
            return false;
        }
    }
}
