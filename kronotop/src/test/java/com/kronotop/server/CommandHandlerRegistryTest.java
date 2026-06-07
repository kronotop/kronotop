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

import java.util.List;
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
        registry.register(CommandType.PING, handler, 0, 1);

        HandlerEntry entry = registry.get(CommandType.PING);

        assertNotNull(entry);
        assertSame(handler, entry.handler());
        assertEquals(CommandType.PING, entry.commandType());
        assertEquals(0, entry.minimumParameterCount());
        assertEquals(1, entry.maximumParameterCount());
    }

    // Behavior: Verifies that a handler can be retrieved by command string.
    @Test
    void shouldRetrieveByCommandString() {
        Handler handler = new TestHandler();
        registry.register(CommandType.GET, handler, 1, 1);

        HandlerEntry entry = registry.get("GET");

        assertNotNull(entry);
        assertSame(handler, entry.handler());
    }

    // Behavior: Verifies that dot notation commands work correctly.
    @Test
    void shouldHandleDotNotationCommands() {
        Handler handler = new TestHandler();
        registry.register(CommandType.BUCKET_INSERT, handler, 2, -1);

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

        registry.register(CommandType.SET, handler1, 2, 2);

        assertThrows(CommandAlreadyRegisteredException.class,
                () -> registry.register(CommandType.SET, handler2, 2, 2));
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
        registry.register(CommandType.PING, new TestHandler(), 0, 1);
        registry.register(CommandType.GET, new TestHandler(), 1, 1);
        registry.register(CommandType.BUCKET_INSERT, new TestHandler(), 2, -1);

        Set<String> commands = registry.getCommands();

        assertEquals(3, commands.size());
        assertTrue(commands.contains("PING"));
        assertTrue(commands.contains("GET"));
        assertTrue(commands.contains("BUCKET.INSERT"));
    }

    // Behavior: Verifies HandlerEntry constraint helpers work correctly.
    @Test
    void shouldCorrectlyReportConstraintPresence() {
        registry.register(CommandType.PING, new TestHandler(), HandlerEntry.NO_CONSTRAINT, HandlerEntry.NO_CONSTRAINT);
        registry.register(CommandType.GET, new TestHandler(), 1, HandlerEntry.NO_CONSTRAINT);
        registry.register(CommandType.SET, new TestHandler(), HandlerEntry.NO_CONSTRAINT, 5);
        registry.register(CommandType.DEL, new TestHandler(), 1, 10);

        HandlerEntry pingEntry = registry.get(CommandType.PING);
        assertFalse(pingEntry.hasMinimumParameterCount());
        assertFalse(pingEntry.hasMaximumParameterCount());

        HandlerEntry getEntry = registry.get(CommandType.GET);
        assertTrue(getEntry.hasMinimumParameterCount());
        assertFalse(getEntry.hasMaximumParameterCount());

        HandlerEntry setEntry = registry.get(CommandType.SET);
        assertFalse(setEntry.hasMinimumParameterCount());
        assertTrue(setEntry.hasMaximumParameterCount());

        HandlerEntry delEntry = registry.get(CommandType.DEL);
        assertTrue(delEntry.hasMinimumParameterCount());
        assertTrue(delEntry.hasMaximumParameterCount());
    }

    private static class TestHandler implements Handler {
        @Override
        public void beforeExecute(Request request) {
        }

        @Override
        public void execute(Request request, Response response) {
        }

        @Override
        public boolean isWatchable() {
            return false;
        }

        @Override
        public List<String> getKeys(Request request) {
            return List.of();
        }

        @Override
        public boolean isRedisCompatible() {
            return false;
        }

        @Override
        public boolean requiresClusterInitialization() {
            return false;
        }
    }
}
