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

import com.kronotop.internal.Preconditions;

import java.util.HashSet;
import java.util.Set;

/**
 * Registry for command handlers using array-based lookup for O(1) dispatch.
 * Eliminates HashMap overhead from the hot path by indexing handlers via CommandType ordinals.
 */
public class CommandHandlerRegistry {
    private final HandlerEntry[] handlers;

    public CommandHandlerRegistry() {
        this.handlers = new HandlerEntry[CommandType.values().length];
    }

    /**
     * Registers a handler for the given command type with parameter constraints.
     *
     * @param commandType           the command type
     * @param handler               the handler instance
     * @param minimumParameterCount minimum required parameters (-1 = no constraint)
     * @param maximumParameterCount maximum allowed parameters (-1 = no constraint)
     * @throws CommandAlreadyRegisteredException if the command is already registered
     */
    public void register(CommandType commandType, Handler handler, int minimumParameterCount, int maximumParameterCount)
            throws CommandAlreadyRegisteredException {
        Preconditions.checkNotNull(handler, "handler cannot be null");
        Preconditions.checkNotNull(commandType, "commandType cannot be null");

        if (handlers[commandType.ordinal()] != null) {
            throw new CommandAlreadyRegisteredException(
                    String.format("command already registered '%s'", commandType.getCommandName())
            );
        }

        handlers[commandType.ordinal()] = new HandlerEntry(
                handler,
                commandType,
                minimumParameterCount,
                maximumParameterCount
        );
    }

    /**
     * Retrieves the handler entry for the given command type.
     *
     * @param commandType the command type
     * @return the handler entry
     * @throws CommandNotFoundException if no handler is registered for the command
     */
    public HandlerEntry get(CommandType commandType) throws CommandNotFoundException {
        if (commandType == null) {
            throw new CommandNotFoundException("unknown command 'null'");
        }
        HandlerEntry entry = handlers[commandType.ordinal()];
        if (entry == null) {
            throw new CommandNotFoundException(
                    String.format("unknown command '%s'", commandType.getCommandName())
            );
        }
        return entry;
    }

    /**
     * Retrieves the handler entry for the given command string.
     * Uses CommandType.parse() for JIT-optimized string-to-enum conversion.
     *
     * @param command the command string
     * @return the handler entry
     * @throws CommandNotFoundException if no handler is registered for the command
     */
    public HandlerEntry get(String command) throws CommandNotFoundException {
        CommandType commandType = CommandType.parse(command);
        if (commandType == null) {
            throw new CommandNotFoundException(String.format("unknown command '%s'", command));
        }
        return get(commandType);
    }

    /**
     * Retrieves the set of registered command names.
     *
     * @return set of registered command names
     */
    public Set<String> getCommands() {
        Set<String> commands = new HashSet<>();
        for (HandlerEntry entry : handlers) {
            if (entry != null) {
                commands.add(entry.commandType().getCommandName());
            }
        }
        return commands;
    }
}
