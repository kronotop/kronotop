/*
 * Copyright (c) 2023-2024 Kronotop
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

import com.kronotop.common.Preconditions;

import java.util.HashMap;
import java.util.Set;

/**
 * The Handlers class represents a collection of registered command handlers.
 * It allows registering handlers for specific commands, retrieving the registered handler for a command, and retrieving the set of registered commands.
 */
public class Handlers {
    private final HashMap<String, Handler> handlers;

    public Handlers() {
        handlers = new HashMap<>();
    }

    /**
     * Registers a command handler with a specified command.
     *
     * @param command the command to register
     * @param handler the handler for the command
     * @throws CommandAlreadyRegisteredException if the command is already registered
     */
    public void register(String command, Handler handler) throws CommandAlreadyRegisteredException {
        Preconditions.checkNotNull(handler, "handler cannot be null");
        if (handlers.containsKey(command)) {
            throw new CommandAlreadyRegisteredException(String.format("command already registered '%s'", command));
        }
        handlers.put(command, handler);
    }

    /**
     * Retrieves the registered handler for the given command.
     *
     * @param command the command for which to retrieve the handler
     * @return the registered handler
     * @throws CommandNotFoundException if the command is not registered
     */
    public Handler get(String command) throws CommandNotFoundException {
        Handler handler = handlers.get(command);
        if (handler == null) {
            throw new CommandNotFoundException(String.format("unknown command '%s'", command));
        }
        return handler;
    }

    /**
     * Retrieves the set of registered commands.
     *
     * @return the Set of registered commands
     */
    public Set<String> getCommands() {
        return handlers.keySet();
    }
}
