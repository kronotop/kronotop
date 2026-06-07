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

package com.kronotop;

import com.kronotop.commands.CommandMetadata;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.Commands;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

/**
 * Base class that handles command handler registration and command definitions from JSON files.
 */
public class CommandHandlerService extends BaseKronotopService {

    public CommandHandlerService(Context context, final String name) {
        super(context, name);
    }

    /**
     * Extracts minimum parameter count from handler annotation.
     *
     * @param handler the handler to inspect
     * @return minimum parameter count or -1 if no constraint
     */
    private int extractMinimumParameterCount(Handler handler) {
        MinimumParameterCount annotation = handler.getClass().getAnnotation(MinimumParameterCount.class);
        return annotation != null ? annotation.value() : HandlerEntry.NO_CONSTRAINT;
    }

    /**
     * Extracts maximum parameter count from handler annotation.
     *
     * @param handler the handler to inspect
     * @return maximum parameter count or -1 if no constraint
     */
    private int extractMaximumParameterCount(Handler handler) {
        MaximumParameterCount annotation = handler.getClass().getAnnotation(MaximumParameterCount.class);
        return annotation != null ? annotation.value() : HandlerEntry.NO_CONSTRAINT;
    }

    /**
     * Registers a handler for a command with the given command name.
     *
     * @param registry    the registry to register with
     * @param commandName the command name string
     * @param handler     the handler instance
     */
    private void registerHandler(CommandHandlerRegistry registry, String commandName, Handler handler) {
        String upperCommand = commandName.toUpperCase();
        CommandType commandType = CommandType.parse(upperCommand);
        if (commandType == null) {
            throw new IllegalArgumentException(
                    String.format("Unknown command type for '%s'. Add it to CommandType enum.", upperCommand)
            );
        }

        int minParams = extractMinimumParameterCount(handler);
        int maxParams = extractMaximumParameterCount(handler);

        registry.register(commandType, handler, minParams, maxParams);
        loadDefinition(upperCommand);
    }

    /**
     * Registers the given command handlers.
     *
     * @param handlers the handlers to register
     * @throws CommandAlreadyRegisteredException if a command is already registered
     */
    protected void handlerMethod(ServerKind kind, Handler... handlers) throws CommandAlreadyRegisteredException {
        CommandHandlerRegistry registry = context.getHandlers(kind);
        for (Handler handler : handlers) {
            Commands commands = handler.getClass().getAnnotation(Commands.class);
            if (commands != null) {
                for (Command command : commands.value()) {
                    registerHandler(registry, command.value(), handler);
                }
            } else {
                Command command = handler.getClass().getAnnotation(Command.class);
                registerHandler(registry, command.value(), handler);
            }
        }
    }

    /**
     * Loads the definition of a command from a JSON file and registers it in the context.
     *
     * @param command the name of the command
     */
    private void loadDefinition(String command) {
        ClassLoader classLoader = getClass().getClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream(String.format("commands/%s.json", command.toLowerCase()))) {
            if (inputStream == null) {
                return;
            }
            byte[] jsonData = inputStream.readAllBytes();
            ObjectMapper objectMapper = new ObjectMapper();
            HashMap<String, CommandMetadata> data = objectMapper.readValue(jsonData, new TypeReference<>() {
            });
            for (String cmd : data.keySet()) {
                context.registerCommandMetadata(cmd.toUpperCase(), data.get(cmd));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
