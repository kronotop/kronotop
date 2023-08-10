/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.core.commands;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kronotop.core.Context;
import com.kronotop.core.KronotopService;
import com.kronotop.server.resp.Handlers;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

public class CommandDefinitions implements KronotopService {
    public static final String NAME = "CommandDefinitions";
    private final HashMap<String, CommandMetadata> definitions = new HashMap<>();


    public CommandDefinitions(Handlers commands) {
        for (String route : commands.getCommands()) {
            loadDefinition(route);
        }
    }

    private void loadDefinition(String cmd) {
        ClassLoader classLoader = getClass().getClassLoader();
        try (InputStream st = classLoader.getResourceAsStream(String.format("commands/%s.json", cmd.toLowerCase()))) {
            if (st == null) {
                return;
            }
            byte[] jsonData = st.readAllBytes();
            ObjectMapper objectMapper = new ObjectMapper();
            HashMap<String, CommandMetadata> data = objectMapper.readValue(jsonData, new TypeReference<>() {
            });
            for (String key : data.keySet()) {
                definitions.put(key, data.get(key));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HashMap<String, CommandMetadata> getDefinitions() {
        return definitions;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Context getContext() {
        return null;
    }

    @Override
    public void shutdown() {
    }
}
