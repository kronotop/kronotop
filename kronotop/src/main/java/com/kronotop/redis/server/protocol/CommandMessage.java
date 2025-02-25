/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.redis.server.protocol;

import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.ArrayList;
import java.util.List;

public class CommandMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "COMMAND";
    public static final String SUBCOMMAND_COUNT = "COUNT";
    public static final String SUBCOMMAND_INFO = "INFO";
    public static final String SUBCOMMAND_DOCS = "DOCS";
    private final List<String> commands = new ArrayList<>();
    private final Request request;
    private String subcommand;
    private boolean hasSubcommand;

    public CommandMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parseCommands() {
        if (request.getParams().size() <= 1) {
            return;
        }
        for (int i = 1; i < request.getParams().size(); i++) {
            byte[] rawCommand = new byte[request.getParams().get(i).readableBytes()];
            request.getParams().get(i).readBytes(rawCommand);
            String command = new String(rawCommand);
            commands.add(command);
        }
    }

    private void parse() {
        if (request.getParams().isEmpty()) {
            return;
        }

        hasSubcommand = true;
        byte[] rawSubcommand = new byte[request.getParams().get(0).readableBytes()];
        request.getParams().get(0).readBytes(rawSubcommand);
        subcommand = new String(rawSubcommand).toUpperCase();
        if (subcommand.equals(SUBCOMMAND_INFO) || subcommand.equals(SUBCOMMAND_DOCS)) {
            parseCommands();
        } else if (subcommand.equals(SUBCOMMAND_COUNT)) {
        }
    }

    public boolean hasSubcommand() {
        return hasSubcommand;
    }

    public String getSubcommand() {
        return subcommand;
    }

    public List<String> getCommands() {
        return commands;
    }

    @Override
    public Void getKey() {
        return null;
    }

    @Override
    public List<Void> getKeys() {
        return null;
    }
}
