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
package com.kronotop.core.handlers.server.protocol;

import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.StringUtil;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.ArrayList;
import java.util.List;

public class CommandMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "COMMAND";
    private final Request request;
    private CommandArgumentKey argument;
    private final List<String> commands = new ArrayList<>();

    public CommandMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        if (request.getParams().isEmpty()) {
            return;
        }
        String raw = ProtocolMessageUtil.readAsString(request.getParams().getFirst());
        argument = valueOfArgument(raw);
        int argc = request.getParams().size() + 1;
        if ((argument.arity > 0 && argument.arity != argc) || argc < -argument.arity) {
            throw new IllegalCommandArgumentException(
                    String.format("wrong number of arguments for 'command|%s' command", argument.name().toLowerCase()));
        }
        for (int i = 1; i < request.getParams().size(); i++) {
            commands.add(ProtocolMessageUtil.readAsString(request.getParams().get(i)));
        }
    }

    public boolean hasArgument() {
        return getArgument() != null;
    }

    public CommandArgumentKey getArgument() {
        return argument;
    }

    /**
     * Arguments given after the subcommand, as sent by the client.
     */
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

    private CommandArgumentKey valueOfArgument(String raw) {
        String upper = StringUtil.toUpperCaseAscii(raw);
        for (CommandArgumentKey key : CommandArgumentKey.values()) {
            if (key.name().equals(upper)) {
                return key;
            }
        }
        throw new IllegalCommandArgumentException(String.format("unknown subcommand '%s'. Try COMMAND HELP.", raw));
    }

    /**
     * Subcommands with their arity. The arity counts COMMAND itself, negative means "at least".
     */
    public enum CommandArgumentKey {
        COUNT(2),
        DOCS(-2),
        GETKEYS(-3),
        GETKEYSANDFLAGS(-3),
        HELP(2),
        INFO(-2),
        LIST(-2);

        private final int arity;

        CommandArgumentKey(int arity) {
            this.arity = arity;
        }
    }
}
