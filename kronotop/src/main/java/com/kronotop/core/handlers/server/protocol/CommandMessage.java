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

import java.util.List;

public class CommandMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "COMMAND";
    public static final int MAXIMUM_PARAMETER_COUNT = 2;
    private final Request request;
    private CommandArgumentKey argument;

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
    }

    public boolean hasArgument() {
        return getArgument() != null;
    }

    public CommandArgumentKey getArgument() {
        return argument;
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

    public enum CommandArgumentKey {
        COUNT,
        DOCS,
        GETKEYS,
        GETKEYSANDFLAGS,
        INFO,
        LIST
    }
}
