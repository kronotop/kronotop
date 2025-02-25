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

package com.kronotop.cluster.handlers.protocol;

import com.kronotop.cluster.handlers.KrAdminSubcommand;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import com.kronotop.server.UnknownSubcommandException;

import java.util.List;

public class KrAdminMessage implements ProtocolMessage<String> {
    public static final String COMMAND = "KR.ADMIN";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    private final Request request;
    private KrAdminSubcommand subcommand;

    public KrAdminMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        byte[] rawSubcommand = new byte[request.getParams().getFirst().readableBytes()];
        request.getParams().getFirst().readBytes(rawSubcommand);
        String cmd = new String(rawSubcommand);
        try {
            subcommand = KrAdminSubcommand.valueOfSubcommand(cmd.toLowerCase());
        } catch (IllegalArgumentException e) {
            throw new UnknownSubcommandException(cmd);
        }
    }

    public KrAdminSubcommand getSubcommand() {
        return subcommand;
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public List<String> getKeys() {
        return null;
    }
}