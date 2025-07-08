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

package com.kronotop.redis.handlers.connection.protocol;

import com.kronotop.KronotopException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.*;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class HelloMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "HELLO";
    private final Request request;
    private Integer protover;
    private String username;
    private String password;
    private boolean auth;
    private boolean setName;
    private String clientName;

    public HelloMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        if (request.getParams().isEmpty()) {
            protover = RESPVersion.RESP3.getValue();
            return;
        }

        String rawProtover = request.getParams().getFirst().toString(StandardCharsets.US_ASCII);
        try {
            protover = Integer.parseInt(rawProtover);
        } catch (NumberFormatException e) {
            throw new KronotopException(RESPError.PROTOCOL_VERSION_FORMAT_ERROR);
        }

        if (protover != RESPVersion.RESP2.getValue() && protover != RESPVersion.RESP3.getValue()) {
            throw new NoProtoException();
        }

        for (int i = 1; i < request.getParams().size(); i++) {
            ByteBuf buf = request.getParams().get(i);
            String parameter = ProtocolMessageUtil.readAsString(buf);
            if (parameter.equalsIgnoreCase("AUTH")) {
                // HELLO $protover AUTH $username $password
                if (request.getParams().size() - i < 2) {
                    throw new KronotopException(String.format("Syntax error in %s option '%s'", COMMAND, parameter));
                }
                username = request.getParams().get(i + 1).toString(StandardCharsets.US_ASCII);
                password = request.getParams().get(i + 2).toString(StandardCharsets.US_ASCII);
                auth = true;
                i = i + 2;
            }

            if (parameter.equalsIgnoreCase("SETNAME")) {
                // HELLO $protover AUTH $username $password SETNAME $client-name
                // HELLO $protover SETNAME $client-name
                if (request.getParams().size() - i < 1) {
                    throw new KronotopException(String.format("Syntax error in %s option '%s'", COMMAND, parameter));
                }
                clientName = request.getParams().get(i + 1).toString(StandardCharsets.US_ASCII);
                setName = true;
                i++;
            }
        }
    }

    public Integer getProtover() {
        return protover;
    }

    @Override
    public Void getKey() {
        return null;
    }

    @Override
    public List<Void> getKeys() {
        return null;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean hasAuth() {
        return auth;
    }

    public boolean hasSetName() {
        return setName;
    }

    public String getClientName() {
        return clientName;
    }
}
