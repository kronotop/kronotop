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

package com.kronotop.stash.handlers.generic.protocol;

import com.kronotop.KronotopException;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.RESPError;
import com.kronotop.server.Request;

import java.util.List;

public class ScanMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "SCAN";
    public static final int MINIMUM_ARGUMENT_COUNT = 1;
    private final Request request;
    private long cursor;
    private int count = 10;
    private String match;
    private String type;

    public ScanMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        byte[] rawCursor = new byte[request.getArguments().get(0).readableBytes()];
        request.getArguments().get(0).readBytes(rawCursor);
        cursor = Long.parseLong(new String(rawCursor));

        if (request.getArguments().size() > 1) {
            for (int i = 1; i < request.getArguments().size(); i = i + 2) {
                byte[] rawArgument = new byte[request.getArguments().get(i).readableBytes()];
                request.getArguments().get(i).readBytes(rawArgument);
                String argument = new String(rawArgument);

                byte[] rawValue = new byte[request.getArguments().get(i + 1).readableBytes()];
                request.getArguments().get(i + 1).readBytes(rawValue);
                String value = new String(rawValue);

                if (argument.equalsIgnoreCase("COUNT")) {
                    try {
                        count = Integer.parseInt(value);
                    } catch (NumberFormatException e) {
                        throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_INTEGER);
                    }
                } else if (argument.equalsIgnoreCase("MATCH")) {
                    match = value;
                } else if (argument.equalsIgnoreCase("TYPE")) {
                    type = value;
                } else {
                    throw new KronotopException("syntax error");
                }
            }
        }
    }

    public String getType() {
        return type;
    }

    public String getMatch() {
        return match;
    }

    public int getCount() {
        return count;
    }

    public Long getCursor() {
        return cursor;
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