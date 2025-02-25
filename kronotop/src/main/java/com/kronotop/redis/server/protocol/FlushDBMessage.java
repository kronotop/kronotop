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

import java.util.List;

public class FlushDBMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "FLUSHDB";
    public static final int MINIMUM_PARAMETER_COUNT = 0;
    public static final int MAXIMUM_PARAMETER_COUNT = 1;
    public static final String ASYNC_OPERAND = "ASYNC";
    public static final String SYNC_OPERAND = "SYNC";
    private final Request request;

    public FlushDBMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        if (request.getParams().isEmpty()) {
            return;
        }

        byte[] rawOperand = new byte[request.getParams().get(0).readableBytes()];
        request.getParams().get(0).readBytes(rawOperand);
        String operand = new String(rawOperand);

        if (!(operand.equalsIgnoreCase(SYNC_OPERAND) || operand.equalsIgnoreCase(ASYNC_OPERAND))) {
            throw new IllegalArgumentException("syntax error");
        }
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
