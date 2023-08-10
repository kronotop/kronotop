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

package com.kronotop.redis.server.protocol;

import com.kronotop.server.resp.KronotopMessage;
import com.kronotop.server.resp.Request;
import io.netty.buffer.ByteBuf;

import java.util.List;

public class CommandMessage implements KronotopMessage<Void> {
    public static final String COMMAND = "COMMAND";
    public static final String COUNT_OPERAND = "COUNT";
    private final Request request;
    private String operand;

    public CommandMessage(Request request) {
        this.request = request;
        parse();
    }

    private String readFromByteBuf(ByteBuf buf) {
        byte[] rawItem = new byte[buf.readableBytes()];
        buf.readBytes(rawItem);
        return new String(rawItem);
    }

    private void parse() {
        if (request.getParams().size() == 0) {
            return;
        }

        byte[] rawOperand = new byte[request.getParams().get(0).readableBytes()];
        request.getParams().get(0).readBytes(rawOperand);
        operand = new String(rawOperand);

        if (operand.equalsIgnoreCase("DOCS")) {
            rawOperand = new byte[request.getParams().get(1).readableBytes()];
            request.getParams().get(1).readBytes(rawOperand);
            String two = new String(rawOperand);
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
