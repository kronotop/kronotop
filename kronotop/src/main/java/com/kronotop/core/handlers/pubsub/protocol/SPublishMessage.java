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

package com.kronotop.core.handlers.pubsub.protocol;

import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import java.util.List;

public class SPublishMessage implements ProtocolMessage<String> {
    public static final String COMMAND = "SPUBLISH";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    public static final int MAXIMUM_PARAMETER_COUNT = 2;

    private final Request request;
    private String channel;
    private byte[] message;

    public SPublishMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        channel = request.getParams().get(0).toString(CharsetUtil.UTF_8);
        ByteBuf buf = request.getParams().get(1);
        message = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), message);
    }

    public byte[] getMessage() {
        return message;
    }

    @Override
    public String getKey() {
        return channel;
    }

    @Override
    public List<String> getKeys() {
        return List.of(channel);
    }
}
