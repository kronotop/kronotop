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

package com.kronotop.redis.handlers.string.protocol;

import com.kronotop.KronotopException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public class SetEXMessage implements ProtocolMessage<String> {
    public static final String COMMAND = "SETEX";
    public static final int MINIMUM_PARAMETER_COUNT = 3;
    public static final int MAXIMUM_PARAMETER_COUNT = 3;
    private final Request request;
    private String key;
    private long seconds;
    private byte[] value;

    public SetEXMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        key = ProtocolMessageUtil.readAsString(request.getParams().getFirst());
        seconds = ProtocolMessageUtil.readAsLong(request.getParams().get(1));
        if (seconds <= 0) {
            throw new KronotopException("invalid expire time in 'setex' command");
        }
        value = new byte[request.getParams().get(2).readableBytes()];
        request.getParams().get(2).readBytes(value);
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public List<String> getKeys() {
        return null;
    }

    public byte[] getValue() {
        return value;
    }

    public long getSeconds() {
        return seconds;
    }
}
