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

package com.kronotop.redis.handlers.generic.protocol;

import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class DelMessage implements ProtocolMessage<String> {
    public static final String COMMAND = "DEL";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    private final Request request;
    private final List<String> keys = new ArrayList<>();

    public DelMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        for (ByteBuf buf : request.getParams()) {
            keys.add(ProtocolMessageUtil.readAsString(buf));
        }
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public List<String> getKeys() {
        return keys;
    }
}