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

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;

import java.util.List;

public class AppendMessage implements KronotopMessage<String> {
    public static final String COMMAND = "APPEND";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    public static final int MAXIMUM_PARAMETER_COUNT = 2;
    private final Request request;
    private String key;
    private byte[] value;

    public AppendMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        byte[] rawkey = new byte[request.getParams().get(0).readableBytes()];
        request.getParams().get(0).readBytes(rawkey);
        key = new String(rawkey);

        value = new byte[request.getParams().get(1).readableBytes()];
        request.getParams().get(1).readBytes(value);
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
}