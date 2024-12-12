/*
 * Copyright (c) 2023-2024 Kronotop
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

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;

import java.util.List;

public class TypeMessage implements KronotopMessage<String> {
    public static final String COMMAND = "TYPE";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    private final Request request;
    private String key;

    public TypeMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        byte[] tmp = new byte[request.getParams().getFirst().readableBytes()];
        request.getParams().getFirst().readBytes(tmp);
        key = new String(tmp);
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public List<String> getKeys() {
        return null;
    }


}