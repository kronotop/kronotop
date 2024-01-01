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

package com.kronotop.redis.string.protocol;

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;

import java.util.ArrayList;
import java.util.List;

public class MGetMessage implements KronotopMessage<String> {
    public static final String COMMAND = "MGET";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    private final Request request;
    private final List<String> keys = new ArrayList<>();

    public MGetMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        for (int i = 0; i < request.getParams().size(); i++) {
            byte[] key = new byte[request.getParams().get(i).readableBytes()];
            request.getParams().get(i).readBytes(key);
            keys.add(new String(key));
        }
    }

    @Override
    public List<String> getKeys() {
        return keys;
    }


    @Override
    public String getKey() {
        return null;
    }
}