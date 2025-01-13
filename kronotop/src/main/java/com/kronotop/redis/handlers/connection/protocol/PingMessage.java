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

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;
import io.netty.util.CharsetUtil;

import java.util.List;

public class PingMessage implements KronotopMessage<Void> {
    public static final String COMMAND = "PING";
    public static final int MAXIMUM_PARAMETER_COUNT = 1;

    private final Request request;
    private String message;

    public PingMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        if (request.getParams().size() > 0) {
            message = request.getParams().get(0).toString(CharsetUtil.UTF_8);
        }
    }

    public String getMessage() {
        return message;
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
