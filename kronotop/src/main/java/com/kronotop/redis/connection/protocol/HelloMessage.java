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

package com.kronotop.redis.connection.protocol;

import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.server.resp.KronotopMessage;
import com.kronotop.server.resp.Request;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class HelloMessage implements KronotopMessage<Void> {
    public static final String COMMAND = "HELLO";
    public static final int DEFAULT_PROTOVER = 2;
    private final Request request;
    private int protover;

    public HelloMessage(Request request) {
        this.request = request;
        parse();

    }

    private void parse() {
        if (request.getParams().size() == 0) {
            protover = DEFAULT_PROTOVER;
            return;
        }
        String rawProtover = request.getParams().get(0).toString(StandardCharsets.US_ASCII);
        try {
            protover = Integer.parseInt(rawProtover);
        } catch (NumberFormatException e) {
            throw new KronotopException(RESPError.PROTOCOL_VERSION_FORMAT_ERROR, e);
        }
    }

    public int getProtover() {
        return protover;
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
