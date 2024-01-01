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

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;
import io.netty.util.CharsetUtil;

import java.util.List;

public class AuthMessage implements KronotopMessage<Void> {
    public static final String COMMAND = "AUTH";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    public static final int MAXIMUM_PARAMETER_COUNT = 2;

    private final Request request;
    private String username;
    private String password;

    public AuthMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        if (request.getParams().size() == 1) {
            password = request.getParams().get(0).toString(CharsetUtil.UTF_8);
            return;
        }
        username = request.getParams().get(0).toString(CharsetUtil.UTF_8);
        password = request.getParams().get(1).toString(CharsetUtil.UTF_8);
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
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
