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

package com.kronotop.redis.transactions;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.transactions.protocol.UnwatchMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

@Command(UnwatchMessage.COMMAND)
@MaximumParameterCount(UnwatchMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(UnwatchMessage.MINIMUM_PARAMETER_COUNT)
public class UnwatchHandler implements Handler {
    private final RedisService service;

    public UnwatchHandler(RedisService service) {
        this.service = service;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.UNWATCH).set(new UnwatchMessage());
    }

    @Override
    public void execute(Request request, Response response) {
        service.getWatcher().cleanupChannelHandlerContext(request.getChannelContext());
        response.writeOK();
    }
}

