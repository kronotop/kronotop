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

package com.kronotop.redis.connection;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.connection.protocol.SelectMessage;
import com.kronotop.server.resp.*;
import com.kronotop.server.resp.annotation.Command;
import com.kronotop.server.resp.annotation.MaximumParameterCount;
import com.kronotop.server.resp.annotation.MinimumParameterCount;
import io.netty.channel.Channel;
import io.netty.util.Attribute;

@Command(SelectMessage.COMMAND)
@MaximumParameterCount(SelectMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(SelectMessage.MINIMUM_PARAMETER_COUNT)
public class SelectHandler implements Handler {
    private final RedisService service;

    public SelectHandler(RedisService service) {
        this.service = service;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SELECT).set(new SelectMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        SelectMessage selectMessage = request.attr(MessageTypes.SELECT).get();
        service.setLogicalDatabase(selectMessage.getIndex());

        Channel channel = response.getContext().channel();
        Attribute<String> redisLogicalDatabase = channel.attr(ChannelAttributes.REDIS_LOGICAL_DATABASE_INDEX);
        redisLogicalDatabase.set(selectMessage.getIndex());
        response.writeOK();
    }
}
