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

package com.kronotop.session.handlers;

import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.BooleanRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.session.handlers.protocol.SessionAttributeMessage;
import com.kronotop.session.handlers.protocol.SessionAttributeParameters;
import io.netty.util.Attribute;

import java.util.HashMap;
import java.util.Map;

@Command(SessionAttributeMessage.COMMAND)
@MaximumParameterCount(SessionAttributeMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(SessionAttributeMessage.MINIMUM_PARAMETER_COUNT)
public class SessionAttributeHandler implements Handler {
    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SESSIONATTRIBUTE).set(new SessionAttributeMessage());
    }

    private void listSubcommand(Request request, Response response) {
        Map<RedisMessage, RedisMessage> children = new HashMap<>();

        // FUTURES
        Attribute<Boolean> futuresAttr = request.getChannelContext().channel().attr(ChannelAttributes.FUTURES);
        children.put(
                new SimpleStringRedisMessage(SessionAttributeParameters.SessionAttribute.FUTURES.name().toLowerCase()),
                futuresAttr.get() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE
        );

        response.writeMap(children);
    }

    private void setSubcommand(Request request, Response response, SessionAttributeParameters parameters) {
        switch (parameters.getAttribute()) {
            case FUTURES -> {
                request.getChannelContext().channel().attr(ChannelAttributes.FUTURES).set(parameters.getFutures());
            }
        }
        response.writeOK();
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        SessionAttributeParameters parameters = new SessionAttributeParameters(request.getParams());
        if (parameters.getSubcommand().equals(SessionAttributeParameters.SessionAttributeSubcommand.SET)) {
            setSubcommand(request, response, parameters);
        } else if (parameters.getSubcommand().equals(SessionAttributeParameters.SessionAttributeSubcommand.LIST)) {
            listSubcommand(request, response);
        }
    }
}
