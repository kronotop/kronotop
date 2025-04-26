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

package com.kronotop.server.handlers;

import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.handlers.protocol.SessionAttributeMessage;
import com.kronotop.server.handlers.protocol.SessionAttributeParameters;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.netty.util.Attribute;

import java.util.LinkedHashMap;
import java.util.Map;

@Command(SessionAttributeMessage.COMMAND)
@MaximumParameterCount(SessionAttributeMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(SessionAttributeMessage.MINIMUM_PARAMETER_COUNT)
public class SessionAttributeHandler implements Handler {
    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SESSIONATTRIBUTE).set(new SessionAttributeMessage());
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    private void listSubcommand(Request request, Response response) {
        Map<RedisMessage, RedisMessage> children = new LinkedHashMap<>();

        // List the attributes have a default value

        // REPLY_TYPE
        Attribute<ReplyType> replyTypeAttr = request.getSession().attr(SessionAttributes.REPLY_TYPE);
        children.put(
                new SimpleStringRedisMessage(SessionAttributeParameters.SessionAttribute.REPLY_TYPE.name().toLowerCase()),
                new SimpleStringRedisMessage(replyTypeAttr.get().name().toLowerCase())
        );

        // INPUT_TYPE
        Attribute<InputType> inputTypeAttr = request.getSession().attr(SessionAttributes.INPUT_TYPE);
        children.put(
                new SimpleStringRedisMessage(SessionAttributeParameters.SessionAttribute.INPUT_TYPE.name().toLowerCase()),
                new SimpleStringRedisMessage(inputTypeAttr.get().name().toLowerCase())
        );

        response.writeMap(children);
    }

    private void setSubcommand(Request request, Response response, SessionAttributeParameters parameters) {
        switch (parameters.getAttribute()) {
            case REPLY_TYPE ->
                    request.getSession().attr(SessionAttributes.REPLY_TYPE).set(parameters.replyType());
            case INPUT_TYPE ->
                    request.getSession().attr(SessionAttributes.INPUT_TYPE).set(parameters.inputType());
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
