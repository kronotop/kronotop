/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.core.handlers.session;

import com.kronotop.KronotopException;
import com.kronotop.core.handlers.session.protocol.SessionAttributeMessage;
import com.kronotop.core.handlers.session.protocol.SessionAttributeParameters;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.util.Attribute;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.server.RESPUtil.bulkString;
import static com.kronotop.server.RESPUtil.wrapBytes;

@Command(SessionAttributeMessage.COMMAND)
@MaximumParameterCount(SessionAttributeMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(SessionAttributeMessage.MINIMUM_PARAMETER_COUNT)
public class SessionAttributeHandler implements Handler {
    private static final byte[] REPLY_TYPE_BYTES = "reply_type".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INPUT_TYPE_BYTES = "input_type".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LIMIT_BYTES = "limit".getBytes(StandardCharsets.UTF_8);
    private static final byte[] OBJECT_ID_FORMAT_BYTES = "object_id_format".getBytes(StandardCharsets.UTF_8);

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SESSIONATTRIBUTE).set(new SessionAttributeMessage());
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    @Override
    public boolean requiresClusterInitialization() {
        return false;
    }

    private void listSubcommand(Request request, Response response) {
        Map<RedisMessage, RedisMessage> children = new LinkedHashMap<>();

        // List the attributes have a default value

        // REPLY_TYPE
        Attribute<ReplyType> replyTypeAttr = request.getSession().attr(SessionAttributes.REPLY_TYPE);
        children.put(
                wrapBytes(REPLY_TYPE_BYTES),
                bulkString(replyTypeAttr.get().name().toLowerCase())
        );

        // INPUT_TYPE
        Attribute<InputType> inputTypeAttr = request.getSession().attr(SessionAttributes.INPUT_TYPE);
        children.put(
                wrapBytes(INPUT_TYPE_BYTES),
                bulkString(inputTypeAttr.get().name().toLowerCase())
        );

        // LIMIT
        Attribute<Integer> bucketBatchSizeAttr = request.getSession().attr(SessionAttributes.LIMIT);
        children.put(
                wrapBytes(LIMIT_BYTES),
                new IntegerRedisMessage(bucketBatchSizeAttr.get())
        );

        // OBJECT_ID_FORMAT
        Attribute<ObjectIdFormat> versionstampFormatAttr = request.getSession().attr(SessionAttributes.OBJECT_ID_FORMAT);
        children.put(
                wrapBytes(OBJECT_ID_FORMAT_BYTES),
                bulkString(versionstampFormatAttr.get().name().toLowerCase())
        );

        RESPVersion protoVer = request.getSession().protocolVersion();
        if (protoVer.equals(RESPVersion.RESP3)) {
            response.writeMap(children);
        } else if (protoVer.equals(RESPVersion.RESP2)) {
            List<RedisMessage> list = new ArrayList<>();
            for (Map.Entry<RedisMessage, RedisMessage> entry : children.entrySet()) {
                list.add(entry.getKey());
                list.add(entry.getValue());
            }
            response.writeArray(list);
        } else {
            throw new KronotopException("Unknown protocol version " + protoVer.getValue());
        }
    }

    private void setSubcommand(Request request, Response response, SessionAttributeParameters parameters) {
        switch (parameters.getAttribute()) {
            case REPLY_TYPE -> request.getSession().attr(SessionAttributes.REPLY_TYPE).set(parameters.replyType());
            case INPUT_TYPE -> request.getSession().attr(SessionAttributes.INPUT_TYPE).set(parameters.inputType());
            case LIMIT -> {
                int bucketBatchSize = parameters.bucketBatchSize();
                if (bucketBatchSize < 1) {
                    throw new KronotopException("'limit' must be greater than 0");
                }
                request.getSession().attr(SessionAttributes.LIMIT).set(bucketBatchSize);
            }
            case OBJECT_ID_FORMAT ->
                    request.getSession().attr(SessionAttributes.OBJECT_ID_FORMAT).set(parameters.objectIdFormat());
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
