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

import com.kronotop.redis.BaseHandler;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.connection.protocol.HelloMessage;
import com.kronotop.server.resp.Handler;
import com.kronotop.server.resp.MessageTypes;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.Response;
import com.kronotop.server.resp.annotation.Command;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;

import java.util.ArrayList;
import java.util.List;

// TODO: This implementation is incomplete & inaccurate

@Command(HelloMessage.COMMAND)
public class HelloHandler extends BaseHandler implements Handler {
    public HelloHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.HELLO).set(new HelloMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        List<RedisMessage> result = new ArrayList<>();
        result.add(new SimpleStringRedisMessage("server"));
        result.add(new SimpleStringRedisMessage("kronotop"));
        result.add(new SimpleStringRedisMessage("version"));
        result.add(new SimpleStringRedisMessage(getClass().getPackage().getImplementationVersion()));
        result.add(new SimpleStringRedisMessage("proto"));
        result.add(new IntegerRedisMessage(2));
        result.add(new SimpleStringRedisMessage("id"));
        result.add(new IntegerRedisMessage(3));
        result.add(new SimpleStringRedisMessage("mode"));
        result.add(new SimpleStringRedisMessage("standalone"));
        result.add(new SimpleStringRedisMessage("role"));
        result.add(new SimpleStringRedisMessage("master"));
        result.add(new SimpleStringRedisMessage("modules"));
        result.add(new ArrayRedisMessage(new ArrayList<>()));
        response.writeArray(result);
    }
}
