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

package com.kronotop.redis.handlers;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.protocol.InfoMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.Unpooled;

@Command(InfoMessage.COMMAND)
public class InfoHandler implements Handler {
    private final RedisService service;

    public InfoHandler(RedisService service) {
        this.service = service;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.INFO).set(new InfoMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        InfoResponse infoResponse = new InfoResponse();
        infoResponse.append("# Server");
        infoResponse.append(String.format("kronotop_version:%s", getClass().getPackage().getImplementationVersion()));
        infoResponse.append(String.format("redis_version:%s", RedisService.REDIS_VERSION));
        infoResponse.append("redis_mode:cluster");
        infoResponse.append(String.format("os:%s %s %s",
                System.getProperty("os.name"),
                System.getProperty("os.version"),
                System.getProperty("os.arch")
        ));

        infoResponse.append("# Cluster");
        infoResponse.append("cluster_enabled:1");
        FullBulkStringRedisMessage fb = new FullBulkStringRedisMessage(
                Unpooled.buffer().writeBytes(infoResponse.toString().getBytes())
        );
        response.writeFullBulkString(fb);
    }

    private static class InfoResponse {
        StringBuilder response = new StringBuilder();

        public void append(String str) {
            response.append(str);
            response.append("\r\n");
        }

        public String toString() {
            return response.toString();
        }
    }
}
