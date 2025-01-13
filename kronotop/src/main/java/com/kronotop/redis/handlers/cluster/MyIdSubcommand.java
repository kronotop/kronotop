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

package com.kronotop.redis.handlers.cluster;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;

class MyIdSubcommand implements SubcommandHandler {
    private final RedisService service;

    MyIdSubcommand(RedisService service) {
        this.service = service;
    }

    @Override
    public void execute(Request request, Response response) {
        ByteBuf buf = response.getChannelContext().alloc().buffer();
        String id = service.getContext().getMember().getId();
        buf.writeBytes(id.getBytes());
        response.writeFullBulkString(new FullBulkStringRedisMessage(buf));
    }
}
