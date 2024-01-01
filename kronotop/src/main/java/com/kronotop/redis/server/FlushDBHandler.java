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

package com.kronotop.redis.server;

import com.kronotop.redis.BaseHandler;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.server.protocol.FlushDBMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

@Command(FlushDBMessage.COMMAND)
@MaximumParameterCount(FlushDBMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(FlushDBMessage.MINIMUM_PARAMETER_COUNT)
public class FlushDBHandler extends BaseHandler implements Handler {
    public FlushDBHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.FLUSHDB).set(new FlushDBMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        service.clearLogicalDatabase();
        response.writeOK();
    }
}
