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

package com.kronotop.redis.handlers.generic;

import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.generic.protocol.ReadonlyMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;

@Command(ReadonlyMessage.COMMAND)
public class ReadonlyHandler extends BaseGenericHandler implements Handler {

    public ReadonlyHandler(RedisService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.READONLY).set(new ReadonlyMessage());
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        request.getChannelContext().channel().attr(ChannelAttributes.READONLY).set(true);
        response.writeOK();
    }
}
