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

package com.kronotop.stash.handlers.transactions;

import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.stash.StashService;
import com.kronotop.stash.handlers.transactions.protocol.DiscardMessage;
import io.netty.util.Attribute;

@Command(DiscardMessage.COMMAND)
@MaximumParameterCount(DiscardMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(DiscardMessage.MINIMUM_PARAMETER_COUNT)
public class DiscardHandler implements Handler {
    private final StashService service;

    public DiscardHandler(StashService service) {
        this.service = service;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.DISCARD).set(new DiscardMessage());
    }

    @Override
    public void execute(Request request, Response response) {
        Session session = request.getSession();
        Attribute<Boolean> redisMulti = session.attr(SessionAttributes.MULTI);
        if (!Boolean.TRUE.equals(redisMulti.get())) {
            response.writeError("DISCARD without MULTI");
            return;
        }
        service.cleanupRedisTransaction(session);
        response.writeOK();
    }
}

