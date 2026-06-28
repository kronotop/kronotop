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

package com.kronotop.core.handlers.pubsub;

import com.kronotop.core.handlers.pubsub.protocol.SSubscribeMessage;
import com.kronotop.core.pubsub.SubscriptionRegistry;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.Session;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

@Command(SSubscribeMessage.COMMAND)
@MinimumParameterCount(SSubscribeMessage.MINIMUM_PARAMETER_COUNT)
public class SSubscribeHandler implements Handler {
    private final SubscriptionRegistry registry;

    public SSubscribeHandler(SubscriptionRegistry registry) {
        this.registry = registry;
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SSUBSCRIBE).set(new SSubscribeMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        SSubscribeMessage message = request.attr(MessageTypes.SSUBSCRIBE).get();
        Session session = request.getSession();

        // Validate ownership for every channel before mutating any subscription state.
        for (String channel : message.getChannels()) {
            registry.ensurePrimary(channel);
        }

        for (String channel : message.getChannels()) {
            int count = registry.subscribe(session, channel);
            registry.sendConfirmation(session, "ssubscribe", channel, count);
        }
    }
}
