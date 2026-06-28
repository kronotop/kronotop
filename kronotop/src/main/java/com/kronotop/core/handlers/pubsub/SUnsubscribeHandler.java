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

import com.kronotop.core.handlers.pubsub.protocol.SUnsubscribeMessage;
import com.kronotop.core.pubsub.SubscriptionRegistry;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.annotation.Command;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Command(SUnsubscribeMessage.COMMAND)
public class SUnsubscribeHandler implements Handler {
    private final SubscriptionRegistry registry;

    public SUnsubscribeHandler(SubscriptionRegistry registry) {
        this.registry = registry;
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SUNSUBSCRIBE).set(new SUnsubscribeMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        SUnsubscribeMessage message = request.attr(MessageTypes.SUNSUBSCRIBE).get();
        Session session = request.getSession();

        List<String> channels = message.getChannels();
        if (channels.isEmpty()) {
            // No channel given: unsubscribe from everything this connection is subscribed to.
            Set<String> subscriptions = session.attr(SessionAttributes.SUBSCRIPTIONS).get();
            channels = new ArrayList<>(subscriptions);
        }

        if (channels.isEmpty()) {
            registry.sendConfirmation(session, "sunsubscribe", null, 0);
            return;
        }

        for (String channel : channels) {
            int count = registry.unsubscribe(session, channel);
            registry.sendConfirmation(session, "sunsubscribe", channel, count);
        }
    }
}
